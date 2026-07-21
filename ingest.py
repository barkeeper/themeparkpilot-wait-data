#!/usr/bin/env python3
"""Record wait-time samples for **every** themeparks.wiki park into two
tiers of plaintext JSON — the ingest half of the app's **$0** historical
feature (GitHub Actions + a Git branch is the whole "server").

Two tiers, so nothing that matters is ever lost yet no file grows without
bound (which would eventually crash a naive reader):

  * `<parkId>.json`         — ROLLING raw window (default 30 days) at the
                              run cadence. Bounded → the app fetches one
                              small file per park, never a giant blob.
  * `<parkId>.summary.json` — per-ride, per-UTC-day [min, max, avg, count],
                              plus [down, closed] appended on days the ride
                              broke down (DOWN) or was closed during park
                              hours (CLOSED / REFURBISHMENT). Kept
                              **forever**. Tiny (a year is a few hundred KB)
                              and never trimmed, so the long-range historical
                              signal only ever gets richer. Days still inside
                              the raw window are recomputed each run; older
                              days are frozen.

Runs on a GitHub Actions cron (hourly by default). Discovers all parks
from `/destinations` unless `parks.json` pins an allowlist. Standard
library only (no pip install on the runner).

    python tools/wait_history/ingest.py --data-dir <dir> \
        [--window-days 30] [--workers 8] [--limit N]
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

_BASE = "https://api.themeparks.wiki/v1"
_UA = "themeparkpilot-wait-history/2.0 (+github actions ingest)"
_MAX_WAIT = 360  # mirror the app's kMaxPlausibleWaitMinutes clamp
_KINDS = {"ATTRACTION", "SHOW"}


def _get_json(url: str) -> dict:
    req = urllib.request.Request(
        url, headers={"User-Agent": _UA, "Accept": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


def discover_parks(allowlist: list[dict]) -> list[dict]:
    """Returns [{'id','name'}]. Uses the allowlist when non-empty, else
    every park themeparks.wiki knows about."""
    pinned = [
        p for p in allowlist
        if p.get("id") and not str(p["id"]).upper().startswith("REPLACE")
    ]
    if pinned:
        return pinned
    data = _get_json(f"{_BASE}/destinations")
    seen, parks = set(), []
    for dest in data.get("destinations", []) or []:
        for park in dest.get("parks", []) or []:
            pid = park.get("id")
            if pid and pid not in seen:
                seen.add(pid)
                parks.append({"id": pid, "name": park.get("name", "")})
    return parks


def classify(live: dict) -> tuple[dict[str, int], list[str], list[str]]:
    """Split a park's live feed into three buckets:

      * `waits`  — `{attractionId: waitMinutes}` for OPERATING rides/shows
                   with a plausible standby wait.
      * `down`   — ids of rides that are **DOWN** (a breakdown — broke and
                   can't be ridden).
      * `closed` — ids of rides that are **CLOSED** or under
                   **REFURBISHMENT** (not rideable).

    The caller only records a park when `waits` is non-empty (i.e. the park
    is open), so every `down` / `closed` id is a ride that broke down or was
    closed *during the park's opening hours* — never the overnight
    all-closed state, which is skipped entirely."""
    waits: dict[str, int] = {}
    down: list[str] = []
    closed: list[str] = []
    for item in live.get("liveData", []) or []:
        if (item.get("entityType") or "").upper() not in _KINDS:
            continue
        rid = item.get("id")
        if not rid:
            continue
        status = (item.get("status") or "").upper()
        if status == "OPERATING":
            standby = (item.get("queue") or {}).get("STANDBY") or {}
            w = standby.get("waitTime")
            if isinstance(w, int) and 0 <= w <= _MAX_WAIT:
                waits[rid] = w
        elif status == "DOWN":
            down.append(rid)
        elif status in ("CLOSED", "REFURBISHMENT"):
            closed.append(rid)
    return waits, down, closed


def _agg_tuple(day_ride: dict) -> list[int]:
    """One ride's daily aggregate: `[min, max, avg, count]` over its
    OPERATING samples, with `[down, closed]` appended **only** when the ride
    broke down or was closed that day. Keeping the common all-operating case
    a 4-tuple stays byte-compatible with readers that index 0..3 (the mobile
    app) and keeps the forever-summary small."""
    ws = day_ride["w"]
    if ws:
        out = [min(ws), max(ws), round(sum(ws) / len(ws)), len(ws)]
    else:
        # Ride never operated this day (down / closed all day): no wait
        # stats, but we still emit an entry so the break shows up.
        out = [0, 0, 0, 0]
    down, closed = day_ride["down"], day_ride["closed"]
    if down or closed:
        out += [down, closed]
    return out


def _load(path: str):
    try:
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return None


def _utc_day(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, timezone.utc).strftime("%Y-%m-%d")


def record_park(data_dir: str, park: dict, now: int, cutoff: int,
                window_days: int, now_iso: str) -> int | None:
    """Fetches one park's live feed and updates its raw + summary files.
    Returns the raw sample count, or None when there's nothing to record."""
    pid, pname = park["id"], park.get("name", "")
    try:
        live = _get_json(f"{_BASE}/entity/{pid}/live")
    except (urllib.error.URLError, TimeoutError, ValueError, OSError) as exc:
        print(f"WARN {pname} ({pid}): fetch failed: {exc}", file=sys.stderr)
        return None
    waits, down, closed = classify(live)
    if not waits:
        return None  # park closed / no live data right now — skip quietly

    # ---- rolling raw window ------------------------------------------
    raw_path = os.path.join(data_dir, f"{pid}.json")
    raw = _load(raw_path) or {"p": pid, "samples": []}
    samples = [
        s for s in raw.get("samples", [])
        if isinstance(s, dict) and isinstance(s.get("t"), int) and s["t"] >= cutoff
    ]
    sample: dict = {"t": now, "r": waits}
    # Only attach the down / closed lists when non-empty so a normal
    # everything-running sample stays as small as before.
    if down:
        sample["d"] = down
    if closed:
        sample["c"] = closed
    samples.append(sample)
    with open(raw_path, "w", encoding="utf-8") as fh:
        json.dump({
            "p": pid, "name": pname, "generatedAt": now_iso,
            "windowDays": window_days, "samples": samples,
        }, fh, separators=(",", ":"))

    # ---- forever daily summary ---------------------------------------
    # Recompute every day still inside the raw window from raw samples and
    # merge into the summary; days that have aged out of the window stay
    # frozen (never recomputed, never dropped).
    sum_path = os.path.join(data_dir, f"{pid}.summary.json")
    summary = _load(sum_path) or {"p": pid, "days": {}}
    # day -> rid -> {"w": [waits], "down": n, "closed": n}
    by_day: dict[str, dict[str, dict]] = {}

    def _slot(day: str, rid: str) -> dict:
        return by_day.setdefault(day, {}).setdefault(
            rid, {"w": [], "down": 0, "closed": 0})

    for s in samples:
        day = _utc_day(s["t"])
        for rid, w in s.get("r", {}).items():
            if isinstance(w, int):
                _slot(day, rid)["w"].append(w)
        for rid in s.get("d", []) or []:
            _slot(day, rid)["down"] += 1
        for rid in s.get("c", []) or []:
            _slot(day, rid)["closed"] += 1
    # CRITICAL: only (re)write a day's aggregate while ALL of that day's
    # samples are still in the raw window (1-day safety margin). Otherwise
    # a day that's mid-way through being trimmed would overwrite its
    # complete, frozen aggregate with a partial (corrupted) one. Days
    # past the margin keep the frozen value they were saved with while
    # complete — so the aging-out data is preserved forever, correctly.
    safe = cutoff + 86400
    for day, rides in by_day.items():
        day_start = datetime.strptime(day, "%Y-%m-%d").replace(
            tzinfo=timezone.utc).timestamp()
        if day_start < safe:
            continue  # boundary / already-frozen day — never recompute
        summary["days"][day] = {
            rid: _agg_tuple(d) for rid, d in rides.items()
        }
    summary["p"] = pid
    summary["name"] = pname
    with open(sum_path, "w", encoding="utf-8") as fh:
        json.dump(summary, fh, separators=(",", ":"))

    return len(samples)


def run(data_dir: str, config_path: str, window_days: int, workers: int,
        limit: int) -> int:
    cfg = _load(config_path) or {}
    parks = discover_parks(cfg.get("parks", []))
    if limit > 0:
        parks = parks[:limit]
    if not parks:
        print("No parks discovered; nothing to do.", file=sys.stderr)
        return 1

    os.makedirs(data_dir, exist_ok=True)
    now = int(time.time())
    cutoff = now - window_days * 86400
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    index, recorded = [], 0
    with cf.ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = {
            pool.submit(record_park, data_dir, p, now, cutoff, window_days, now_iso): p
            for p in parks
        }
        for fut in cf.as_completed(futures):
            park = futures[fut]
            try:
                count = fut.result()
            except Exception as exc:  # noqa: BLE001 — one park must not fail the run
                print(f"WARN {park.get('name')}: {exc}", file=sys.stderr)
                continue
            if count is not None:
                recorded += 1
                index.append({"id": park["id"], "name": park.get("name", ""),
                              "samples": count})

    with open(os.path.join(data_dir, "index.json"), "w", encoding="utf-8") as fh:
        json.dump({"generatedAt": now_iso, "windowDays": window_days,
                   "parks": index}, fh, separators=(",", ":"))
    print(f"Recorded {recorded}/{len(parks)} parks (rest closed / no live data).")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--data-dir", required=True)
    ap.add_argument(
        "--config",
        default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "parks.json"),
    )
    ap.add_argument("--window-days", type=int, default=30)
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--limit", type=int, default=0,
                    help="Cap the park count (0 = all) — handy for testing.")
    args = ap.parse_args()
    return run(args.data_dir, args.config, args.window_days, args.workers,
               args.limit)


if __name__ == "__main__":
    sys.exit(main())
