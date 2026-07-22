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

Runs on a GitHub Actions cron in the PUBLIC data repo, so runner minutes
are unlimited and the whole thing is $0. Discovers all parks from
`/destinations` unless `parks.json` pins an allowlist. Standard library
only (no pip install on the runner).

    python tools/wait_history/ingest.py --data-dir <dir> \
        [--window-days 7] [--workers 6] [--cold-every 4] [--limit N]

## Adaptive polling (2026-07-22)

themeparks.wiki is a free community API, and being polite to it is the
only real limit on how often we can scan — so the request budget goes
where the data is. Roughly 57 of ~132 parks are open at any moment; the
other ~75 requests per run return "no live data" and are thrown away.

Each park therefore sits on one of two rotations, tracked in
`_state.json` next to the data:

  * **hot** — produced data within `_HOT_WINDOW_H` hours: polled every run.
  * **cold** — closed, overnight, or has no live feed at all: polled once
    every `--cold-every` runs.

A cold park that reopens is picked up within `cold-every` runs (one hour
at the shipped 15-minute cadence), which is finer than opening times are
announced anyway. That saving is what pays for the higher cadence without
leaning any harder on the upstream than the old hourly run did.
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
_UA = "themeparkpilot-wait-history/2.1 (+github actions ingest)"
_MAX_WAIT = 360  # mirror the app's kMaxPlausibleWaitMinutes clamp
_KINDS = {"ATTRACTION", "SHOW"}

# A park that produced data this recently is "hot" and polled every run.
# 20 h rather than 24 so a park still counts as hot the morning after a
# normal operating day, without a park that closed for the season staying
# hot forever.
_HOT_WINDOW_H = 20

# Bookkeeping for adaptive polling. Underscore-prefixed so it sorts away
# from the `<parkId>.json` files the app reads; the app ignores it.
_STATE_FILE = "_state.json"


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


def select_parks(parks: list[dict], state: dict, now: int,
                 cold_every: int) -> tuple[list[dict], int]:
    """Splits the park list into the ones worth polling this run.

    Returns `(to_poll, skipped)`. A park is polled when it produced data
    inside the hot window, or when this run's turn on the cold rotation
    comes up. Never-seen parks ride the cold rotation too, so a newly
    listed park is discovered within `cold_every` runs rather than costing
    a request every single run forever (most of the ~132 parks in the
    catalog have no live feed at all)."""
    run_no = int(state.get("run", 0)) + 1
    state["run"] = run_no
    last_open = state.get("lastOpen", {}) or {}
    if cold_every <= 1 or not last_open:
        # Either adaptive polling is off, or this is a cold start (first
        # ever run, or the state file was lost). With no idea which parks
        # are live, the only correct move is to probe all of them — the
        # alternative is an empty run that also learns nothing.
        return parks, 0
    hot_cutoff = now - _HOT_WINDOW_H * 3600
    # `- 1` so the FIRST run is a cold turn rather than the `cold_every`-th.
    # Otherwise a scheduler that drops runs could go a long time before the
    # counter happens to land on a multiple.
    cold_turn = (run_no - 1) % cold_every == 0

    to_poll, skipped = [], 0
    for park in parks:
        seen = last_open.get(park["id"])
        if isinstance(seen, int) and seen >= hot_cutoff:
            to_poll.append(park)          # hot — currently operating
        elif cold_turn:
            to_poll.append(park)          # cold — periodic re-probe
        else:
            skipped += 1
    return to_poll, skipped


def run(data_dir: str, config_path: str, window_days: int, workers: int,
        limit: int, cold_every: int) -> int:
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

    state_path = os.path.join(data_dir, _STATE_FILE)
    state = _load(state_path) or {"run": 0, "lastOpen": {}}
    state.setdefault("lastOpen", {})
    polled, skipped = select_parks(parks, state, now, cold_every)

    index, recorded = [], 0
    with cf.ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = {
            pool.submit(record_park, data_dir, p, now, cutoff, window_days, now_iso): p
            for p in polled
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
                state["lastOpen"][park["id"]] = now
                index.append({"id": park["id"], "name": park.get("name", ""),
                              "samples": count})

    # The index must describe the WHOLE archive, not just this run — a
    # cold park still has a file on disk and the app must still find it.
    # Carry forward last run's entries for anything we skipped.
    previous = {
        e["id"]: e
        for e in ((_load(os.path.join(data_dir, "index.json")) or {}).get("parks") or [])
        if isinstance(e, dict) and e.get("id")
    }
    fresh_ids = {e["id"] for e in index}
    for pid, entry in previous.items():
        if pid not in fresh_ids:
            index.append(entry)

    with open(os.path.join(data_dir, "index.json"), "w", encoding="utf-8") as fh:
        json.dump({"generatedAt": now_iso, "windowDays": window_days,
                   "parks": index}, fh, separators=(",", ":"))
    with open(state_path, "w", encoding="utf-8") as fh:
        json.dump(state, fh, separators=(",", ":"))

    print(f"Polled {len(polled)}/{len(parks)} parks "
          f"({skipped} cold-skipped), recorded {recorded}.")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--data-dir", required=True)
    ap.add_argument(
        "--config",
        default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "parks.json"),
    )
    # 7 days, not 30: the app's chart is a trailing-7-day line
    # (`wait_history_chart.dart`) and the crowd forecast's lookback is
    # 7 days too, so days 8–30 of the raw window were downloaded by every
    # user on every park open and then never drawn. The forever summary is
    # what serves long-range history, and it is unaffected by this.
    ap.add_argument("--window-days", type=int, default=7)
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--cold-every", type=int, default=4,
                    help="Re-probe closed parks every Nth run (1 = every "
                         "run, i.e. disable adaptive polling).")
    ap.add_argument("--limit", type=int, default=0,
                    help="Cap the park count (0 = all) — handy for testing.")
    args = ap.parse_args()
    return run(args.data_dir, args.config, args.window_days, args.workers,
               args.limit, args.cold_every)


if __name__ == "__main__":
    sys.exit(main())
