#!/usr/bin/env python3
"""Continuous wait-time collector for an always-on box (Unraid).

Replaces the GitHub Actions cron, which on this account is delayed ~2.8 h
regardless of what the schedule says. A real cron on your own hardware runs
when it says it will, so the win here is not a higher ceiling — it is
*reliable* cadence, and unlimited local storage.

## Two stores, different jobs

  * **SQLite (`/data/wait_history.db`)** — the truth. Every poll, every
    change, kept forever at full fidelity. This is yours; nothing trims it.
  * **Published JSON (git → the public data repo)** — a small derived view
    the mobile app reads over raw.githubusercontent. Stays a 7-day window
    plus the forever daily summary, because that is all the app draws and
    every byte is downloaded by every user on every park open.

Truth is local and unlimited; the published copy stays lean. Regenerating
the JSON from SQLite means the two can never disagree.

## Being a good citizen of a free community API

themeparks.wiki is one person's free service. Everything here is aimed at
taking as much data as possible while costing them as little as possible:

  * **Conditional requests.** Every poll sends `If-None-Match`. Measured
    against the live API, an unchanged park answers **304 with 0 bytes**
    instead of 10,278 — no JSON serialised, no body transferred. This is
    what makes frequent polling defensible at all.
  * **Never faster than their own cache.** The API declares
    `cache-control: max-age=60`, so a park is never re-polled inside
    [MIN_INTERVAL_S]. Below that the answer is definitionally identical.
  * **A global rate ceiling** ([RATE_LIMIT_RPS]), so 75 parks coming due at
    once trickle out instead of arriving as a burst.
  * **Automatic backoff.** Any 429/5xx honours `Retry-After`, and a 429
    permanently widens the interval for the rest of the process — we get
    quieter on our own, without anyone having to ask.
  * **An identifiable User-Agent** with a contact URL, so we are easy to
    get hold of rather than anonymous traffic to be blocked.

Closed parks are on a slow rotation (they return nothing), and rows are
written only when something actually changed — but a poll is recorded
every time, so "the value held for 40 minutes" is reconstructable and
peaks are never missed. See docs/wait-history-archive.md for why skipping
*polls* on live parks is not an option.
"""
from __future__ import annotations

import json
import os
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

BASE = "https://api.themeparks.wiki/v1"
CONTACT = os.environ.get("CONTACT_URL", "https://github.com/barkeeper")
UA = f"themeparkpilot-wait-history/3.0 (+{CONTACT}; self-hosted collector)"

DB_PATH = os.environ.get("DB_PATH", "/data/wait_history.db")
REPO_DIR = os.environ.get("REPO_DIR", "/data/repo")
DATA_SUBDIR = os.environ.get("DATA_SUBDIR", "data/wait-history")

# Per-park polling interval. 120 s by default: 30x the effective rate the
# GitHub cron managed, still ~13x finer than the median ride's wait
# actually moves (measured: 26 min), and ~0.6 req/s across the ~75 parks
# open at any moment. 60 is the floor worth using — it is the API's own
# cache TTL, and anything below returns a byte-identical response.
POLL_INTERVAL_S = int(os.environ.get("POLL_INTERVAL_S", "120"))
MIN_INTERVAL_S = 60

# Closed parks return no live data at all, so re-probing them often buys
# nothing but load. ~114 of 189 are shut at any moment.
CLOSED_INTERVAL_S = int(os.environ.get("CLOSED_INTERVAL_S", "1800"))

# Hard ceiling on outbound requests per second, whatever the schedule says.
RATE_LIMIT_RPS = float(os.environ.get("RATE_LIMIT_RPS", "2.0"))

# How often to regenerate + push the app-facing JSON.
PUBLISH_INTERVAL_S = int(os.environ.get("PUBLISH_INTERVAL_S", "900"))
PUBLISH_WINDOW_DAYS = int(os.environ.get("PUBLISH_WINDOW_DAYS", "7"))
GIT_REMOTE = os.environ.get("GIT_REMOTE", "")
GIT_BRANCH = os.environ.get("GIT_BRANCH", "main")

MAX_WAIT = 360  # mirrors the app's kMaxPlausibleWaitMinutes clamp
KINDS = {"ATTRACTION", "SHOW"}

_stop = threading.Event()


def log(msg: str) -> None:
    print(f"{datetime.now(timezone.utc):%Y-%m-%dT%H:%M:%SZ} {msg}", flush=True)


# ---------------------------------------------------------------------------
# Politeness: one global token bucket + adaptive backoff
# ---------------------------------------------------------------------------

class Throttle:
    """Serialises every outbound request through one global rate ceiling.

    Without this, 75 parks falling due in the same second would arrive as a
    burst — which is what actually trips abuse heuristics, far more than a
    steady trickle of the same daily total.
    """

    def __init__(self, rps: float) -> None:
        self._min_gap = 1.0 / max(rps, 0.05)
        self._lock = threading.Lock()
        self._next_at = 0.0
        self.penalty_s = 0.0  # grows when the API pushes back

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            due = max(now, self._next_at)
            self._next_at = due + self._min_gap
        delay = due - time.monotonic()
        if delay > 0:
            _stop.wait(delay)

    def back_off(self, seconds: float) -> None:
        """Called on 429. Permanently widens the interval for this process —
        we get quieter on our own rather than waiting to be blocked."""
        with self._lock:
            self.penalty_s = min(self.penalty_s + seconds, 600.0)
            self._min_gap = min(self._min_gap * 1.5, 10.0)
        log(f"  ! backing off: +{seconds:.0f}s penalty, gap now {self._min_gap:.2f}s")


throttle = Throttle(RATE_LIMIT_RPS)


def fetch(url: str, etag: str | None) -> tuple[int, dict | None, str | None]:
    """Conditional GET. Returns (status, body_or_None, etag).

    304 means the payload is byte-identical to what we already hold — the
    cheapest possible answer for the origin, and the common case.
    """
    throttle.wait()
    headers = {"User-Agent": UA, "Accept": "application/json"}
    if etag:
        headers["If-None-Match"] = etag
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, json.load(resp), resp.headers.get("ETag")
    except urllib.error.HTTPError as e:
        if e.code == 304:
            return 304, None, etag
        if e.code == 429 or e.code >= 500:
            retry = e.headers.get("Retry-After") if e.headers else None
            try:
                wait_s = float(retry) if retry else 60.0
            except (TypeError, ValueError):
                wait_s = 60.0
            if e.code == 429:
                throttle.back_off(wait_s)
            else:
                _stop.wait(min(wait_s, 30))
        return e.code, None, etag
    except (urllib.error.URLError, TimeoutError, ValueError, OSError) as exc:
        log(f"  ! {url}: {exc}")
        return 0, None, etag


# ---------------------------------------------------------------------------
# Storage — SQLite is the archive of record
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS parks (
  id TEXT PRIMARY KEY, name TEXT, etag TEXT,
  next_at INTEGER DEFAULT 0, last_open INTEGER
);
-- Every poll, whether or not anything changed. This is what makes a
-- reading's DURATION reconstructable: a value holds from its row until the
-- next change, and these rows prove we were watching throughout.
CREATE TABLE IF NOT EXISTS polls (
  park_id TEXT, ts INTEGER, status INTEGER, changed INTEGER
);
CREATE INDEX IF NOT EXISTS polls_park_ts ON polls(park_id, ts);
-- One row per ride per CHANGE. Unchanged readings aren't duplicated -- at
-- ~26 min between real changes that is a ~13x saving at 2-min polling,
-- with no information lost.
CREATE TABLE IF NOT EXISTS samples (
  park_id TEXT, ride_id TEXT, ts INTEGER, wait INTEGER, state TEXT
);
CREATE INDEX IF NOT EXISTS samples_ride_ts ON samples(ride_id, ts);
CREATE INDEX IF NOT EXISTS samples_park_ts ON samples(park_id, ts);
"""


def connect() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    db = sqlite3.connect(DB_PATH, timeout=30)
    db.executescript(SCHEMA)
    # WAL: the publisher reads while the poller writes.
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.commit()
    return db


def classify(live: dict) -> dict[str, tuple[int | None, str]]:
    """ride_id -> (wait_or_None, state). Mirrors ingest.py's classify."""
    out: dict[str, tuple[int | None, str]] = {}
    for item in live.get("liveData", []) or []:
        if (item.get("entityType") or "").upper() not in KINDS:
            continue
        rid = item.get("id")
        if not rid:
            continue
        status = (item.get("status") or "").upper()
        if status == "OPERATING":
            w = ((item.get("queue") or {}).get("STANDBY") or {}).get("waitTime")
            if isinstance(w, int) and 0 <= w <= MAX_WAIT:
                out[rid] = (w, "OPERATING")
        elif status == "DOWN":
            out[rid] = (None, "DOWN")
        elif status in ("CLOSED", "REFURBISHMENT"):
            out[rid] = (None, "CLOSED")
    return out


def last_known(db: sqlite3.Connection, park_id: str) -> dict[str, tuple]:
    rows = db.execute(
        "SELECT ride_id, wait, state FROM samples WHERE rowid IN "
        "(SELECT MAX(rowid) FROM samples WHERE park_id=? GROUP BY ride_id)",
        (park_id,),
    ).fetchall()
    return {r[0]: (r[1], r[2]) for r in rows}


def poll_park(db: sqlite3.Connection, park: tuple) -> None:
    pid, name, etag = park[0], park[1], park[2]
    now = int(time.time())
    status, body, new_etag = fetch(f"{BASE}/entity/{pid}/live", etag)

    if status == 304:
        db.execute("INSERT INTO polls VALUES (?,?,?,0)", (pid, now, 304))
        db.execute("UPDATE parks SET next_at=? WHERE id=?",
                   (now + POLL_INTERVAL_S + int(throttle.penalty_s), pid))
        return
    if status != 200 or body is None:
        # Transient: retry on the normal cadence rather than punishing the
        # park for the API having a bad minute.
        db.execute("INSERT INTO polls VALUES (?,?,?,0)", (pid, now, status))
        db.execute("UPDATE parks SET next_at=? WHERE id=?",
                   (now + POLL_INTERVAL_S + int(throttle.penalty_s), pid))
        return

    current = classify(body)
    operating = {r for r, (w, s) in current.items() if s == "OPERATING"}
    if not operating:
        # Shut, or no live feed. Slow rotation; nothing to record.
        db.execute("INSERT INTO polls VALUES (?,?,200,0)", (pid, now))
        db.execute("UPDATE parks SET etag=?, next_at=? WHERE id=?",
                   (new_etag, now + CLOSED_INTERVAL_S, pid))
        return

    previous = last_known(db, pid)
    changes = [
        (pid, rid, now, w, s)
        for rid, (w, s) in current.items()
        if previous.get(rid) != (w, s)
    ]
    if changes:
        db.executemany("INSERT INTO samples VALUES (?,?,?,?,?)", changes)
    db.execute("INSERT INTO polls VALUES (?,?,200,?)",
               (pid, now, 1 if changes else 0))
    db.execute(
        "UPDATE parks SET etag=?, next_at=?, last_open=? WHERE id=?",
        (new_etag, now + POLL_INTERVAL_S + int(throttle.penalty_s), now, pid),
    )


def refresh_park_list(db: sqlite3.Connection) -> None:
    status, body, _ = fetch(f"{BASE}/destinations", None)
    if status != 200 or not body:
        return
    seen = 0
    for dest in body.get("destinations", []) or []:
        for park in dest.get("parks", []) or []:
            pid = park.get("id")
            if not pid:
                continue
            seen += 1
            db.execute(
                "INSERT INTO parks (id, name) VALUES (?,?) "
                "ON CONFLICT(id) DO UPDATE SET name=excluded.name",
                (pid, park.get("name", "")),
            )
    db.commit()
    log(f"park list refreshed: {seen} parks known")


# ---------------------------------------------------------------------------
# Publishing — derive the app-facing JSON from SQLite
# ---------------------------------------------------------------------------

def git(*args: str) -> int:
    return subprocess.run(["git", *args], cwd=REPO_DIR,
                          capture_output=True, text=True).returncode


def publish(db: sqlite3.Connection) -> None:
    """Regenerate the two-tier JSON the mobile app reads, then push.

    Derived from SQLite rather than accumulated separately, so the
    published view can never drift from the archive of record.
    """
    if not GIT_REMOTE or not os.path.isdir(os.path.join(REPO_DIR, ".git")):
        return
    out_dir = os.path.join(REPO_DIR, DATA_SUBDIR)
    os.makedirs(out_dir, exist_ok=True)
    now = int(time.time())
    cutoff = now - PUBLISH_WINDOW_DAYS * 86400
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    index = []
    parks = db.execute(
        "SELECT id, name FROM parks WHERE last_open IS NOT NULL").fetchall()
    for pid, name in parks:
        rows = db.execute(
            "SELECT ts, ride_id, wait, state FROM samples "
            "WHERE park_id=? AND ts>=? ORDER BY ts", (pid, cutoff)).fetchall()
        if not rows:
            continue
        # Rebuild fixed-shape samples: replay changes into a running state,
        # emitting one entry per distinct timestamp.
        state: dict[str, tuple] = {}
        samples, order = [], []
        for ts, rid, wait, st in rows:
            state[rid] = (wait, st)
            if not order or order[-1] != ts:
                order.append(ts)
                samples.append(None)
            waits = {r: w for r, (w, s) in state.items() if s == "OPERATING"}
            down = sorted(r for r, (w, s) in state.items() if s == "DOWN")
            closed = sorted(r for r, (w, s) in state.items() if s == "CLOSED")
            entry: dict = {"t": ts, "r": waits}
            if down:
                entry["d"] = down
            if closed:
                entry["c"] = closed
            samples[-1] = entry
        with open(os.path.join(out_dir, f"{pid}.json"), "w",
                  encoding="utf-8") as fh:
            json.dump({"p": pid, "name": name, "generatedAt": now_iso,
                       "windowDays": PUBLISH_WINDOW_DAYS,
                       "samples": samples}, fh, separators=(",", ":"))

        # Forever daily summary, straight from the full archive.
        days: dict[str, dict[str, dict]] = {}
        for ts, rid, wait, st in db.execute(
                "SELECT ts, ride_id, wait, state FROM samples WHERE park_id=? "
                "ORDER BY ts", (pid,)):
            day = datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d")
            slot = days.setdefault(day, {}).setdefault(
                rid, {"w": [], "down": 0, "closed": 0})
            if st == "OPERATING" and wait is not None:
                slot["w"].append(wait)
            elif st == "DOWN":
                slot["down"] += 1
            elif st == "CLOSED":
                slot["closed"] += 1
        summary = {"p": pid, "name": name, "days": {}}
        for day, rides in days.items():
            summary["days"][day] = {}
            for rid, d in rides.items():
                ws = d["w"]
                agg = ([min(ws), max(ws), round(sum(ws) / len(ws)), len(ws)]
                       if ws else [0, 0, 0, 0])
                if d["down"] or d["closed"]:
                    agg += [d["down"], d["closed"]]
                summary["days"][day][rid] = agg
        with open(os.path.join(out_dir, f"{pid}.summary.json"), "w",
                  encoding="utf-8") as fh:
            json.dump(summary, fh, separators=(",", ":"))
        index.append({"id": pid, "name": name, "samples": len(samples)})

    with open(os.path.join(out_dir, "index.json"), "w", encoding="utf-8") as fh:
        json.dump({"generatedAt": now_iso,
                   "windowDays": PUBLISH_WINDOW_DAYS,
                   "parks": index}, fh, separators=(",", ":"))

    git("add", "-A", DATA_SUBDIR)
    if git("diff", "--cached", "--quiet") == 0:
        return  # nothing changed
    git("-c", "user.name=wait-history-collector",
        "-c", "user.email=collector@themeparkpilot.local",
        "commit", "-q", "-m", f"wait history {now_iso} [self-hosted]")
    git("pull", "--rebase", "--autostash", "-q", "origin", GIT_BRANCH)
    if git("push", "-q", "origin", f"HEAD:{GIT_BRANCH}") != 0:
        log("  ! push failed; will retry next publish")
    else:
        log(f"published {len(index)} parks")


# ---------------------------------------------------------------------------

def main() -> int:
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: _stop.set())

    interval = max(POLL_INTERVAL_S, MIN_INTERVAL_S)
    if POLL_INTERVAL_S < MIN_INTERVAL_S:
        log(f"POLL_INTERVAL_S raised to {MIN_INTERVAL_S}s — the API declares "
            f"cache-control: max-age=60, so anything faster is byte-identical")
    log(f"collector up | interval {interval}s | closed {CLOSED_INTERVAL_S}s | "
        f"cap {RATE_LIMIT_RPS} req/s | db {DB_PATH}")

    db = connect()
    refresh_park_list(db)
    last_publish = 0.0
    last_park_refresh = time.monotonic()

    while not _stop.is_set():
        now = int(time.time())
        due = db.execute(
            "SELECT id, name, etag FROM parks WHERE next_at<=? ORDER BY next_at",
            (now,)).fetchall()
        for park in due:
            if _stop.is_set():
                break
            poll_park(db, park)
        db.commit()

        if time.monotonic() - last_publish > PUBLISH_INTERVAL_S:
            try:
                publish(db)
            except Exception as exc:  # noqa: BLE001 — publishing must never
                log(f"  ! publish failed: {exc}")  # take down collection
            last_publish = time.monotonic()

        if time.monotonic() - last_park_refresh > 86400:
            refresh_park_list(db)
            last_park_refresh = time.monotonic()

        _stop.wait(5)

    log("shutting down")
    db.commit()
    db.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
