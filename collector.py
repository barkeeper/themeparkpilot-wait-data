#!/usr/bin/env python3
"""Continuous wait-time collector for an always-on box (Unraid).

Replaces the GitHub Actions cron, which on this account is delayed ~2.8 h
regardless of what the schedule says. A real cron on your own hardware runs
when it says it will, so the win here is not a higher ceiling — it is
*reliable* cadence, and unlimited local storage.

## Two stores, different jobs

  * **SQLite (`/data/wait_history.db`)** — the truth. Every poll, every
    change, kept forever at full fidelity. This is yours; nothing trims it.
  * **Published JSON (`PUBLISH_DIR`)** — a small derived view the app
    reads: a 7-day window plus the forever daily summary, because that is
    all the app draws and every byte is downloaded by every user on every
    park open. A plain static directory, so any web server can serve it
    straight off the array. Mirroring it to git is optional and off by
    default.

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

import gzip
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

# Where the derived, app-facing JSON is written. This is now the PRIMARY
# output: a static directory any web server can serve straight off the
# array. Publishing to git is an optional extra on top, not the mechanism.
PUBLISH_DIR = os.environ.get("PUBLISH_DIR", "/data/published")

# Optional git mirror. Leave GIT_REMOTE empty to skip GitHub entirely.
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

# Keep the FULL response, gzipped, every time it changes.
#
# The request is the scarce resource, not the disk. Each live payload also
# carries SINGLE_RIDER / RETURN_TIME / PAID_RETURN_TIME queues, showtimes,
# operatingHours, diningAvailability and (some parks) an official wait
# forecast -- all of which the structured tables below throw away. Storing
# the raw body costs no extra requests and makes any future question
# answerable from history instead of only from the day you thought to ask.
# ~2 KB gzipped per change; budget ~35-40 GB/year at 60 s polling.
STORE_RAW = os.environ.get("STORE_RAW", "1") not in ("0", "false", "no")

# Park opening hours. Changes daily at most, so once a day per park is
# plenty -- and it is what tells you whether a ride being CLOSED means
# "broken" or "the park is shut".
SCHEDULE_INTERVAL_S = int(os.environ.get("SCHEDULE_INTERVAL_S", "86400"))

# Consistent on-line snapshots of the archive. SQLite's VACUUM INTO takes
# a correct copy while writes continue, so this needs no downtime. The
# live DB is a single file: without a second copy, one bad sector or one
# mistaken `rm` is the entire history. Kept for BACKUP_KEEP days.
BACKUP_INTERVAL_S = int(os.environ.get("BACKUP_INTERVAL_S", "86400"))
BACKUP_KEEP = int(os.environ.get("BACKUP_KEEP", "7"))
BACKUP_DIR = os.environ.get("BACKUP_DIR", "/data/backups")

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
  next_at INTEGER DEFAULT 0, last_open INTEGER,
  sched_at INTEGER DEFAULT 0, sched_etag TEXT
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
-- The whole response, gzipped, whenever it changed. Everything the
-- structured tables drop is still in here.
CREATE TABLE IF NOT EXISTS raw (
  park_id TEXT, ts INTEGER, gz BLOB
);
CREATE INDEX IF NOT EXISTS raw_park_ts ON raw(park_id, ts);
-- Park operating hours, one row per park per date per entry type.
CREATE TABLE IF NOT EXISTS schedules (
  park_id TEXT, date TEXT, type TEXT, opening TEXT, closing TEXT,
  fetched_at INTEGER,
  PRIMARY KEY (park_id, date, type)
);
-- EVERY queue type, one row per ride per queue per CHANGE.
--
-- `samples` only ever kept the STANDBY wait, so single-rider lines,
-- virtual-queue return windows, boarding groups and paid Lightning-Lane
-- PRICES were fetched on every poll and then thrown away -- recoverable
-- only by decompressing and re-parsing `raw`, which no chart can do.
-- The feed already contains them; not storing them was pure loss.
--
-- `value` is the comparable number for the kind (minutes for STANDBY /
-- SINGLE_RIDER, minutes-until-return for RETURN_TIME, the group number
-- for BOARDING_GROUP, price in minor units for PAID_RETURN_TIME) and
-- `extra` keeps the full sub-object so nothing is lost to a schema
-- guess made today.
CREATE TABLE IF NOT EXISTS queues (
  park_id TEXT, ride_id TEXT, ts INTEGER, kind TEXT,
  value INTEGER, extra TEXT
);
CREATE INDEX IF NOT EXISTS queues_ride_ts ON queues(ride_id, kind, ts);
CREATE INDEX IF NOT EXISTS queues_park_ts ON queues(park_id, ts);
-- Ride names + types, so a chart can label a series without calling the
-- upstream API again. Upserted whenever a poll sees a name.
CREATE TABLE IF NOT EXISTS entities (
  id TEXT PRIMARY KEY, park_id TEXT, name TEXT, kind TEXT,
  first_seen INTEGER, last_seen INTEGER
);
CREATE INDEX IF NOT EXISTS entities_park ON entities(park_id);
-- One-shot bookkeeping (e.g. "queues backfilled from raw"), so an
-- expensive migration runs once rather than on every container start.
CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT);
"""


# Columns added to `parks` after the first release. `CREATE TABLE IF NOT
# EXISTS` does NOT alter an existing table, so a database created by an
# older build keeps its old shape and every query against a new column
# raises `no such column` -- which crash-loops the container. Fresh-install
# testing never catches this; upgrades always hit it.
_PARK_COLUMNS = {
    "sched_at": "INTEGER DEFAULT 0",
    "sched_etag": "TEXT",
}


def migrate(db: sqlite3.Connection) -> None:
    """Adds any column missing from an older `parks` table. Idempotent."""
    have = {r[1] for r in db.execute("PRAGMA table_info(parks)")}
    for col, decl in _PARK_COLUMNS.items():
        if col not in have:
            db.execute(f"ALTER TABLE parks ADD COLUMN {col} {decl}")
            log(f"migrated: parks.{col} added")
    db.commit()


BACKFILL_KEY = "queues_backfilled_from_raw_v1"


def backfill_queues(db: sqlite3.Connection) -> None:
    """Replays archived `raw` payloads into `queues` + `entities`, once.

    STORE_RAW has been on since the start, so every queue type the feed
    ever carried is already on disk -- just compressed and unqueryable.
    Without this, `queues` would begin at the moment of this deploy and
    the existing history would stay invisible, which would be a
    self-inflicted gap in an archive whose whole promise is that nothing
    is ever deleted.

    Guarded by a `meta` row: decompressing the entire raw table is
    expensive and must not run on every container start.
    """
    done = db.execute(
        "SELECT v FROM meta WHERE k=?", (BACKFILL_KEY,)).fetchone()
    if done:
        return
    if db.execute("SELECT 1 FROM queues LIMIT 1").fetchone():
        db.execute("INSERT OR REPLACE INTO meta VALUES (?,?)",
                   (BACKFILL_KEY, "skipped-nonempty"))
        db.commit()
        return

    total = db.execute("SELECT COUNT(*) FROM raw").fetchone()[0]
    if not total:
        db.execute("INSERT OR REPLACE INTO meta VALUES (?,?)",
                   (BACKFILL_KEY, "skipped-empty"))
        db.commit()
        return

    log(f"backfill: replaying {total} raw payloads into queues/entities…")
    seen: dict[str, dict[tuple[str, str], tuple]] = {}
    written = 0
    # Streamed in timestamp order so the change-only rule produces exactly
    # what live polling would have produced.
    for pid, ts, gz in db.execute(
            "SELECT park_id, ts, gz FROM raw ORDER BY ts"):
        try:
            body = json.loads(gzip.decompress(gz).decode())
        except Exception:
            continue  # a corrupt blob must not abort the whole backfill
        prev = seen.setdefault(pid, {})
        rows = []
        for (rid, kind), (value, extra) in queue_rows(body, ts).items():
            if prev.get((rid, kind)) == (value, extra):
                continue
            prev[(rid, kind)] = (value, extra)
            rows.append((pid, rid, ts, kind, value, extra))
        if rows:
            db.executemany("INSERT INTO queues VALUES (?,?,?,?,?,?)", rows)
            written += len(rows)
        ents = entity_rows(body)
        if ents:
            db.executemany(
                "INSERT INTO entities (id, park_id, name, kind, first_seen, "
                "last_seen) VALUES (?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET "
                "name=excluded.name, last_seen=excluded.last_seen",
                [(rid, pid, nm, kd, ts, ts) for rid, nm, kd in ents])
    db.execute("INSERT OR REPLACE INTO meta VALUES (?,?)",
               (BACKFILL_KEY, f"rows={written}"))
    db.commit()
    log(f"backfill: {written} queue rows recovered from raw")


def connect() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    db = sqlite3.connect(DB_PATH, timeout=30)
    db.executescript(SCHEMA)
    migrate(db)
    backfill_queues(db)
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
            else:
                # Operating with no standby queue (a show, or a
                # virtual-queue-only ride). Still worth a row: "open, no
                # posted wait" is different from "closed", and the raw
                # payload holds the RETURN_TIME / BOARDING_GROUP detail.
                out[rid] = (None, "OPERATING")
        elif status == "DOWN":
            out[rid] = (None, "DOWN")
        elif status in ("CLOSED", "REFURBISHMENT"):
            out[rid] = (None, "CLOSED")
    return out


def _return_minutes(sub: dict, now: int) -> int | None:
    """Minutes from now until a virtual queue's return window opens.

    Stored as a duration rather than the raw timestamp so it is directly
    comparable with a standby wait -- "come back in 90 minutes" and "queue
    for 90 minutes" are the same axis, which is the whole point of
    plotting them together.
    """
    raw = sub.get("returnStart") or sub.get("returnTime")
    if not isinstance(raw, str) or not raw:
        return None
    try:
        when = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    mins = int((when.timestamp() - now) / 60)
    # Past windows and absurd futures are noise, not data.
    return mins if -60 <= mins <= 1440 else None


def queue_rows(live: dict, now: int) -> dict[tuple[str, str], tuple]:
    """(ride_id, kind) -> (value, extra_json) for every queue in the feed.

    Everything `classify` throws away. Kept deliberately permissive: an
    unrecognised queue kind still gets a row with a null value and its
    full sub-object in `extra`, so a new upstream field starts being
    archived the day it appears rather than the day I notice it.
    """
    out: dict[tuple[str, str], tuple] = {}
    for item in live.get("liveData", []) or []:
        if (item.get("entityType") or "").upper() not in KINDS:
            continue
        rid = item.get("id")
        if not rid:
            continue
        queue = item.get("queue") or {}
        if not isinstance(queue, dict):
            continue
        for kind, sub in queue.items():
            if not isinstance(sub, dict):
                continue
            k = (kind or "").upper()
            value: int | None = None
            if k in ("STANDBY", "SINGLE_RIDER"):
                w = sub.get("waitTime")
                if isinstance(w, int) and 0 <= w <= MAX_WAIT:
                    value = w
            elif k in ("RETURN_TIME", "PAID_RETURN_TIME"):
                value = _return_minutes(sub, now)
                price = sub.get("price")
                if value is None and isinstance(price, dict):
                    amount = price.get("amount")
                    if isinstance(amount, int):
                        value = amount
            elif k == "BOARDING_GROUP":
                for key in ("currentGroupStart", "allocationStatus"):
                    v = sub.get(key)
                    if isinstance(v, int):
                        value = v
                        break
            out[(rid, k)] = (
                value,
                json.dumps(sub, separators=(",", ":"), sort_keys=True),
            )
    return out


def entity_rows(live: dict) -> list[tuple[str, str, str]]:
    """(ride_id, name, entityType) for everything named in the feed."""
    out = []
    for item in live.get("liveData", []) or []:
        kind = (item.get("entityType") or "").upper()
        if kind not in KINDS:
            continue
        rid, name = item.get("id"), item.get("name")
        if rid and isinstance(name, str) and name:
            out.append((rid, name, kind))
    return out


def last_known_queues(
    db: sqlite3.Connection, park_id: str
) -> dict[tuple[str, str], tuple]:
    rows = db.execute(
        "SELECT ride_id, kind, value, extra FROM queues WHERE rowid IN "
        "(SELECT MAX(rowid) FROM queues WHERE park_id=? GROUP BY ride_id, kind)",
        (park_id,),
    ).fetchall()
    return {(r[0], r[1]): (r[2], r[3]) for r in rows}


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

    if STORE_RAW:
        db.execute(
            "INSERT INTO raw VALUES (?,?,?)",
            (pid, now, gzip.compress(
                json.dumps(body, separators=(",", ":")).encode(), 6)))

    previous = last_known(db, pid)
    changes = [
        (pid, rid, now, w, s)
        for rid, (w, s) in current.items()
        if previous.get(rid) != (w, s)
    ]
    if changes:
        db.executemany("INSERT INTO samples VALUES (?,?,?,?,?)", changes)

    # Same change-only discipline as `samples`: a single-rider line that
    # has not moved costs nothing to keep.
    prev_q = last_known_queues(db, pid)
    q_changes = [
        (pid, rid, now, kind, value, extra)
        for (rid, kind), (value, extra) in queue_rows(body, now).items()
        if prev_q.get((rid, kind)) != (value, extra)
    ]
    if q_changes:
        db.executemany("INSERT INTO queues VALUES (?,?,?,?,?,?)", q_changes)

    ents = entity_rows(body)
    if ents:
        db.executemany(
            "INSERT INTO entities (id, park_id, name, kind, first_seen, "
            "last_seen) VALUES (?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET "
            "name=excluded.name, kind=excluded.kind, last_seen=excluded.last_seen",
            [(rid, pid, nm, kd, now, now) for rid, nm, kd in ents],
        )
    db.execute("INSERT INTO polls VALUES (?,?,200,?)",
               (pid, now, 1 if changes else 0))
    db.execute(
        "UPDATE parks SET etag=?, next_at=?, last_open=? WHERE id=?",
        (new_etag, now + POLL_INTERVAL_S + int(throttle.penalty_s), now, pid),
    )


def backup(db: sqlite3.Connection) -> None:
    """Point-in-time snapshot via VACUUM INTO — consistent even mid-write.

    Deliberately a separate FILE rather than a copy of the live one: a
    half-copied SQLite database looks fine until the day you need it.
    """
    os.makedirs(BACKUP_DIR, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    dest = os.path.join(BACKUP_DIR, f"wait_history-{stamp}.db")
    if os.path.exists(dest):
        return
    try:
        db.execute("VACUUM INTO ?", (dest,))
        log(f"backup -> {dest} ({os.path.getsize(dest)/1024/1024:.1f} MB)")
    except sqlite3.Error as exc:
        log(f"  ! backup failed: {exc}")
        return
    # Prune, oldest first. Never touches the live DB.
    snaps = sorted(f for f in os.listdir(BACKUP_DIR)
                   if f.startswith("wait_history-") and f.endswith(".db"))
    for old in snaps[:-BACKUP_KEEP] if len(snaps) > BACKUP_KEEP else []:
        try:
            os.remove(os.path.join(BACKUP_DIR, old))
        except OSError:
            pass


def poll_schedule(db: sqlite3.Connection, pid: str, etag: str | None) -> None:
    """Park opening hours. One request per park per day.

    Worth having for its own sake, and it is also the context that makes
    the live feed interpretable: a ride reading CLOSED at 03:00 is not a
    breakdown, it is a shut park. Without hours you cannot tell those
    apart after the fact.
    """
    now = int(time.time())
    status, body, new_etag = fetch(f"{BASE}/entity/{pid}/schedule", etag)
    if status == 200 and body:
        rows = []
        for e in body.get("schedule", []) or []:
            d, t = e.get("date"), e.get("type")
            if d and t:
                rows.append((pid, d, t, e.get("openingTime"),
                             e.get("closingTime"), now))
        if rows:
            db.executemany(
                "INSERT INTO schedules VALUES (?,?,?,?,?,?) "
                "ON CONFLICT(park_id,date,type) DO UPDATE SET "
                "opening=excluded.opening, closing=excluded.closing, "
                "fetched_at=excluded.fetched_at", rows)
    db.execute("UPDATE parks SET sched_at=?, sched_etag=? WHERE id=?",
               (now + SCHEDULE_INTERVAL_S, new_etag, pid))


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
    # Always write the static directory. This is what the app reads when
    # it is pointed at your own server; git is only a mirror.
    out_dir = PUBLISH_DIR
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

        # Extend the series to NOW.
        #
        # Rows are only written on change, so without this a ride whose
        # wait last moved four hours ago produces a chart that simply stops
        # four hours ago — indistinguishable from "the collector died".
        # We genuinely know the value held until the most recent poll (the
        # `polls` table is the evidence), so carry the final state forward
        # to that timestamp. Not a fabricated reading: it is the last time
        # we looked and saw exactly this.
        last_poll = db.execute(
            "SELECT MAX(ts) FROM polls WHERE park_id=? AND status IN (200,304)",
            (pid,)).fetchone()[0]
        if last_poll and samples and last_poll > samples[-1]["t"]:
            tail = dict(samples[-1])
            tail["t"] = last_poll
            samples.append(tail)
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
        # Ride names, so a chart can label a series without the client
        # calling the upstream API again just to turn ids into words.
        summary["rides"] = {
            r[0]: r[1] for r in db.execute(
                "SELECT id, name FROM entities WHERE park_id=?", (pid,))
        }
        # Hour-of-day profile: 24 buckets of [avg, count] per ride, over
        # the whole archive. This is the shape a park day actually has --
        # a daily min/max/avg cannot show that a ride peaks at 14:00 --
        # and it is cheap because it is derived, not stored.
        hourly: dict[str, list[list[int]]] = {}
        for rid, hour, avg, cnt in db.execute(
                "SELECT ride_id, CAST(strftime('%H', ts, 'unixepoch') AS "
                "INTEGER), AVG(wait), COUNT(*) FROM samples WHERE park_id=? "
                "AND state='OPERATING' AND wait IS NOT NULL "
                "GROUP BY ride_id, 2", (pid,)):
            slot = hourly.setdefault(rid, [[0, 0] for _ in range(24)])
            slot[int(hour)] = [round(avg), cnt]
        summary["hourly"] = hourly
        with open(os.path.join(out_dir, f"{pid}.summary.json"), "w",
                  encoding="utf-8") as fh:
            json.dump(summary, fh, separators=(",", ":"))

        # Non-standby queues get their own file: most parks have none, so
        # folding them into the summary would tax every reader for data
        # only a handful of rides carry.
        qdays: dict[str, dict[str, dict[str, list]]] = {}
        for ts, rid, kind, value in db.execute(
                "SELECT ts, ride_id, kind, value FROM queues WHERE park_id=? "
                "AND kind<>'STANDBY' AND value IS NOT NULL ORDER BY ts",
                (pid,)):
            day = datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d")
            qdays.setdefault(day, {}).setdefault(rid, {}).setdefault(
                kind, []).append(value)
        qout: dict[str, dict] = {}
        for day, rides in qdays.items():
            qout[day] = {}
            for rid, kinds in rides.items():
                qout[day][rid] = {
                    k: [min(v), max(v), round(sum(v) / len(v)), len(v)]
                    for k, v in kinds.items()
                }
        if qout:
            with open(os.path.join(out_dir, f"{pid}.queues.json"), "w",
                      encoding="utf-8") as fh:
                json.dump({"p": pid, "name": name, "days": qout}, fh,
                          separators=(",", ":"))

        # Operating hours. Without them a CLOSED reading is ambiguous --
        # "broken" and "the park is shut" look identical on a chart.
        sched = {
            r[0]: [r[1], r[2]] for r in db.execute(
                "SELECT date, opening, closing FROM schedules WHERE park_id=? "
                "AND type='OPERATING'", (pid,))
        }
        if sched:
            with open(os.path.join(out_dir, f"{pid}.schedule.json"), "w",
                      encoding="utf-8") as fh:
                json.dump({"p": pid, "days": sched}, fh,
                          separators=(",", ":"))

        index.append({
            "id": pid,
            "name": name,
            "samples": len(samples),
            "rides": len(summary["rides"]),
            "queueKinds": sorted({
                r[0] for r in db.execute(
                    "SELECT DISTINCT kind FROM queues WHERE park_id=?", (pid,))
            }),
        })

    with open(os.path.join(out_dir, "index.json"), "w", encoding="utf-8") as fh:
        json.dump({"generatedAt": now_iso,
                   "windowDays": PUBLISH_WINDOW_DAYS,
                   "parks": index}, fh, separators=(",", ":"))

    # Also expose everything under a `wait-history/` subdirectory.
    #
    # Reverse proxies are split on whether `proxy_pass http://host:8095`
    # (no trailing slash) forwards `/wait-history/index.json` verbatim or
    # strips the prefix first. Getting that wrong is the single most
    # common way this setup 404s. Serving the same files at BOTH `/` and
    # `/wait-history/` makes either proxy config work, for the cost of
    # ~180 KB of duplication -- far cheaper than a support round-trip.
    mirror = os.path.join(out_dir, "wait-history")
    os.makedirs(mirror, exist_ok=True)
    for fname in os.listdir(out_dir):
        if not fname.endswith(".json"):
            continue
        with open(os.path.join(out_dir, fname), "rb") as src,              open(os.path.join(mirror, fname), "wb") as dst:
            dst.write(src.read())

    # Optional git mirror. Skipped entirely when GIT_REMOTE is empty, so a
    # fully self-hosted setup never touches GitHub.
    if not GIT_REMOTE or not os.path.isdir(os.path.join(REPO_DIR, ".git")):
        log(f"published {len(index)} parks -> {out_dir}")
        return
    repo_out = os.path.join(REPO_DIR, DATA_SUBDIR)
    os.makedirs(repo_out, exist_ok=True)
    for fname in os.listdir(out_dir):
        if fname.endswith(".json"):
            with open(os.path.join(out_dir, fname), "rb") as src,                  open(os.path.join(repo_out, fname), "wb") as dst:
                dst.write(src.read())
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
        log(f"published {len(index)} parks (git mirror)")


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
    last_backup = 0.0
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

        # Schedules ride the same throttle, so they can never crowd out
        # live polling — they just fill idle capacity.
        for pid, sched_etag in db.execute(
                "SELECT id, sched_etag FROM parks WHERE sched_at<=? "
                "AND last_open IS NOT NULL LIMIT 5", (now,)).fetchall():
            if _stop.is_set():
                break
            poll_schedule(db, pid, sched_etag)
        db.commit()

        if time.monotonic() - last_publish > PUBLISH_INTERVAL_S:
            try:
                publish(db)
            except Exception as exc:  # noqa: BLE001 — publishing must never
                log(f"  ! publish failed: {exc}")  # take down collection
            last_publish = time.monotonic()

        if time.monotonic() - last_backup > BACKUP_INTERVAL_S:
            backup(db)
            last_backup = time.monotonic()

        if time.monotonic() - last_park_refresh > 86400:
            refresh_park_list(db)
            last_park_refresh = time.monotonic()

        _stop.wait(5)

    log("shutting down")
    db.commit()
    # Fold the write-ahead log back into the main file so the archive is a
    # single self-contained database at rest, rather than one that depends
    # on a -wal sidecar surviving alongside it.
    try:
        db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    except sqlite3.Error:
        pass
    db.close()
    log("archive closed cleanly")
    return 0


if __name__ == "__main__":
    sys.exit(main())
