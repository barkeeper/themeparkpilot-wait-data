#!/usr/bin/env python3
"""Multi-source ingest + fast live publish for the collector.

The base collector polls themeparks.wiki and writes UUID-keyed samples. This
module adds the OTHER sources (queue-times.com, parkqueuetimes.com) alongside
it and derives the small, fresh ``{park}.live.json`` the app reads.

Design choices that keep it safe to bolt onto the running archive:

  * **Reconcile on the way in.** Each source's native ride ids are mapped to
    themeparks UUIDs via ``idmap`` *before* insert, so ``samples.ride_id`` is
    always a themeparks UUID regardless of source and the merge just groups by
    it. Unmatched rides are dropped, never stored (a wrong ride is worse than
    a missing one).
  * **Per-(park, source) scheduling** in a new ``source_state`` table, so each
    source keeps its own etag / next_at / cadence and one slow source never
    blocks another. The existing themeparks path in ``collector.py`` is left
    untouched.
  * **Same politeness.** Every request goes through the collector's shared
    ``Throttle`` and its conditional ``fetch`` (If-None-Match → 304).
  * **Change-only, per source.** A source's unchanged reading costs nothing,
    exactly like the base ``samples`` discipline — but scoped by source so two
    sources reporting the same ride don't collapse each other's history.

``fetch(url, etag) -> (status, body_or_None, etag)`` and the ``Throttle`` are
passed in from ``collector.py`` to avoid an import cycle.
"""
from __future__ import annotations

import gzip
import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone

import idmap
import merge

MAX_WAIT = 360  # mirrors the app's kMaxPlausibleWaitMinutes clamp


def log(msg: str) -> None:
    """Line-buffered stderr log (collector.py has its own timestamped one; this
    keeps multisource usable standalone + in tests without importing it)."""
    print(msg, file=sys.stderr, flush=True)

# ---------------------------------------------------------------------------
# Source registry — each knows how to build its live URL and read its rides.
# ---------------------------------------------------------------------------


def _qt_live_url(ext_id: str) -> str:
    return f"https://queue-times.com/parks/{ext_id}/queue_times.json"


def _qt_classify(body: dict) -> dict[str, tuple[int | None, str]]:
    """queue-times: lands[].rides[] + rides[], each {id, is_open, wait_time}."""
    out: dict[str, tuple[int | None, str]] = {}
    rides = []
    for land in body.get("lands", []) or []:
        rides.extend(land.get("rides", []) or [])
    rides.extend(body.get("rides", []) or [])
    for r in rides:
        rid = r.get("id")
        if rid is None:
            continue
        if r.get("is_open"):
            w = r.get("wait_time")
            w = w if isinstance(w, int) and 0 <= w <= MAX_WAIT else None
            out[str(rid)] = (w, merge.OPERATING)
        else:
            out[str(rid)] = (None, merge.CLOSED)
    return out


def _pqt_live_url(ext_id: str) -> str:
    return f"https://api.parkqueuetimes.com/v1/parks/{ext_id}/live"


def _pqt_classify(body: dict) -> dict[str, tuple[int | None, str]]:
    """parkqueuetimes: {data:{rides|lands[].rides}}. Defensive about field
    names (wait_time/waitTime/wait; is_open/status)."""
    data = body.get("data", body) or {}
    rides = []
    for land in data.get("lands", []) or []:
        rides.extend(land.get("rides", []) or [])
    rides.extend(data.get("rides", []) or [])
    out: dict[str, tuple[int | None, str]] = {}
    for r in rides:
        rid = r.get("id")
        if rid is None:
            continue
        status = (r.get("status") or "").upper()
        is_open = r.get("is_open")
        operating = is_open is True or status in ("OPERATING", "OPEN")
        if status in ("DOWN", "BREAKDOWN"):
            out[str(rid)] = (None, merge.DOWN)
        elif operating:
            w = r.get("wait_time")
            if w is None:
                w = r.get("waitTime")
            if w is None:
                w = r.get("wait")
            w = w if isinstance(w, int) and 0 <= w <= MAX_WAIT else None
            out[str(rid)] = (w, merge.OPERATING)
        else:
            out[str(rid)] = (None, merge.CLOSED)
    return out


# name -> (live_url_fn, classify_fn, headers_env, poll_interval_env, default_s)
SOURCES = {
    "queue_times": {
        "url": _qt_live_url, "classify": _qt_classify,
        "key_env": None, "interval_env": "QUEUE_TIMES_POLL_INTERVAL_S",
        "default_interval": 300,  # their data updates every 5 min
        # queue-times publishes no hard limit; stay a polite citizen anyway.
        "day_env": "QUEUE_TIMES_MAX_PER_DAY", "default_max_day": 0,  # 0 = none
        "min_env": "QUEUE_TIMES_MAX_PER_MIN", "default_max_min": 60,
    },
    "parkqueuetimes": {
        "url": _pqt_live_url, "classify": _pqt_classify,
        "key_env": "PARKQUEUETIMES_API_KEY",
        "interval_env": "PARKQUEUETIMES_POLL_INTERVAL_S",
        # Free tier is HARD-capped at 60 req/min AND 10 000 req/day. With ~75
        # mapped parks, a 120 s interval would be ~54 000/day — 5x over. 900 s
        # keeps even all-day coverage at ~7 200/day (75 × 96); the budget caps
        # below are the real ceiling that can never be crossed regardless of
        # park count or a mis-set interval. Headroom left under both limits.
        "default_interval": 900,
        "day_env": "PARKQUEUETIMES_MAX_PER_DAY", "default_max_day": 9500,
        "min_env": "PARKQUEUETIMES_MAX_PER_MIN", "default_max_min": 55,
    },
}


def _source_limits(source: str) -> tuple[int, int]:
    """(max_per_day, max_per_min) for a source; 0 = unlimited for that axis."""
    cfg = SOURCES[source]
    day = int(os.environ.get(cfg["day_env"], cfg["default_max_day"]))
    per_min = int(os.environ.get(cfg["min_env"], cfg["default_max_min"]))
    return day, per_min


def _requests_since(db: sqlite3.Connection, source: str, since_ts: int) -> int:
    """How many requests this source has made since ``since_ts`` — one `polls`
    row is written per request (200/304/error), so this IS the request count,
    and it survives restarts (unlike an in-memory counter)."""
    return db.execute(
        "SELECT COUNT(*) FROM polls WHERE source=? AND ts>=?",
        (source, since_ts)).fetchone()[0]


def enabled_sources() -> list[str]:
    """queue_times unless disabled; parkqueuetimes only if its key is set."""
    out = []
    if os.environ.get("QUEUE_TIMES_ENABLED", "1") not in ("0", "false", "no"):
        out.append("queue_times")
    if os.environ.get("PARKQUEUETIMES_API_KEY"):
        out.append("parkqueuetimes")
    return out


# ---------------------------------------------------------------------------
# Schema — additive. Source columns on existing tables + a scheduling table.
# ---------------------------------------------------------------------------

_SOURCE_COLUMN_TABLES = ("polls", "samples", "queues", "raw")


def ensure_schema(db: sqlite3.Connection) -> None:
    """Add a ``source`` column (default 'themeparks') to the sample-bearing
    tables and create ``source_state``. Idempotent — safe on every boot."""
    for table in _SOURCE_COLUMN_TABLES:
        have = {r[1] for r in db.execute(f"PRAGMA table_info({table})")}
        if "source" not in have:
            db.execute(
                f"ALTER TABLE {table} ADD COLUMN source TEXT "
                "DEFAULT 'themeparks'")
    db.execute(
        "CREATE TABLE IF NOT EXISTS source_state ("
        "  park_id TEXT, source TEXT, ext_id TEXT, etag TEXT,"
        "  next_at INTEGER DEFAULT 0, last_open INTEGER,"
        "  PRIMARY KEY (park_id, source))")
    db.execute(
        "CREATE INDEX IF NOT EXISTS samples_park_source_ts "
        "ON samples(park_id, source, ts)")
    db.commit()


def seed_source_state(db: sqlite3.Connection, source: str) -> int:
    """Insert (park_id, source, ext_id) rows for every park in this source's
    parkmap that isn't already scheduled. Returns rows added."""
    smap = idmap.load_source_map(source)
    added = 0
    for tp_park_uuid in smap.parkmap:
        ext_id = smap.source_park_for(tp_park_uuid)
        if not ext_id:
            continue
        cur = db.execute(
            "INSERT OR IGNORE INTO source_state (park_id, source, ext_id) "
            "VALUES (?,?,?)", (tp_park_uuid, source, str(ext_id)))
        added += cur.rowcount
    db.commit()
    return added


# ---------------------------------------------------------------------------
# Ingest — one due (park, source) at a time, reconciled + change-only.
# ---------------------------------------------------------------------------

def _last_known(db: sqlite3.Connection, park_id: str,
                source: str) -> dict[str, tuple]:
    rows = db.execute(
        "SELECT ride_id, wait, state FROM samples WHERE rowid IN "
        "(SELECT MAX(rowid) FROM samples WHERE park_id=? AND source=? "
        "GROUP BY ride_id)", (park_id, source)).fetchall()
    return {r[0]: (r[1], r[2]) for r in rows}


def poll_source_park(db: sqlite3.Connection, fetch, throttle, source: str,
                     row: tuple, store_raw: bool = True) -> None:
    """Poll one park from one source, reconcile to UUIDs, write change-only.

    ``row`` = (park_id, ext_id, etag) from ``source_state``.
    ``fetch(url, etag)`` and ``throttle`` come from collector.py.
    """
    park_id, ext_id, etag = row
    cfg = SOURCES[source]
    interval = int(os.environ.get(cfg["interval_env"], cfg["default_interval"]))
    now = int(time.time())
    headers = {}
    if cfg["key_env"] and os.environ.get(cfg["key_env"]):
        headers = {"x-api-key": os.environ[cfg["key_env"]]}

    status, body, new_etag = fetch(cfg["url"](ext_id), etag, headers=headers) \
        if _fetch_takes_headers(fetch) else fetch(cfg["url"](ext_id), etag)

    if status == 304 or status != 200 or body is None:
        db.execute("INSERT INTO polls (park_id, ts, status, changed, source) "
                   "VALUES (?,?,?,0,?)", (park_id, now, status, source))
        db.execute("UPDATE source_state SET next_at=? WHERE park_id=? AND "
                   "source=?", (now + interval + int(throttle.penalty_s),
                                park_id, source))
        return

    smap = idmap.load_source_map(source)
    native = cfg["classify"](body)
    # Reconcile native ride ids → themeparks UUIDs; drop unmatched.
    current: dict[str, tuple[int | None, str]] = {}
    for native_id, (wait, state) in native.items():
        uuid = smap.uuid_for_ride(native_id)
        if uuid:
            current[uuid] = (wait, state)

    operating = {r for r, (w, s) in current.items() if s == merge.OPERATING}
    if not operating:
        db.execute("INSERT INTO polls (park_id, ts, status, changed, source) "
                   "VALUES (?,?,200,0,?)", (park_id, now, source))
        db.execute("UPDATE source_state SET etag=?, next_at=? WHERE park_id=? "
                   "AND source=?", (new_etag, now + interval, park_id, source))
        return

    if store_raw:
        db.execute("INSERT INTO raw (park_id, ts, gz, source) VALUES (?,?,?,?)",
                   (park_id, now, gzip.compress(
                       json.dumps(body, separators=(",", ":")).encode(), 6),
                    source))

    previous = _last_known(db, park_id, source)
    changes = [(park_id, rid, now, w, s, source)
               for rid, (w, s) in current.items() if previous.get(rid) != (w, s)]
    if changes:
        db.executemany(
            "INSERT INTO samples (park_id, ride_id, ts, wait, state, source) "
            "VALUES (?,?,?,?,?,?)", changes)
    db.execute("INSERT INTO polls (park_id, ts, status, changed, source) "
               "VALUES (?,?,200,?,?)",
               (park_id, now, 1 if changes else 0, source))
    db.execute("UPDATE source_state SET etag=?, next_at=?, last_open=? WHERE "
               "park_id=? AND source=?",
               (new_etag, now + interval + int(throttle.penalty_s), now,
                park_id, source))


def _fetch_takes_headers(fetch) -> bool:
    import inspect
    try:
        return "headers" in inspect.signature(fetch).parameters
    except (TypeError, ValueError):
        return False


def poll_due_sources(db: sqlite3.Connection, fetch, throttle,
                     store_raw: bool = True, limit_per_source: int = 50) -> int:
    """Poll currently-due (park, source) pairs, honouring each source's
    per-minute AND per-day request budget. Returns count polled.

    The budget is enforced against the persisted `polls` counts, so a source
    can NEVER exceed its documented limit (e.g. parkqueuetimes' 60/min +
    10 000/day free tier) even if its park count grows or its interval is
    mis-set — we just stop polling it for the rest of the minute / day and
    resume when the window rolls over. Keys aren't at risk of revocation.
    """
    now = int(time.time())
    day_start = now - (now % 86400)  # UTC midnight (unix epoch is UTC-aligned)
    polled = 0
    for source in enabled_sources():
        max_day, max_min = _source_limits(source)
        budget = limit_per_source
        if max_day:
            used_day = _requests_since(db, source, day_start)
            if used_day >= max_day:
                log(f"{source}: daily budget {max_day} reached "
                    f"({used_day}) — pausing until UTC midnight")
                continue
            budget = min(budget, max_day - used_day)
        if max_min:
            used_min = _requests_since(db, source, now - 60)
            budget = min(budget, max(0, max_min - used_min))
        if budget <= 0:
            continue
        due = db.execute(
            "SELECT park_id, ext_id, etag FROM source_state WHERE source=? "
            "AND next_at<=? ORDER BY next_at LIMIT ?",
            (source, now, budget)).fetchall()
        for row in due:
            poll_source_park(db, fetch, throttle, source, row,
                             store_raw=store_raw)
            polled += 1
    db.commit()
    return polled


# ---------------------------------------------------------------------------
# Fast live publish — merge latest-per-source into {park}.live.json.
# ---------------------------------------------------------------------------

def _latest_per_source(db: sqlite3.Connection, park_id: str,
                       fresh_cutoff: int) -> dict[str, list[merge.Reading]]:
    """Latest sample per (ride, source) for a park, keeping only sources whose
    most recent successful poll of this park is within the freshness cutoff."""
    fresh_sources = {
        r[0] for r in db.execute(
            "SELECT source, MAX(ts) FROM polls WHERE park_id=? AND "
            "status IN (200,304) GROUP BY source HAVING MAX(ts)>=?",
            (park_id, fresh_cutoff))}
    if not fresh_sources:
        return {}
    rows = db.execute(
        "SELECT ride_id, source, wait, state, ts FROM samples WHERE rowid IN "
        "(SELECT MAX(rowid) FROM samples WHERE park_id=? GROUP BY ride_id, "
        "source)", (park_id,)).fetchall()
    by_ride: dict[str, list[merge.Reading]] = {}
    for ride_id, source, wait, state, ts in rows:
        if source not in fresh_sources:
            continue
        by_ride.setdefault(ride_id, []).append(
            merge.Reading(source=source, wait=wait, state=state, ts=ts))
    return by_ride


def publish_live(db: sqlite3.Connection, publish_dir: str,
                 window_s: int = 1800) -> int:
    """Write ``{park}.live.json`` for every park with fresh readings from any
    source. Returns the number of parks written. UUID-keyed, waits + DOWN +
    CLOSED, with the merge provenance the app can surface."""
    os.makedirs(publish_dir, exist_ok=True)
    now = int(time.time())
    fresh_cutoff = now - window_s
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    parks = [r[0] for r in db.execute(
        "SELECT DISTINCT park_id FROM polls WHERE ts>=? AND status IN (200,304)",
        (fresh_cutoff,))]
    written = 0
    for park_id in parks:
        by_ride = _latest_per_source(db, park_id, fresh_cutoff)
        if not by_ride:
            continue
        merged = merge.merge_park(by_ride)
        if not merged:
            continue
        r_open, down, closed, prov = {}, [], [], {}
        used_sources: set[str] = set()
        for uuid, m in merged.items():
            used_sources.update(m.sources)
            if m.state == merge.OPERATING:
                if m.wait is not None:
                    r_open[uuid] = m.wait
                if m.outliers or m.flags:
                    prov[uuid] = {"sources": m.sources,
                                  "outliers": m.outliers, "flags": m.flags}
            elif m.state == merge.DOWN:
                down.append(uuid)
            elif m.state == merge.CLOSED:
                closed.append(uuid)
        doc = {"p": park_id, "generatedAt": now_iso, "t": now,
               "r": r_open, "sources": sorted(used_sources)}
        if down:
            doc["d"] = sorted(down)
        if closed:
            doc["c"] = sorted(closed)
        if prov:
            doc["prov"] = prov
        with open(os.path.join(publish_dir, f"{park_id}.live.json"), "w",
                  encoding="utf-8") as fh:
            json.dump(doc, fh, separators=(",", ":"))
        written += 1
    return written


def publish_showtimes(db: sqlite3.Connection, publish_dir: str,
                      window_s: int = 7200) -> int:
    """Write ``{park}.showtimes.json`` from the latest themeparks raw payload.

    Showtimes only come from themeparks.wiki (queue-times/parkqueuetimes don't
    carry them), and the base collector already stores the full gzipped payload
    in `raw`. We decode the newest one per park and emit the SHOW entities that
    have upcoming times, keyed by their themeparks UUID — so the app's Today
    screen reads them from the server instead of calling the live API.

    Each show is emitted in the app's `ThemeparksLiveDataDto` JSON shape so the
    client parses it with the existing `fromJson`. Returns parks written.
    """
    os.makedirs(publish_dir, exist_ok=True)
    now = int(time.time())
    cutoff = now - window_s
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    latest = db.execute(
        "SELECT park_id, MAX(ts) FROM raw WHERE ts>=? AND "
        "(source IS NULL OR source='themeparks') GROUP BY park_id",
        (cutoff,)).fetchall()
    written = 0
    for park_id, ts in latest:
        row = db.execute(
            "SELECT gz FROM raw WHERE park_id=? AND ts=? AND "
            "(source IS NULL OR source='themeparks') LIMIT 1",
            (park_id, ts)).fetchone()
        if not row:
            continue
        try:
            body = json.loads(gzip.decompress(row[0]).decode())
        except (OSError, ValueError):
            continue
        shows = []
        for item in body.get("liveData", []) or []:
            if (item.get("entityType") or "").upper() != "SHOW":
                continue
            times = item.get("showtimes") or []
            if not times:
                continue
            shows.append({
                "id": item.get("id"),
                "name": item.get("name"),
                "entityType": "SHOW",
                "status": item.get("status") or "OPERATING",
                "showtimes": [
                    {"type": s.get("type"), "startTime": s.get("startTime"),
                     "endTime": s.get("endTime")}
                    for s in times
                ],
            })
        if not shows:
            continue
        with open(os.path.join(publish_dir, f"{park_id}.showtimes.json"), "w",
                  encoding="utf-8") as fh:
            json.dump({"p": park_id, "generatedAt": now_iso, "shows": shows},
                      fh, separators=(",", ":"))
        written += 1
    return written
