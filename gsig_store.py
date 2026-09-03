#!/usr/bin/env python3
"""Durable, restart-safe storage for the g-force signature aggregation.

Lives in its OWN SQLite file (default ``/data/gsig.db``), decoupled from the
main wait-history DB so the ingest endpoint and the collector's poll loop never
contend for the same writer. Each ``{park, attraction}`` occupies one row whose
``data`` blob is the serialized :class:`gsig_merge.RideAggregate` (fixed-size
per cluster, ≤3 clusters) — so the table is O(#attractions), flat over time.

``publish_all`` writes the per-park ``{park}.gsig.json`` bundles into
``PUBLISH_DIR/gsig/`` for the static web server to serve; only clusters with
``n ≥ K_MIN`` are published (never a single-person profile). Pure + stdlib.
"""
from __future__ import annotations

import json
import os
import sqlite3

import gsig_merge as gm

_TABLE_SQL = (
    "CREATE TABLE IF NOT EXISTS gsig_agg("
    "park TEXT NOT NULL, attraction TEXT NOT NULL, data TEXT NOT NULL, "
    "updated_at INTEGER NOT NULL DEFAULT 0, "
    "PRIMARY KEY(park, attraction))"
)

# Reject absurd payloads early (a signature is ~1.5 KB; 100 KB is generous).
MAX_PAYLOAD_BYTES = 100_000


def open_db(path):
    conn = sqlite3.connect(path, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute(_TABLE_SQL)
    conn.commit()
    return conn


def _valid_sig(sig):
    return (isinstance(sig, dict)
            and len(sig.get("v", [])) == gm.ENV_LEN
            and len(sig.get("l", [])) == gm.ENV_LEN
            and len(sig.get("f", [])) == gm.FEAT_LEN)


def ingest(conn, record, now=0):
    """Fold one upload into its ride aggregate. Returns True when accepted.

    ``record`` = {"p": park, "a": attraction, "an"?: name, "sig": {"v","l","f"}}.
    """
    if not isinstance(record, dict):
        return False
    sig = record.get("sig")
    if not _valid_sig(sig) or "p" not in record or "a" not in record:
        return False
    park, attr = record["p"], record["a"]
    row = conn.execute(
        "SELECT data FROM gsig_agg WHERE park=? AND attraction=?",
        (park, attr)).fetchone()
    if row:
        ride = gm.RideAggregate.from_dict(json.loads(row[0]))
        if ride.attraction_name is None and record.get("an"):
            ride.attraction_name = record["an"]
    else:
        ride = gm.RideAggregate(park, attr, record.get("an"))
    ride.fold(sig)
    conn.execute(
        "INSERT OR REPLACE INTO gsig_agg(park, attraction, data, updated_at) "
        "VALUES(?,?,?,?)",
        (park, attr, json.dumps(ride.to_dict(), separators=(",", ":")), now))
    conn.commit()
    return True


def publish_all(conn, publish_dir):
    """Write `{park}.gsig.json` for every park that has ≥1 publishable profile.
    Returns the number of park bundles written."""
    outdir = os.path.join(publish_dir, "gsig")
    os.makedirs(outdir, exist_ok=True)

    by_park = {}
    for park, _attr, data in conn.execute(
            "SELECT park, attraction, data FROM gsig_agg"):
        by_park.setdefault(park, []).append(
            gm.RideAggregate.from_dict(json.loads(data)))

    written = 0
    for park, rides in by_park.items():
        profiles = []
        for ride in rides:
            published = [c for c in ride.clusters if c.n >= gm.K_MIN]
            for i, c in enumerate(published):
                profiles.append(gm._profile_dict(ride, c, i))
        if not profiles:
            continue
        tmp = os.path.join(outdir, "%s.gsig.json.tmp" % park)
        dst = os.path.join(outdir, "%s.gsig.json" % park)
        with open(tmp, "w") as fh:
            json.dump({"schema": gm.SCHEMA, "profiles": profiles}, fh,
                      separators=(",", ":"))
        os.replace(tmp, dst)  # atomic
        written += 1
    return written
