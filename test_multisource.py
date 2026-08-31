#!/usr/bin/env python3
"""End-to-end unit test for multisource.py against in-memory SQLite + a fake
fetch (no network). Validates: schema migration, per-source scheduling,
native→UUID reconciliation, change-only ingest, and the merged live publish.

    python -m unittest test_multisource -v   # from docker/wait-history/
"""
from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import unittest

import idmap
import multisource


_BASE_SCHEMA = """
CREATE TABLE polls (park_id TEXT, ts INTEGER, status INTEGER, changed INTEGER);
CREATE TABLE samples (park_id TEXT, ride_id TEXT, ts INTEGER, wait INTEGER, state TEXT);
CREATE TABLE queues (park_id TEXT, ride_id TEXT, ts INTEGER, kind TEXT, value INTEGER, extra TEXT);
CREATE TABLE raw (park_id TEXT, ts INTEGER, gz BLOB);
"""

TP_PARK = "uuid-park-1"
RIDE_A = "uuid-ride-a"
RIDE_B = "uuid-ride-b"


class Throttle:
    penalty_s = 0.0


class MultiSourceTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.tmp.name
        # Point idmap at a temp maps dir with a queue_times map.
        self._orig_maps = idmap._MAPS_DIR
        idmap._MAPS_DIR = d
        _w(os.path.join(d, "queue_times.parkmap.json"),
           {"mappings": {TP_PARK: {"id": "77"}}})
        _w(os.path.join(d, "queue_times.ridemap.json"),
           {"mappings": {"101": RIDE_A, "102": RIDE_B}})
        os.environ["QUEUE_TIMES_ENABLED"] = "1"
        os.environ.pop("PARKQUEUETIMES_API_KEY", None)

        self.db = sqlite3.connect(":memory:")
        self.db.executescript(_BASE_SCHEMA)
        multisource.ensure_schema(self.db)

    def tearDown(self):
        idmap._MAPS_DIR = self._orig_maps
        self.db.close()
        self.tmp.cleanup()

    def _fetch(self, body):
        def fetch(url, etag):
            return 200, body, "etag-1"
        return fetch

    def test_schema_added_source_columns(self):
        for table in ("polls", "samples", "queues", "raw"):
            cols = {r[1] for r in self.db.execute(f"PRAGMA table_info({table})")}
            self.assertIn("source", cols)
        cols = {r[1] for r in self.db.execute("PRAGMA table_info(source_state)")}
        self.assertEqual(cols, {"park_id", "source", "ext_id", "etag",
                                "next_at", "last_open"})

    def test_seed_and_ingest_and_publish(self):
        added = multisource.seed_source_state(self.db, "queue_times")
        self.assertEqual(added, 1)

        body = {"lands": [{"rides": [
            {"id": 101, "name": "Ride A", "is_open": True, "wait_time": 30},
            {"id": 102, "name": "Ride B", "is_open": False, "wait_time": 0},
            {"id": 999, "name": "Unmapped", "is_open": True, "wait_time": 10},
        ]}]}
        polled = multisource.poll_due_sources(
            self.db, self._fetch(body), Throttle(), store_raw=True)
        self.assertEqual(polled, 1)

        # Reconciled to UUIDs; the unmapped ride 999 is dropped entirely.
        rows = self.db.execute(
            "SELECT ride_id, wait, state, source FROM samples ORDER BY ride_id"
        ).fetchall()
        self.assertEqual(rows, [
            (RIDE_A, 30, "OPERATING", "queue_times"),
            (RIDE_B, None, "CLOSED", "queue_times"),
        ])

        # Live publish merges into {park}.live.json (single source here).
        pub = os.path.join(self.tmp.name, "published")
        n = multisource.publish_live(self.db, pub)
        self.assertEqual(n, 1)
        with open(os.path.join(pub, f"{TP_PARK}.live.json")) as fh:
            doc = json.load(fh)
        self.assertEqual(doc["r"], {RIDE_A: 30})
        self.assertEqual(doc["c"], [RIDE_B])
        self.assertEqual(doc["sources"], ["queue_times"])

    def test_change_only_discipline(self):
        multisource.seed_source_state(self.db, "queue_times")
        body = {"lands": [{"rides": [
            {"id": 101, "name": "Ride A", "is_open": True, "wait_time": 30}]}]}
        # Force it due again by zeroing next_at between polls.
        multisource.poll_due_sources(self.db, self._fetch(body), Throttle())
        self.db.execute("UPDATE source_state SET next_at=0")
        multisource.poll_due_sources(self.db, self._fetch(body), Throttle())
        # Same reading twice → only ONE sample row (change-only).
        count = self.db.execute(
            "SELECT COUNT(*) FROM samples WHERE ride_id=?", (RIDE_A,)
        ).fetchone()[0]
        self.assertEqual(count, 1)
        # But two poll records (we looked twice).
        polls = self.db.execute("SELECT COUNT(*) FROM polls").fetchone()[0]
        self.assertEqual(polls, 2)

    def test_304_writes_poll_not_sample(self):
        multisource.seed_source_state(self.db, "queue_times")

        def fetch_304(url, etag):
            return 304, None, etag
        multisource.poll_due_sources(self.db, fetch_304, Throttle())
        self.assertEqual(
            self.db.execute("SELECT COUNT(*) FROM samples").fetchone()[0], 0)
        self.assertEqual(
            self.db.execute("SELECT status FROM polls").fetchone()[0], 304)

    def test_publish_showtimes_from_raw(self):
        import gzip
        import time
        body = {"liveData": [
            {"id": "show-1", "name": "Fireworks", "entityType": "SHOW",
             "status": "OPERATING",
             "showtimes": [{"type": "Performance Time",
                            "startTime": "2026-08-31T21:00:00Z"}]},
            {"id": "ride-1", "name": "Coaster", "entityType": "ATTRACTION",
             "status": "OPERATING", "queue": {"STANDBY": {"waitTime": 20}}},
        ]}
        gz = gzip.compress(json.dumps(body).encode())
        self.db.execute(
            "INSERT INTO raw (park_id, ts, gz, source) VALUES (?,?,?,?)",
            (TP_PARK, int(time.time()), gz, "themeparks"))
        self.db.commit()

        pub = os.path.join(self.tmp.name, "published")
        n = multisource.publish_showtimes(self.db, pub)
        self.assertEqual(n, 1)
        with open(os.path.join(pub, f"{TP_PARK}.showtimes.json")) as fh:
            doc = json.load(fh)
        self.assertEqual(len(doc["shows"]), 1)  # ride dropped, show kept
        self.assertEqual(doc["shows"][0]["id"], "show-1")
        self.assertEqual(doc["shows"][0]["showtimes"][0]["startTime"],
                         "2026-08-31T21:00:00Z")

    def test_disabled_source_not_polled(self):
        os.environ["QUEUE_TIMES_ENABLED"] = "0"
        multisource.seed_source_state(self.db, "queue_times")
        polled = multisource.poll_due_sources(
            self.db, self._fetch({}), Throttle())
        self.assertEqual(polled, 0)

    def test_daily_budget_caps_a_source(self):
        # Seed a source that is already at its daily cap via existing `polls`
        # rows, then confirm poll_due_sources refuses to poll it further.
        os.environ["QUEUE_TIMES_MAX_PER_DAY"] = "3"
        try:
            multisource.seed_source_state(self.db, "queue_times")
            import time as _t
            now = int(_t.time())
            self.db.executemany(
                "INSERT INTO polls (park_id, ts, status, changed, source) "
                "VALUES (?,?,200,0,'queue_times')",
                [(TP_PARK, now)] * 3)  # already spent the day's budget
            self.db.commit()
            body = {"lands": [{"rides": [
                {"id": 101, "name": "A", "is_open": True, "wait_time": 5}]}]}
            polled = multisource.poll_due_sources(
                self.db, self._fetch(body), Throttle())
            self.assertEqual(polled, 0)  # budget exhausted → no new poll
        finally:
            os.environ.pop("QUEUE_TIMES_MAX_PER_DAY", None)

    def test_within_budget_still_polls(self):
        os.environ["QUEUE_TIMES_MAX_PER_DAY"] = "100"
        try:
            multisource.seed_source_state(self.db, "queue_times")
            body = {"lands": [{"rides": [
                {"id": 101, "name": "A", "is_open": True, "wait_time": 5}]}]}
            polled = multisource.poll_due_sources(
                self.db, self._fetch(body), Throttle())
            self.assertEqual(polled, 1)
        finally:
            os.environ.pop("QUEUE_TIMES_MAX_PER_DAY", None)


def _w(path, obj):
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(obj, fh)


if __name__ == "__main__":
    unittest.main()


class ParkQueueTimesWaitFieldTest(unittest.TestCase):
    """The live API returns `waitMinutes`. It was absent from the fallback
    chain, so every ride classified OPERATING with wait=None — which merge
    then excluded, silently degrading every ride to `single_source` while
    the collector still burned the parkqueuetimes budget."""

    def test_waitMinutes_is_parsed(self):
        body = {"data": {"parkId": 31, "rides": [
            {"id": 1193, "name": "Silver Star", "status": "OPERATING",
             "waitMinutes": 10, "lastUpdated": None}]}}
        self.assertEqual(multisource._pqt_classify(body),
                         {"1193": (10, "OPERATING")})

    def test_legacy_wait_field_names_still_work(self):
        for field in ("wait_time", "waitTime", "wait"):
            body = {"data": {"rides": [
                {"id": 7, "status": "OPERATING", field: 25}]}}
            with self.subTest(field=field):
                self.assertEqual(multisource._pqt_classify(body),
                                 {"7": (25, "OPERATING")})
