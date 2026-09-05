#!/usr/bin/env python3
"""Bounded, streaming aggregation of g-force ride-signatures → per-ride
reference profiles (the ride-fingerprint crowd library).

The app uploads only a **derived, non-reversible** signature (128-pt signed-
vertical + lateral envelopes + a 12-feature vector) on a *confirmed* match —
never the raw motion trace. This module folds each upload into ONE fixed-size
profile per ``{park, attraction, variant}`` and then **discards it**. There is
no per-recording log, so total storage is O(#attractions × ≤3 variants), flat
over time no matter how many rides are ever recorded (see the plan's storage
budget). Profiles sharpen as ``n`` grows (variance shrinks → the matcher gets
more discriminating + better-calibrated).

Design (owner decisions, 2026-09-02):
  * **Fold-and-discard** streaming Welford mean/variance per dimension.
  * **Variant clustering, ≤3** — a genuinely different mode (forward/backward,
    dueling side) that lands far (> ``SPLIT_GATE``) from every existing cluster
    spawns a new one, up to 3; most rides stay single-``default``.
  * **Reservoir** (M=24) of recent envelopes per cluster — kept for drift
    detection / outlier context; bounded.
  * **Outlier rejection** — an upload that is absurdly far (> ``OUTLIER_GATE``)
    from every cluster when no slot is free is dropped, not folded.
  * **K≥5 gate** — a cluster is only published once ≥5 contributors folded in
    (never a single-person profile).

Pure + stdlib-only + deterministic (no clock, no I/O). The output dict matches
the Dart ``GForceRefProfile`` bundle so ``{park}.gsig.json`` decodes directly.
"""
from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field

ENV_LEN = 128
FEAT_LEN = 12
SCHEMA = 1

K_MIN = 5              # min contributors before a variant profile is published
MAX_VARIANTS = 3      # forward / backward / dueling — never more
RESERVOIR_M = 24
SPLIT_GATE = 0.6      # envelope RMS (g) beyond which a signature is a new mode
OUTLIER_GATE = 2.0    # envelope RMS (g) beyond which an upload is junk


def _rms(a, b):
    """Normalized L2 (RMS) distance between two equal-length vectors."""
    s = 0.0
    for x, y in zip(a, b):
        d = x - y
        s += d * d
    return math.sqrt(s / len(a))


class _Welford:
    """Per-dimension streaming mean + M2 (→ variance) over a fixed-length vec."""

    __slots__ = ("n", "mean", "m2")

    def __init__(self, length):
        self.n = 0
        self.mean = [0.0] * length
        self.m2 = [0.0] * length

    def update(self, vec):
        self.n += 1
        for i, x in enumerate(vec):
            d = x - self.mean[i]
            self.mean[i] += d / self.n
            self.m2[i] += d * (x - self.mean[i])

    def variance(self):
        if self.n == 0:
            return [0.0] * len(self.mean)
        return [m / self.n for m in self.m2]  # population variance (matches Dart)

    def to_dict(self):
        return {"n": self.n, "mean": self.mean, "m2": self.m2}

    @classmethod
    def from_dict(cls, d):
        w = cls(len(d["mean"]))
        w.n = d["n"]
        w.mean = list(d["mean"])
        w.m2 = list(d["m2"])
        return w


class VariantCluster:
    """One mode of a ride: streaming stats over envelopes + features, plus a
    bounded reservoir. Fixed memory regardless of how many fold in."""

    def __init__(self):
        self.envv = _Welford(ENV_LEN)
        self.envl = _Welford(ENV_LEN)
        self.feat = _Welford(FEAT_LEN)
        self.reservoir = deque(maxlen=RESERVOIR_M)

    @property
    def n(self):
        return self.envv.n

    @property
    def centroid(self):
        return self.envv.mean

    def fold(self, sig):
        self.envv.update(sig["v"])
        self.envl.update(sig["l"])
        self.feat.update(sig["f"])
        self.reservoir.append(sig["v"])

    def to_dict(self):
        return {
            "vv": self.envv.to_dict(),
            "vl": self.envl.to_dict(),
            "vf": self.feat.to_dict(),
            "res": list(self.reservoir),
        }

    @classmethod
    def from_dict(cls, d):
        c = cls()
        c.envv = _Welford.from_dict(d["vv"])
        c.envl = _Welford.from_dict(d["vl"])
        c.feat = _Welford.from_dict(d["vf"])
        c.reservoir = deque(d.get("res", []), maxlen=RESERVOIR_M)
        return c


class RideAggregate:
    """All variant clusters for one ``{park, attraction}``."""

    def __init__(self, park_id, attraction_id, attraction_name=None):
        self.park_id = park_id
        self.attraction_id = attraction_id
        self.attraction_name = attraction_name
        self.clusters = []  # list[VariantCluster], ≤ MAX_VARIANTS

    def fold(self, sig):
        if self.clusters:
            dists = [(_rms(sig["v"], c.centroid), c) for c in self.clusters]
            best_dist, best = min(dists, key=lambda t: t[0])
        else:
            best_dist, best = float("inf"), None

        if best is not None and best_dist <= SPLIT_GATE:
            best.fold(sig)
        elif len(self.clusters) < MAX_VARIANTS:
            c = VariantCluster()
            c.fold(sig)
            self.clusters.append(c)
        elif best_dist <= OUTLIER_GATE:
            best.fold(sig)  # forced into nearest (slots full, still plausible)
        # else: outlier — dropped.

    def to_dict(self):
        return {
            "p": self.park_id,
            "a": self.attraction_id,
            "an": self.attraction_name,
            "cl": [c.to_dict() for c in self.clusters],
        }

    @classmethod
    def from_dict(cls, d):
        r = cls(d["p"], d["a"], d.get("an"))
        r.clusters = [VariantCluster.from_dict(c) for c in d.get("cl", [])]
        return r


class GSigStore:
    """The whole library: one [RideAggregate] per (park, attraction)."""

    def __init__(self):
        self._rides = {}  # (park, attr) -> RideAggregate

    def ingest(self, record):
        """Fold one upload. ``record`` = {"p","a","an"?, "sig":{"v","l","f"}}."""
        sig = record["sig"]
        if (len(sig.get("v", [])) != ENV_LEN
                or len(sig.get("l", [])) != ENV_LEN
                or len(sig.get("f", [])) != FEAT_LEN):
            return  # malformed — ignore
        key = (record["p"], record["a"])
        ride = self._rides.get(key)
        if ride is None:
            ride = RideAggregate(record["p"], record["a"], record.get("an"))
            self._rides[key] = ride
        elif ride.attraction_name is None and record.get("an"):
            ride.attraction_name = record["an"]
        ride.fold(sig)

    def profiles_for_park(self, park_id):
        """Published (n≥K) profiles for a park, in bundle-profile dict form."""
        out = []
        for (pk, _attr), ride in self._rides.items():
            if pk != park_id:
                continue
            published = [c for c in ride.clusters if c.n >= K_MIN]
            for i, c in enumerate(published):
                out.append(_profile_dict(ride, c, i))
        return out

    def publish_park(self, park_id):
        """A `{park}.gsig.json` bundle for a park."""
        return {"schema": SCHEMA, "profiles": self.profiles_for_park(park_id)}


_VARIANT_LABELS = ["default", "v2", "v3"]


def _profile_dict(ride, cluster, index):
    d = {
        "a": ride.attraction_id,
        "p": ride.park_id,
        "var": _VARIANT_LABELS[index] if index < len(_VARIANT_LABELS)
        else "v%d" % (index + 1),
        "n": cluster.n,
        "vm": cluster.envv.mean,
        "vv": cluster.envv.variance(),
        "lm": cluster.envl.mean,
        "lv": cluster.envl.variance(),
        "fm": cluster.feat.mean,
        "fv": cluster.feat.variance(),
    }
    if ride.attraction_name:
        d["an"] = ride.attraction_name
    return d
