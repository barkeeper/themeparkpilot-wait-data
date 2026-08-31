#!/usr/bin/env python3
"""Cross-source ID reconciliation for the multi-source collector.

Every upstream (themeparks.wiki, wartezeiten.app, queue-times.com,
parkqueuetimes.com) has its OWN park and ride IDs. The app — and the whole
Drift catalog it ships — is keyed on **themeparks.wiki entity UUIDs**. So a
wait number from queue-times is useless until we know which themeparks UUID
it belongs to. This module resolves that, server-side, before publish.

Two matchers, faithful ports of the app's proven Dart code so the server and
the client agree on what "the same ride" means:

  * ``match_rides`` — attraction-name matcher. Mirrors
    ``lib/data/services/wartezeiten_attraction_match.dart``: lowercase +
    diacritic-fold + strip every non-alphanumeric char, then
    ``1 - levenshtein/maxlen`` with a 0.7 cutoff, greedy best-per-row.
  * ``park_similarity`` — park-name matcher. Mirrors
    ``tools/fetch_wartezeiten_park_map.dart``: word-set Jaccard over a
    stop-word-stripped normalisation, 0.6 threshold at the call site.

At runtime the collector loads the pre-generated maps (built offline by
``build_source_maps.py`` and hand-curated) rather than matching live, so a
mis-map is caught in review, not shipped silently. ``match_rides`` stays here
because the map builder uses it and because a park with no ride-map entry can
fall back to a live name-match against the ``entities`` table.
"""
from __future__ import annotations

import json
import os
from typing import Iterable

# ---------------------------------------------------------------------------
# Normalisation — two variants, each faithful to the Dart source it mirrors.
# ---------------------------------------------------------------------------

# Attraction fold (wartezeiten_attraction_match.dart::_stripped).
_ATTR_FOLD = {
    "ä": "a", "ö": "o", "ü": "u", "ß": "ss",
    "á": "a", "é": "e", "è": "e", "ê": "e",
    "í": "i", "ï": "i", "ñ": "n", "ó": "o",
    "ú": "u", "ç": "c",
}

# Park fold (fetch_wartezeiten_park_map.dart::_normalise) — a slightly
# smaller set; kept separate so behaviour matches the app exactly.
_PARK_FOLD = {
    "ä": "a", "ö": "o", "ü": "u", "ß": "ss",
    "é": "e", "è": "e", "ê": "e", "í": "i",
    "ï": "i", "ñ": "n", "ç": "c",
}

_PARK_STOPWORDS = {"park", "resort", "theme", "amusement", "land", "world"}


def strip_name(s: str) -> str:
    """Attraction normalisation: lowercase, diacritic-fold, drop every
    non-``[a-z0-9]`` character (spaces, dashes, apostrophes, em-dashes)."""
    out = s.lower()
    for src, dst in _ATTR_FOLD.items():
        out = out.replace(src, dst)
    return "".join(ch for ch in out if ch.isascii() and ch.isalnum())


def normalise_park(s: str) -> str:
    """Park normalisation: lowercase, fold, keep only ``[a-z0-9 ]``, strip
    discriminating-stopwords, collapse whitespace."""
    out = s.lower()
    for src, dst in _PARK_FOLD.items():
        out = out.replace(src, dst)
    out = "".join(ch if (ch.isascii() and ch.isalnum()) else " " for ch in out)
    words = [w for w in out.split() if w not in _PARK_STOPWORDS]
    return " ".join(words)


def levenshtein(a: str, b: str) -> int:
    """Two-row Levenshtein — same algorithm as the Dart ``_levenshtein``."""
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)
    prev = list(range(len(b) + 1))
    curr = [0] * (len(b) + 1)
    for i in range(1, len(a) + 1):
        curr[0] = i
        for j in range(1, len(b) + 1):
            cost = 0 if a[i - 1] == b[j - 1] else 1
            v = prev[j - 1] + cost
            if curr[j - 1] + 1 < v:
                v = curr[j - 1] + 1
            if prev[j] + 1 < v:
                v = prev[j] + 1
            curr[j] = v
        prev, curr = curr, prev
    return prev[len(b)]


def park_similarity(a: str, b: str) -> float:
    """Word-set Jaccard on normalised park names. 1.0 on an exact
    normalised match; 0.0 when either side is empty after normalisation."""
    na, nb = normalise_park(a), normalise_park(b)
    if na == nb and na:
        return 1.0
    aw, bw = set(na.split()), set(nb.split())
    if not aw or not bw:
        return 0.0
    return len(aw & bw) / len(aw | bw)


def match_rides(
    targets: list[tuple[str, str]],
    source_rows: Iterable[tuple[str, str]],
    cutoff: float = 0.7,
) -> dict[str, tuple[str, float]]:
    """Map each source ride to its best themeparks UUID by fuzzy name.

    ``targets``     — themeparks entities as ``(uuid, name)``.
    ``source_rows`` — the source's rides as ``(source_ride_id, name)``.

    Returns ``{source_ride_id: (uuid, score)}`` keeping only matches at or
    above ``cutoff``. Greedy: each source row goes to its single best target,
    ties broken by raw Levenshtein — identical to the Dart ``matchByName``.
    Unmatched rows are simply absent (never guessed).
    """
    norm_targets = [(uuid, strip_name(name)) for uuid, name in targets]
    out: dict[str, tuple[str, float]] = {}
    for src_id, src_name in source_rows:
        src_stripped = strip_name(src_name)
        if not src_stripped:
            continue
        best_uuid: str | None = None
        best_score = 0.0
        best_dist = 1 << 31
        for uuid, tgt_stripped in norm_targets:
            if not tgt_stripped:
                continue
            dist = levenshtein(src_stripped, tgt_stripped)
            max_len = max(len(src_stripped), len(tgt_stripped))
            score = 1.0 - (dist / max_len)
            if score < cutoff:
                continue
            if score > best_score or (score == best_score and dist < best_dist):
                best_score = score
                best_uuid = uuid
                best_dist = dist
        if best_uuid is not None:
            out[src_id] = (best_uuid, round(best_score, 3))
    return out


# ---------------------------------------------------------------------------
# Runtime map loading — the collector consumes the curated maps, not live
# matching. maps/<source>.parkmap.json / .ridemap.json + maps/overrides.json.
# ---------------------------------------------------------------------------

_MAPS_DIR = os.environ.get(
    "MAPS_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), "maps")
)


class SourceMap:
    """The park + ride mapping for one upstream source, loaded from disk.

    ``parkmap``: themeparks park UUID -> source park id (string).
    ``ridemap``: source ride id -> themeparks entity UUID.
    Overrides in ``maps/overrides.json`` (``{source: {ridemap|parkmap: {...}}}``)
    win over the generated files, so a hand-fix survives a regeneration.
    """

    def __init__(self, source: str, parkmap: dict, ridemap: dict) -> None:
        self.source = source
        self.parkmap = parkmap        # tp_park_uuid -> source_park_id
        self.ridemap = ridemap        # source_ride_id -> tp_entity_uuid

    def source_park_for(self, tp_park_uuid: str) -> str | None:
        entry = self.parkmap.get(tp_park_uuid)
        if isinstance(entry, dict):
            return entry.get("id")
        return entry

    def uuid_for_ride(self, source_ride_id: str) -> str | None:
        return self.ridemap.get(source_ride_id)


def _load_json(path: str) -> dict:
    try:
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return {}


def load_source_map(source: str, maps_dir: str | None = None) -> SourceMap:
    """Load ``<source>.parkmap.json`` + ``<source>.ridemap.json`` and fold in
    any ``overrides.json`` entries for this source. Missing files → empty
    maps (the source simply contributes nothing until its map exists)."""
    d = maps_dir or _MAPS_DIR
    parkmap = _load_json(os.path.join(d, f"{source}.parkmap.json")).get(
        "mappings", {}
    )
    ridemap = _load_json(os.path.join(d, f"{source}.ridemap.json")).get(
        "mappings", {}
    )
    overrides = _load_json(os.path.join(d, "overrides.json")).get(source, {})
    parkmap = {**parkmap, **overrides.get("parkmap", {})}
    ridemap = {**ridemap, **overrides.get("ridemap", {})}
    return SourceMap(source, parkmap, ridemap)
