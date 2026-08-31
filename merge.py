#!/usr/bin/env python3
"""Median/consensus merge of a ride's wait across multiple sources.

When themeparks.wiki, wartezeiten.app, queue-times.com and parkqueuetimes.com
all report the same (reconciled) ride, they rarely agree to the minute. This
reduces their readings to one number the app shows, favouring the CONSENSUS:

  * **Status** — majority vote across sources. Ties break toward OPERATING
    when at least one source posts a numeric wait (someone seeing the ride
    run outweighs a stale "closed"); otherwise toward the more-severe state.
  * **Wait** (only when the merged status is OPERATING) — the **median** of
    the operating sources' numeric waits. Median is robust: one wild source
    can't drag it. With an even count the median is the mean of the two
    middle values, rounded.
  * **Outlier flag** — any source outside ``median ± max(5, ceil(0.15·median))``
    is recorded (surfaced in the live file's provenance, and useful for
    spotting a broken source), but it does NOT change the merged value.

Degradation is explicit: with a single source the "median" is that value and
the merge is flagged ``single_source`` (no consensus possible); with two it is
their mean. The collector filters readings to a freshness window BEFORE
calling here — this module is pure and deterministic (no clock).
"""
from __future__ import annotations

from dataclasses import dataclass, field

OPERATING = "OPERATING"
DOWN = "DOWN"
CLOSED = "CLOSED"

# Lower = more severe, used only to break a status tie when NO source posts a
# numeric wait.
_SEVERITY = {CLOSED: 0, DOWN: 1, OPERATING: 2}


@dataclass(frozen=True)
class Reading:
    """One source's current view of a ride."""
    source: str
    wait: int | None
    state: str
    ts: int = 0  # epoch seconds; used only to pick latest-per-source


@dataclass
class Merged:
    wait: int | None
    state: str
    sources: list[str] = field(default_factory=list)   # sources that agreed
    outliers: list[str] = field(default_factory=list)   # sources out of band
    flags: list[str] = field(default_factory=list)      # e.g. single_source


def latest_per_source(readings: list[Reading]) -> list[Reading]:
    """Collapse multiple readings from the same source to its newest."""
    newest: dict[str, Reading] = {}
    for r in readings:
        cur = newest.get(r.source)
        if cur is None or r.ts >= cur.ts:
            newest[r.source] = r
    return list(newest.values())


def _median(values: list[int]) -> int:
    s = sorted(values)
    n = len(s)
    mid = n // 2
    if n % 2 == 1:
        return s[mid]
    return round((s[mid - 1] + s[mid]) / 2)


def _merge_state(readings: list[Reading]) -> str:
    counts: dict[str, int] = {}
    for r in readings:
        counts[r.state] = counts.get(r.state, 0) + 1
    top = max(counts.values())
    winners = [st for st, c in counts.items() if c == top]
    if len(winners) == 1:
        return winners[0]
    # Tie: prefer OPERATING if anyone posts a real wait, else most severe.
    if OPERATING in winners and any(
            r.state == OPERATING and r.wait is not None for r in readings):
        return OPERATING
    return min(winners, key=lambda st: _SEVERITY.get(st, 99))


def merge_ride(readings: list[Reading]) -> Merged | None:
    """Reduce one ride's cross-source readings to a single merged value.

    Returns None when there are no readings at all.
    """
    readings = latest_per_source(readings)
    if not readings:
        return None

    state = _merge_state(readings)
    if state != OPERATING:
        # Not operating → no wait number; report which sources concurred.
        agree = sorted(r.source for r in readings if r.state == state)
        return Merged(wait=None, state=state, sources=agree)

    op = [r for r in readings if r.state == OPERATING and r.wait is not None]
    if not op:
        # Operating but nobody posts a standby wait (show / virtual-queue).
        agree = sorted(r.source for r in readings if r.state == OPERATING)
        return Merged(wait=None, state=OPERATING, sources=agree)

    values = [r.wait for r in op]  # type: ignore[misc]
    med = _median(values)
    band = max(5, -(-int(med * 15) // 100))  # ceil(0.15*med), floor 5
    sources, outliers = [], []
    for r in op:
        (outliers if abs(r.wait - med) > band else sources).append(r.source)  # type: ignore[operator]
    merged = Merged(wait=med, state=OPERATING,
                    sources=sorted(sources), outliers=sorted(outliers))
    if len(op) == 1:
        merged.flags.append("single_source")
    elif len(op) == 2:
        merged.flags.append("two_source_mean")
    return merged


def merge_park(
    readings_by_ride: dict[str, list[Reading]],
) -> dict[str, Merged]:
    """Merge every ride in a park. Rides with no readings are omitted."""
    out: dict[str, Merged] = {}
    for uuid, readings in readings_by_ride.items():
        m = merge_ride(readings)
        if m is not None:
            out[uuid] = m
    return out
