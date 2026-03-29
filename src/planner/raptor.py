"""
Transit routing: time-dependent label setting with walk + scheduled ride legs.

Uses GTFS transfers for walking, stop_times for vehicles, and a minimum transfer
buffer when boarding after a ride.
"""

from __future__ import annotations

import heapq
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple, Union

from planner.preprocess import RaptorContext, TripTimetable

INF = 10**18


@dataclass
class PWalk:
    prev_key: Tuple[str, int]
    t_start: int
    t_end: int
    from_stop: str
    to_stop: str


@dataclass
class PRide:
    prev_key: Tuple[str, int]
    trip_id: str
    board_idx: int
    alight_idx: int
    t_board: int
    t_alight: int


ParentEntry = Union[PWalk, PRide]


@dataclass
class LegSummary:
    mode: str  # 'walk' | 'ride'
    from_stop: str
    to_stop: str
    start_sec: int
    end_sec: int
    trip_id: Optional[str] = None
    route_id: Optional[str] = None
    headsign: Optional[str] = None
    route_short_name: Optional[str] = None
    board_stop_idx: Optional[int] = None
    alight_stop_idx: Optional[int] = None


@dataclass
class Journey:
    arrival_stop: str
    arrival_sec: int
    vehicle_legs: int
    legs: List[LegSummary]
    total_walk_sec: int


def _relax_walk(
    ctx: RaptorContext,
    t: int,
    s: str,
    lg: int,
    best: Dict[Tuple[str, int], int],
    parent: Dict[Tuple[str, int], ParentEntry],
    q: List[Tuple[int, str, int]],
) -> None:
    for nb, w in ctx.footpaths.get(s, []):
        nt = t + w
        key = (nb, lg)
        if nt < best.get(key, INF):
            best[key] = nt
            parent[key] = PWalk(prev_key=(s, lg), t_start=t, t_end=nt, from_stop=s, to_stop=nb)
            heapq.heappush(q, (nt, nb, lg))


def _relax_rides(
    ctx: RaptorContext,
    t: int,
    s: str,
    lg: int,
    min_transfer_sec: int,
    max_vehicle_legs: int,
    best: Dict[Tuple[str, int], int],
    parent: Dict[Tuple[str, int], ParentEntry],
    q: List[Tuple[int, str, int]],
) -> None:
    xfer = 0 if lg == 0 else min_transfer_sec
    cutoff = t + xfer
    if lg >= max_vehicle_legs:
        return
    nlg = lg + 1
    for tr, idx in ctx.board_at.get(s, []):
        if tr.dep_s[idx] < cutoff:
            continue
        for j in range(idx + 1, len(tr.stop_ids)):
            sj = tr.stop_ids[j]
            arr = tr.arr_s[j]
            key = (sj, nlg)
            if arr < best.get(key, INF):
                best[key] = arr
                parent[key] = PRide(
                    prev_key=(s, lg),
                    trip_id=tr.trip_id,
                    board_idx=idx,
                    alight_idx=j,
                    t_board=tr.dep_s[idx],
                    t_alight=arr,
                )
                heapq.heappush(q, (arr, sj, nlg))


def run_routing(
    ctx: RaptorContext,
    origin_stops: Set[str],
    target_stops: Set[str],
    dep_sec: int,
    *,
    min_transfer_sec: int = 90,
    max_vehicle_legs: int = 12,
    max_pareto: int = 5,
) -> List[Journey]:
    best: Dict[Tuple[str, int], int] = {}
    parent: Dict[Tuple[str, int], ParentEntry] = {}
    q: List[Tuple[int, str, int]] = []

    for o in origin_stops:
        heapq.heappush(q, (dep_sec, o, 0))
        if dep_sec < best.get((o, 0), INF):
            best[(o, 0)] = dep_sec

    while q:
        t, s, lg = heapq.heappop(q)
        key = (s, lg)
        if t != best.get(key, INF):
            continue

        _relax_walk(ctx, t, s, lg, best, parent, q)
        _relax_rides(ctx, t, s, lg, min_transfer_sec, max_vehicle_legs, best, parent, q)

    candidates: List[Tuple[int, int, str]] = []
    for (stop, legs), tarr in best.items():
        if stop in target_stops:
            candidates.append((tarr, legs, stop))

    def dominates(a: Tuple[int, int, str], b: Tuple[int, int, str]) -> bool:
        ta, la, _ = a
        tb, lb, _ = b
        return (ta <= tb and la <= lb) and (ta < tb or la < lb)

    pareto: List[Tuple[int, int, str]] = []
    for c in sorted(candidates, key=lambda x: (x[0], x[1])):
        if any(dominates(p, c) for p in pareto):
            continue
        pareto = [p for p in pareto if not dominates(c, p)]
        pareto.append(c)

    pareto.sort(key=lambda x: (x[0], x[1]))
    journeys: List[Journey] = []
    for tarr, legs, stop in pareto[:max_pareto]:
        j = _reconstruct(ctx, parent, stop, legs, tarr)
        if j:
            journeys.append(j)
    return journeys


def _find_trip(ctx: RaptorContext, trip_id: str) -> Optional[TripTimetable]:
    for trs in ctx.trips_by_route.values():
        for tr in trs:
            if tr.trip_id == trip_id:
                return tr
    return None


def _reconstruct(
    ctx: RaptorContext,
    parent: Dict[Tuple[str, int], ParentEntry],
    stop: str,
    legs: int,
    arrival_sec: int,
) -> Optional[Journey]:
    rev: List[LegSummary] = []
    walk_total = 0
    v_legs = 0
    cur: Tuple[str, int] = (stop, legs)

    if cur not in parent:
        return Journey(
            arrival_stop=stop,
            arrival_sec=arrival_sec,
            vehicle_legs=0,
            legs=[],
            total_walk_sec=0,
        )

    while cur in parent:
        e = parent[cur]
        if isinstance(e, PWalk):
            walk_total += e.t_end - e.t_start
            rev.append(
                LegSummary(
                    mode="walk",
                    from_stop=e.from_stop,
                    to_stop=e.to_stop,
                    start_sec=e.t_start,
                    end_sec=e.t_end,
                )
            )
            cur = e.prev_key
        else:
            tr = _find_trip(ctx, e.trip_id)
            if not tr:
                return None
            v_legs += 1
            short_n, _, _ = ctx.route_meta.get(tr.route_id, ("", "", ""))
            rev.append(
                LegSummary(
                    mode="ride",
                    from_stop=tr.stop_ids[e.board_idx],
                    to_stop=tr.stop_ids[e.alight_idx],
                    start_sec=e.t_board,
                    end_sec=e.t_alight,
                    trip_id=tr.trip_id,
                    route_id=tr.route_id,
                    headsign=tr.headsign,
                    route_short_name=short_n or None,
                    board_stop_idx=e.board_idx,
                    alight_stop_idx=e.alight_idx,
                )
            )
            cur = e.prev_key

    if not rev:
        return None
    rev.reverse()
    arrival_sec = rev[-1].end_sec
    return Journey(
        arrival_stop=stop,
        arrival_sec=arrival_sec,
        vehicle_legs=v_legs,
        legs=rev,
        total_walk_sec=walk_total,
    )
