"""
Transit routing: round-based RAPTOR-style transit legs + GTFS transfer walks.

Uses ``transfers.txt`` footpaths, scheduled trips from preprocess, and the same
minimum transfer buffer as before when boarding after a ride (no buffer before
the first vehicle leg).

Complexity is roughly O(K * sum_trip_stops) per query instead of Dijkstra-style
``heap push per downstream stop`` growth on busy hubs.
"""

from __future__ import annotations

from collections import deque
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


def _relax_foot_layer_seeded(
    ctx: RaptorContext,
    lg: int,
    best: Dict[Tuple[str, int], int],
    parent: Dict[Tuple[str, int], ParentEntry],
    seeds: Set[str],
) -> Set[str]:
    """Relax transfers at leg ``lg`` starting from ``seeds``; return stops whose label improved."""
    active = {s for s in seeds if best.get((s, lg), INF) < INF}
    if not active:
        return set()
    updated: Set[str] = set(active)
    q: deque[Tuple[str, int]] = deque((s, best[(s, lg)]) for s in active)
    while q:
        s, t = q.popleft()
        if best.get((s, lg), INF) != t:
            continue
        for nb, w in ctx.footpaths.get(s, []):
            nt = t + w
            key = (nb, lg)
            if nt < best.get(key, INF):
                best[key] = nt
                parent[key] = PWalk(
                    prev_key=(s, lg),
                    t_start=t,
                    t_end=nt,
                    from_stop=s,
                    to_stop=nb,
                )
                updated.add(nb)
                q.append((nb, nt))
    return updated


def _route_scan_marked_routes(
    ctx: RaptorContext,
    lg: int,
    min_transfer_sec: int,
    max_vehicle_legs: int,
    marked_route_ids: Set[str],
    best: Dict[Tuple[str, int], int],
    parent: Dict[Tuple[str, int], ParentEntry],
) -> Set[str]:
    """One RAPTOR vehicle round on ``marked_route_ids`` only; return alight stops improved at lg+1."""
    if lg >= max_vehicle_legs:
        return set()
    nlg = lg + 1
    need_xfer = 0 if lg == 0 else min_transfer_sec
    improved_stops: Set[str] = set()

    for route_id in marked_route_ids:
        trips = ctx.trips_by_route.get(route_id)
        if not trips:
            continue
        for tr in trips:
            delta = INF
            board_idx: Optional[int] = None
            for i, p in enumerate(tr.stop_ids):
                arr_here = best.get((p, lg), INF)
                if arr_here + need_xfer <= tr.dep_s[i]:
                    delta = tr.arr_s[i]
                    board_idx = i
                elif delta < INF:
                    delta = tr.arr_s[i]
                if delta >= INF or board_idx is None:
                    continue
                key = (p, nlg)
                if delta < best.get(key, INF):
                    best[key] = delta
                    parent[key] = PRide(
                        prev_key=(tr.stop_ids[board_idx], lg),
                        trip_id=tr.trip_id,
                        board_idx=board_idx,
                        alight_idx=i,
                        t_board=tr.dep_s[board_idx],
                        t_alight=tr.arr_s[i],
                    )
                    improved_stops.add(p)
    return improved_stops


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
    stop_to_routes = ctx.stop_to_routes

    for o in origin_stops:
        if dep_sec < best.get((o, 0), INF):
            best[(o, 0)] = dep_sec

    foot_updated = _relax_foot_layer_seeded(ctx, 0, best, parent, set(origin_stops))

    for lg in range(max_vehicle_legs):
        marked_route_ids: Set[str] = set()
        for s in foot_updated:
            marked_route_ids.update(stop_to_routes.get(s, ()))
        ride_updated = (
            _route_scan_marked_routes(
                ctx,
                lg,
                min_transfer_sec,
                max_vehicle_legs,
                marked_route_ids,
                best,
                parent,
            )
            if marked_route_ids
            else set()
        )
        foot_updated = _relax_foot_layer_seeded(ctx, lg + 1, best, parent, ride_updated)

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
    return ctx.trips_by_trip_id.get(trip_id)


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
