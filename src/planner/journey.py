"""Multi-waypoint chaining: A → B → C using per-leg routing."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import List, Optional, Sequence, Tuple

from planner.preprocess import RaptorContext, nearest_stops
from planner.raptor import Journey, LegSummary, run_routing


@dataclass
class SegmentResult:
    from_idx: int
    to_idx: int
    depart_sec: int
    options: List[Journey]
    chosen: Optional[Journey] = None


@dataclass
class MultiLegPlan:
    """Full plan across ordered waypoints (coordinates)."""

    service_date: dt.date
    segments: List[SegmentResult] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return all(s.chosen is not None for s in self.segments)

    @property
    def final_arrival_sec(self) -> Optional[int]:
        if not self.segments or not self.segments[-1].chosen:
            return None
        return self.segments[-1].chosen.arrival_sec


def plan_multi(
    ctx: RaptorContext,
    waypoints: Sequence[Tuple[float, float]],
    service_date: dt.date,
    depart_sec: int,
    *,
    snap_radius_m: float = 450.0,
    snap_k: int = 8,
    min_leg_transfer_sec: int = 120,
    min_transfer_sec: int = 90,
    max_vehicle_legs: int = 12,
    max_pareto: int = 5,
) -> MultiLegPlan:
    """
    Chain routing legs between consecutive (lat, lon) pairs.

    `depart_sec` is seconds from midnight on `service_date` (GTFS-style, may exceed 86400).
    """
    plan = MultiLegPlan(service_date=service_date)
    dep = depart_sec

    if len(waypoints) < 2:
        return plan

    for i in range(len(waypoints) - 1):
        lat0, lon0 = waypoints[i]
        lat1, lon1 = waypoints[i + 1]
        o_near = nearest_stops(ctx, lat0, lon0, max_m=snap_radius_m, k=snap_k)
        t_near = nearest_stops(ctx, lat1, lon1, max_m=snap_radius_m, k=snap_k)
        origin_stops = {s for s, _ in o_near}
        target_stops = {s for s, _ in t_near}
        if not origin_stops or not target_stops:
            plan.segments.append(
                SegmentResult(
                    from_idx=i,
                    to_idx=i + 1,
                    depart_sec=dep,
                    options=[],
                    chosen=None,
                )
            )
            continue

        options = run_routing(
            ctx,
            origin_stops,
            target_stops,
            dep,
            min_transfer_sec=min_transfer_sec,
            max_vehicle_legs=max_vehicle_legs,
            max_pareto=max_pareto,
        )
        chosen = options[0] if options else None
        plan.segments.append(
            SegmentResult(
                from_idx=i,
                to_idx=i + 1,
                depart_sec=dep,
                options=options,
                chosen=chosen,
            )
        )
        if chosen is None:
            break
        dep = chosen.arrival_sec + min_leg_transfer_sec

    return plan


def merge_chosen_journeys(segments: List[SegmentResult]) -> Optional[Journey]:
    """Concatenate chosen legs into one Journey for full-map display."""
    if not segments or any(s.chosen is None for s in segments):
        return None
    legs: List[LegSummary] = []
    walk = 0
    v = 0
    last_stop = ""
    arr = 0
    for s in segments:
        j = s.chosen
        assert j is not None
        legs.extend(j.legs)
        walk += j.total_walk_sec
        v += j.vehicle_legs
        last_stop = j.arrival_stop
        arr = j.arrival_sec
    return Journey(
        arrival_stop=last_stop,
        arrival_sec=arr,
        vehicle_legs=v,
        legs=legs,
        total_walk_sec=walk,
    )
