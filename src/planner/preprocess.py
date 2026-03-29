"""Build routing structures from a loaded CsvGtfsRepository."""

from __future__ import annotations

import datetime as dt
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import DefaultDict, Dict, List, Optional, Set, Tuple

import pandas as pd

from planner.repository import CsvGtfsRepository
from planner.timeutil import gtfs_time_to_seconds


@dataclass
class TripTimetable:
    trip_id: str
    route_id: str
    shape_id: str
    headsign: str
    stop_ids: List[str]
    dep_s: List[int]
    arr_s: List[int]


@dataclass
class RaptorContext:
    """Static data for one planning day (service filter applied)."""

    footpaths: Dict[str, List[Tuple[str, int]]]
    route_ids: List[str]
    route_stops: Dict[str, List[str]]
    trips_by_route: Dict[str, List[TripTimetable]]
    board_at: Dict[str, List[Tuple[TripTimetable, int]]]
    stop_coords: Dict[str, Tuple[float, float]]
    route_meta: Dict[str, Tuple[str, str, str]]  # route_id -> (short_name, long_name, color)
    shape_by_trip: Dict[str, str]
    all_stops: Set[str]
    stop_names: Dict[str, str]


def _build_footpaths(transfers: pd.DataFrame) -> Dict[str, List[Tuple[str, int]]]:
    foot: DefaultDict[str, List[Tuple[str, int]]] = defaultdict(list)
    if transfers.empty:
        return {}
    for _, row in transfers.iterrows():
        a = str(row["from_stop_id"])
        b = str(row["to_stop_id"])
        try:
            w = int(row["min_transfer_time"])
        except (TypeError, ValueError):
            w = 60
        foot[a].append((b, w))
    return dict(foot)


def _canonical_route_pattern(
    trip_ids: List[str], st_by_trip: Dict[str, pd.DataFrame]
) -> Optional[List[str]]:
    seq_counter: Counter = Counter()
    for tid in trip_ids:
        chunk = st_by_trip.get(tid)
        if chunk is None or chunk.empty:
            continue
        seq = tuple(chunk.sort_values("stop_sequence")["stop_id"].astype(str).tolist())
        if seq:
            seq_counter[seq] += 1
    if not seq_counter:
        return None
    return list(seq_counter.most_common(1)[0][0])


def build_raptor_context(
    repo: CsvGtfsRepository,
    on_date: dt.date,
) -> RaptorContext:
    repo.load()
    services = repo.service_ids_on(on_date)
    trips = repo.trips.copy()
    trips = trips[trips["service_id"].astype(str).isin(services)]
    active_trip_ids = set(trips["trip_id"].astype(str))

    st = repo.stop_times.copy()
    st = st[st["trip_id"].astype(str).isin(active_trip_ids)]

    st_by_trip: Dict[str, pd.DataFrame] = {
        tid: g for tid, g in st.groupby(st["trip_id"].astype(str), sort=False)
    }

    routes_df = repo.routes
    # One pass over trips — avoid O(|trips|) boolean scans per trip_id.
    tmeta = trips[["trip_id", "route_id", "shape_id", "trip_headsign"]].copy()
    tmeta["trip_id"] = tmeta["trip_id"].astype(str)
    tmeta = tmeta.drop_duplicates(subset=["trip_id"], keep="first")
    trip_route = dict(zip(tmeta["trip_id"], tmeta["route_id"].astype(str)))
    trip_shape = dict(zip(tmeta["trip_id"], tmeta["shape_id"].fillna("").astype(str)))
    trip_head = dict(zip(tmeta["trip_id"], tmeta["trip_headsign"].fillna("").astype(str)))

    trips_by_route_id: DefaultDict[str, List[str]] = defaultdict(list)
    for tid in active_trip_ids:
        r = trip_route.get(tid)
        if r is not None:
            trips_by_route_id[str(r)].append(tid)

    route_stops: Dict[str, List[str]] = {}
    trips_struct: Dict[str, List[TripTimetable]] = {}
    shape_by_trip: Dict[str, str] = {}
    route_meta: Dict[str, Tuple[str, str, str]] = {}
    all_stops: Set[str] = set()
    board_at: DefaultDict[str, List[Tuple[TripTimetable, int]]] = defaultdict(list)

    for route_id, tids in trips_by_route_id.items():
        pattern = _canonical_route_pattern(tids, st_by_trip)
        if not pattern:
            continue
        route_stops[route_id] = pattern
        all_stops.update(pattern)
        sub = routes_df[routes_df["route_id"].astype(str) == str(route_id)]
        if not sub.empty:
            row = sub.iloc[0]
            short_n = str(row.get("route_short_name", "") or "")
            long_n = str(row.get("route_long_name", "") or "")
            color = (
                str(row["route_color"])
                if "route_color" in sub.columns and pd.notna(row.get("route_color", None))
                else ""
            )
            route_meta[route_id] = (short_n, long_n, color)
        else:
            route_meta[route_id] = ("", "", "")

        tt_list: List[TripTimetable] = []
        pat_t = tuple(pattern)
        for tid in tids:
            chunk = st_by_trip.get(tid)
            if chunk is None or chunk.empty:
                continue
            chunk = chunk.sort_values("stop_sequence")
            seq = chunk["stop_id"].astype(str).tolist()
            if tuple(seq) != pat_t:
                continue
            dep_s = [
                gtfs_time_to_seconds(x) for x in chunk["departure_time"].astype(str).tolist()
            ]
            arr_s = [gtfs_time_to_seconds(x) for x in chunk["arrival_time"].astype(str).tolist()]
            if len(dep_s) != len(seq):
                continue
            shape_id = trip_shape.get(str(tid), "")
            headsign = trip_head.get(str(tid), "")
            tt = TripTimetable(
                trip_id=str(tid),
                route_id=route_id,
                shape_id=shape_id,
                headsign=headsign,
                stop_ids=seq,
                dep_s=dep_s,
                arr_s=arr_s,
            )
            tt_list.append(tt)
            shape_by_trip[str(tid)] = shape_id
            for idx, sid in enumerate(seq):
                board_at[sid].append((tt, idx))
        if tt_list:
            trips_struct[route_id] = tt_list

    route_ids = sorted(trips_struct.keys())
    foot = _build_footpaths(repo.transfers)
    xfer_stops: Set[str] = set()
    if not repo.transfers.empty:
        xfer_stops |= set(repo.transfers["from_stop_id"].astype(str))
        xfer_stops |= set(repo.transfers["to_stop_id"].astype(str))
    all_stops |= xfer_stops

    stops_df = repo.stops.set_index("stop_id", drop=False)
    stop_coords: Dict[str, Tuple[float, float]] = {}
    stop_names: Dict[str, str] = {}
    for sid in sorted(all_stops):
        if sid in stops_df.index:
            r = stops_df.loc[sid]
            if isinstance(r, pd.DataFrame):
                r = r.iloc[0]
            try:
                stop_coords[sid] = (float(r["stop_lat"]), float(r["stop_lon"]))
            except (TypeError, ValueError):
                pass
            stop_names[sid] = str(r["stop_name"]) if "stop_name" in r.index else str(sid)

    return RaptorContext(
        footpaths=foot,
        route_ids=route_ids,
        route_stops=route_stops,
        trips_by_route=trips_struct,
        board_at=dict(board_at),
        stop_coords=stop_coords,
        route_meta=route_meta,
        shape_by_trip=shape_by_trip,
        all_stops=all_stops,
        stop_names=stop_names,
    )


def nearest_stops(
    ctx: RaptorContext,
    lat: float,
    lon: float,
    *,
    max_m: float = 500.0,
    k: int = 5,
) -> List[Tuple[str, float]]:
    """Return up to k (stop_id, distance_m) within max_m (haversine)."""
    import math

    R = 6371000.0

    def dist_m(a: float, b: float, c: float, d: float) -> float:
        rlat1, rlon1, rlat2, rlon2 = map(math.radians, (a, b, c, d))
        x = (rlon2 - rlon1) * math.cos((rlat1 + rlat2) / 2)
        y = rlat2 - rlat1
        return math.hypot(x, y) * R

    cand: List[Tuple[str, float]] = []
    for sid, (slat, slon) in ctx.stop_coords.items():
        d = dist_m(lat, lon, slat, slon)
        if d <= max_m:
            cand.append((sid, d))
    cand.sort(key=lambda x: x[1])
    return cand[:k]
