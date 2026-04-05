"""Build routing structures from a loaded GTFS repository (CSV or PostgreSQL)."""

from __future__ import annotations

import datetime as dt
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import DefaultDict, Dict, List, Optional, Set, Tuple, Protocol

import numpy as np
import pandas as pd
from sklearn.neighbors import BallTree

from planner.timeutil import gtfs_time_to_seconds


class TransitDataSource(Protocol):
    """Protocol for GTFS data sources."""
    stops: pd.DataFrame
    routes: pd.DataFrame
    trips: pd.DataFrame
    stop_times: pd.DataFrame
    transfers: pd.DataFrame
    shapes: pd.DataFrame
    
    def service_ids_on(self, on_date: dt.date) -> set[str]: ...


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
    trips_by_trip_id: Dict[str, TripTimetable]
    board_at: Dict[str, List[Tuple[TripTimetable, int]]]
    stop_to_routes: Dict[str, Set[str]]
    stop_coords: Dict[str, Tuple[float, float]]
    route_meta: Dict[str, Tuple[str, str, str]]  # route_id -> (short_name, long_name, color)
    shape_by_trip: Dict[str, str]
    all_stops: Set[str]
    stop_names: Dict[str, str]
    snap_ball_tree: Optional[BallTree] = None
    snap_stop_ids: Optional[np.ndarray] = None


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


def _build_snap_index(
    stop_coords: Dict[str, Tuple[float, float]],
) -> tuple[Optional[BallTree], Optional[np.ndarray]]:
    """BallTree in radians (haversine); aligned stop ids for index rows."""
    if not stop_coords:
        return None, None
    ids = sorted(stop_coords.keys())
    coords = np.array([stop_coords[sid] for sid in ids], dtype=np.float64)
    coords_rad = np.radians(coords)
    tree = BallTree(coords_rad, metric="haversine")
    ids_arr = np.array(ids, dtype=object)
    return tree, ids_arr


def _exact_haversine_m(lat: float, lon: float, slat: float, slon: float) -> float:
    """Great-circle distance in meters (matches sklearn BallTree metric='haversine')."""
    import math

    r_earth = 6371000.0
    phi1, phi2 = math.radians(lat), math.radians(slat)
    dphi = math.radians(slat - lat)
    dlamb = math.radians(slon - lon)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlamb / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(max(0.0, 1.0 - a)))
    return r_earth * c


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
    repo: TransitDataSource,
    on_date: dt.date,
) -> RaptorContext:
    """Build RAPTOR context from repository (optimized for date-specific loading)."""
    
    # If using optimized repository, data is already filtered by date!
    # No need for additional filtering - HUGE PERFORMANCE BOOST
    services = repo.service_ids_on(on_date)
    trips = repo.trips.copy()
    
    # Only filter if data not already filtered (for backward compatibility)
    if not trips.empty and "service_id" in trips.columns:
        services_in_trips = set(trips["service_id"].astype(str))
        if not services.issubset(services_in_trips) and len(services_in_trips) > len(services) * 2:
            # Data likely not pre-filtered, filter now
            trips = trips[trips["service_id"].astype(str).isin(services)]
    
    active_trip_ids = set(trips["trip_id"].astype(str))

    st = repo.stop_times.copy()
    
    # Only filter if needed (optimized repo already filtered)
    if not st.empty and not active_trip_ids.issubset(set(st["trip_id"].astype(str))):
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
    trips_by_trip_id: Dict[str, TripTimetable] = {}

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
            trips_by_trip_id[str(tid)] = tt
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

    snap_tree, snap_ids = _build_snap_index(stop_coords)

    stop_to_routes: Dict[str, Set[str]] = {}
    for rid, pat in route_stops.items():
        for p in pat:
            stop_to_routes.setdefault(p, set()).add(rid)

    return RaptorContext(
        footpaths=foot,
        route_ids=route_ids,
        route_stops=route_stops,
        trips_by_route=trips_struct,
        trips_by_trip_id=trips_by_trip_id,
        board_at=dict(board_at),
        stop_to_routes=stop_to_routes,
        stop_coords=stop_coords,
        route_meta=route_meta,
        shape_by_trip=shape_by_trip,
        all_stops=all_stops,
        stop_names=stop_names,
        snap_ball_tree=snap_tree,
        snap_stop_ids=snap_ids,
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
    if ctx.snap_ball_tree is not None and ctx.snap_stop_ids is not None:
        q = np.radians(np.array([[lat, lon]], dtype=np.float64))
        r_rad = (max_m * 1.02) / 6371000.0
        idxs = ctx.snap_ball_tree.query_radius(q, r=r_rad)[0]
        cand: List[Tuple[str, float]] = []
        for j in idxs:
            sid = str(ctx.snap_stop_ids[int(j)])
            slat, slon = ctx.stop_coords[sid]
            d = _exact_haversine_m(lat, lon, slat, slon)
            if d <= max_m:
                cand.append((sid, d))
        cand.sort(key=lambda x: (x[1], x[0]))
        return cand[:k]

    cand_bf: List[Tuple[str, float]] = []
    for sid, (slat, slon) in ctx.stop_coords.items():
        d = _exact_haversine_m(lat, lon, slat, slon)
        if d <= max_m:
            cand_bf.append((sid, d))
    cand_bf.sort(key=lambda x: (x[1], x[0]))
    return cand_bf[:k]
