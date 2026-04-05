"""
Streamlit trip planner: multi-waypoint coordinates, connection-based routing, Folium map.

**Data source (mutually exclusive):** set ``DATABASE_URL`` for PostgreSQL, or leave it unset and
provide a GTFS CSV directory (``transfers.txt`` required there). Shapes and stop_times always
come from that same source — never mixed.

Run: ``python -m planner`` or ``streamlit run .../planner/app.py``.
"""

from __future__ import annotations

import datetime as dt
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import folium
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from streamlit_folium import st_folium

load_dotenv()

from gtfs_source import database_url_fingerprint, use_database

from planner.journey import (
    MultiLegPlan,
    NearbyStopsFn,
    merge_chosen_with_indices,
    plan_multi,
)
from planner.preprocess import RaptorContext, build_raptor_context
from planner.repository import load_transit_repository, MissingTransfersError
from planner.raptor import Journey
from planner.timeutil import seconds_to_gtfs_time
from planner.timing import log_phase, timed_phase, timing_enabled


def _duration_label(sec: int) -> str:
    if sec < 3600:
        return f"{sec // 60} min"
    h, r = divmod(sec, 3600)
    m = r // 60
    return f"{h} h {m} min"


def _shapes_dict_from_dataframe(shapes: pd.DataFrame) -> Dict[str, List[Tuple[float, float]]]:
    """Build trip-mapper polylines from the same ``repo.shapes`` used for routing (DB or CSV)."""
    if shapes is None or shapes.empty:
        return {}
    need = {"shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence"}
    if not need.issubset(shapes.columns):
        return {}
    s = shapes[list(need)].sort_values(["shape_id", "shape_pt_sequence"])
    out: Dict[str, List[Tuple[float, float]]] = {}
    for sid, g in s.groupby("shape_id", sort=False):
        out[str(sid)] = list(
            zip(g["shape_pt_lat"].astype(float), g["shape_pt_lon"].astype(float))
        )
    return out


@st.cache_data(show_spinner=False)
def _cached_routing_bundle(
    feed_cache_key: str,
    service_date: dt.date,
    gtfs_resolved: str,
) -> Tuple[RaptorContext, Dict[str, List[Tuple[float, float]]]]:
    """CSV mode: one load, Raptor context + shapes (pickle-safe for ``cache_data``)."""
    t0 = time.perf_counter()
    repo = load_transit_repository(gtfs_dir=gtfs_resolved)
    t1 = time.perf_counter()
    if hasattr(repo, "load_for_date"):
        repo.load_for_date(service_date)
    else:
        repo.load()
    t2 = time.perf_counter()
    ctx = build_raptor_context(repo, service_date)
    t3 = time.perf_counter()
    shapes = _shapes_dict_from_dataframe(repo.shapes)
    t4 = time.perf_counter()
    if timing_enabled():
        log_phase("routing_bundle(csv): create_repo", (t1 - t0) * 1000)
        log_phase("routing_bundle(csv): load_gtfs", (t2 - t1) * 1000)
        log_phase("routing_bundle(csv): build_raptor_context", (t3 - t2) * 1000)
        log_phase("routing_bundle(csv): shapes_dict", (t4 - t3) * 1000)
        log_phase("routing_bundle(csv): total", (t4 - t0) * 1000)
    return ctx, shapes


_PLAN_TTL_SEC = 120.0
_SHAPE_MAP_MAX_POINTS = 450


@st.cache_resource(show_spinner=False)
def _cached_postgres_routing_bundle(
    url_fingerprint: str,
    service_date: dt.date,
) -> Tuple[RaptorContext, Dict[str, List[Tuple[float, float]]], Any]:
    """PostgreSQL: **single** repository — load once, build context, reuse for PostGIS snap."""
    _ = url_fingerprint
    t0 = time.perf_counter()
    repo = load_transit_repository(".")
    t1 = time.perf_counter()
    repo.load_for_date(service_date)
    t2 = time.perf_counter()
    ctx = build_raptor_context(repo, service_date)
    t3 = time.perf_counter()
    shapes = _shapes_dict_from_dataframe(repo.shapes)
    t4 = time.perf_counter()
    if timing_enabled():
        log_phase("routing_bundle(pg): create_repo", (t1 - t0) * 1000)
        log_phase("routing_bundle(pg): load_for_date", (t2 - t1) * 1000)
        log_phase("routing_bundle(pg): build_raptor_context", (t3 - t2) * 1000)
        log_phase("routing_bundle(pg): shapes_dict", (t4 - t3) * 1000)
        log_phase("routing_bundle(pg): total", (t4 - t0) * 1000)
    return ctx, shapes, repo


def _make_nearby_stops_fn(repo: Any, ctx: RaptorContext) -> NearbyStopsFn:
    """PostGIS nearby; filter to stops present in the built routing context."""

    def fn(lat: float, lon: float, max_m: float, k: int) -> List[Tuple[str, float]]:
        lim = max(100, k * 25)
        df = repo.find_stops_nearby(lat, lon, int(max_m), limit=lim)
        out: List[Tuple[str, float]] = []
        if df is None or df.empty:
            return out
        for _, row in df.iterrows():
            sid = str(row["stop_id"])
            if sid not in ctx.stop_coords:
                continue
            out.append((sid, float(row["distance_meters"])))
            if len(out) >= k:
                break
        return out

    return fn


def _plan_run_signature(
    feed_cache_key: str,
    service_date: dt.date,
    waypoints: Tuple[Tuple[float, float], ...],
    dep_sec: int,
    snap_m: int,
    max_alt: int,
    min_xfer: int,
    leg_buf: int,
    n_wp: int,
    use_now: bool,
) -> tuple:
    wp_r = tuple((round(la, 5), round(lo, 5)) for la, lo in waypoints)
    dep_q = (dep_sec // 60) * 60
    return (
        feed_cache_key,
        service_date.isoformat(),
        wp_r,
        dep_q,
        snap_m,
        max_alt,
        min_xfer,
        leg_buf,
        n_wp,
        use_now,
        12,
    )


def _decimate_shape_points(
    pts: List[Tuple[float, float]], max_points: int = _SHAPE_MAP_MAX_POINTS
) -> List[Tuple[float, float]]:
    if len(pts) <= max_points:
        return pts
    step = max(1, len(pts) // max_points)
    return pts[::step]


def _journey_map(
    ctx,
    journey: Journey,
    shapes: Dict[str, List[Tuple[float, float]]],
    waypoints: List[Tuple[float, float]],
) -> folium.Map:
    lats = [w[0] for w in waypoints]
    lons = [w[1] for w in waypoints]
    mid_lat = sum(lats) / len(lats)
    mid_lon = sum(lons) / len(lons)
    m = folium.Map(location=[mid_lat, mid_lon], zoom_start=12, tiles="OpenStreetMap")

    for i, (la, lo) in enumerate(waypoints):
        color = "green" if i == 0 else ("red" if i == len(waypoints) - 1 else "blue")
        folium.Marker(
            [la, lo],
            tooltip=f"W{i + 1}",
            icon=folium.Icon(color=color, icon="map-marker", prefix="fa"),
        ).add_to(m)

    for leg in journey.legs:
        if leg.mode == "walk":
            c0 = ctx.stop_coords.get(leg.from_stop)
            c1 = ctx.stop_coords.get(leg.to_stop)
            if c0 and c1:
                folium.PolyLine(
                    [c0[::-1], c1[::-1]],
                    color="gray",
                    weight=3,
                    dash_array="8 8",
                    opacity=0.85,
                    tooltip="Walk",
                ).add_to(m)
        else:
            sh = shapes.get(ctx.shape_by_trip.get(leg.trip_id or "", ""), [])
            sh = _decimate_shape_points(list(sh))
            if len(sh) >= 2:
                folium.PolyLine(
                    [(lat, lon) for lat, lon in sh],
                    color="blue",
                    weight=4,
                    opacity=0.75,
                    tooltip=f"Route {leg.route_short_name or leg.route_id}",
                ).add_to(m)
            else:
                c0 = ctx.stop_coords.get(leg.from_stop)
                c1 = ctx.stop_coords.get(leg.to_stop)
                if c0 and c1:
                    folium.PolyLine(
                        [c0[::-1], c1[::-1]],
                        color="navy",
                        weight=4,
                        opacity=0.7,
                    ).add_to(m)

    return m


def _render_journey(ctx, j: Journey, title: str) -> None:
    st.markdown(f"#### {title}")
    total_ride = sum(leg.end_sec - leg.start_sec for leg in j.legs if leg.mode == "ride")
    st.caption(
        f"Arrive **{seconds_to_gtfs_time(j.arrival_sec)}** · "
        f"Vehicles **{j.vehicle_legs}** · "
        f"Walk **{_duration_label(j.total_walk_sec)}** · "
        f"In-vehicle ~**{_duration_label(total_ride)}**"
    )
    for li, leg in enumerate(j.legs, 1):
        n0 = ctx.stop_names.get(leg.from_stop, leg.from_stop)
        n1 = ctx.stop_names.get(leg.to_stop, leg.to_stop)
        if leg.mode == "walk":
            st.markdown(
                f"{li}. Walk **{_duration_label(leg.end_sec - leg.start_sec)}** {n0} → {n1}"
            )
        else:
            line = leg.route_short_name or leg.route_id or "?"
            head = leg.headsign or ""
            st.markdown(
                f"{li}. **{line}** {head} · {seconds_to_gtfs_time(leg.start_sec)} → "
                f"{seconds_to_gtfs_time(leg.end_sec)} · {n0} → {n1}"
            )


def _render_plan_results(
    ctx: RaptorContext,
    shapes: Dict[str, List[Tuple[float, float]]],
    waypoints: List[Tuple[float, float]],
    dep_sec: int,
    plan: MultiLegPlan,
) -> None:
    if not plan.segments:
        st.warning("Need at least two waypoints.")
        return

    for si, seg in enumerate(plan.segments):
        st.markdown(f"### Leg {seg.from_idx + 1} → {seg.to_idx + 1}")
        if not seg.options:
            st.error("No route found for this leg (try larger snap radius or different time).")
            continue
        st.caption(
            f"Depart from leg start after **{seconds_to_gtfs_time(seg.depart_sec)}** "
            f"(service day)"
        )
        opts = seg.options
        if len(opts) == 1:
            pick = 0
        else:
            pick = st.selectbox(
                "Itinerary for this leg",
                options=list(range(len(opts))),
                format_func=lambda i: (
                    f"Option {i + 1} — arrive {seconds_to_gtfs_time(opts[i].arrival_sec)} "
                    f"· {opts[i].vehicle_legs} vehicle leg(s)"
                ),
                key=f"opt_pick_{si}",
            )
        _render_journey(ctx, opts[pick], f"Leg {si + 1} (selected option)")

    merged = None
    if plan.ok:
        picks: List[int] = []
        for si, seg in enumerate(plan.segments):
            opts = seg.options
            if len(opts) == 1:
                picks.append(0)
            else:
                picks.append(int(st.session_state.get(f"opt_pick_{si}", 0)))
        merged = merge_chosen_with_indices(plan, picks)

    if merged is not None:
        total = merged.arrival_sec - dep_sec
        st.success(
            f"Selected chain: total elapsed **{_duration_label(total)}**, "
            f"final arrival **{seconds_to_gtfs_time(merged.arrival_sec)}**"
        )
        if st.checkbox("Show route map", value=False, key="show_planner_route_map"):
            fm = _journey_map(ctx, merged, shapes, waypoints)
            st_folium(fm, width=None, height=520, returned_objects=[])


@st.fragment
def _plan_fragment(
    ctx: RaptorContext,
    shapes: Dict[str, List[Tuple[float, float]]],
    *,
    feed_cache_key: str,
    nearby_stops_fn: Optional[NearbyStopsFn],
    service_date: dt.date,
    use_now: bool,
    dep_time: dt.time,
    min_xfer: int,
    leg_buf: int,
    snap_m: int,
    max_alt: int,
    n_wp: int,
) -> None:
    """Waypoint inputs + Plan + results. Reruns alone when these widgets change (not the full app)."""
    st.markdown("### Waypoints (lat, lon)")
    cols = st.columns(min(n_wp, 4))
    waypoints: List[Tuple[float, float]] = []
    for i in range(n_wp):
        c = cols[i % len(cols)]
        with c:
            st.markdown(f"**W{i + 1}**")
            la = st.number_input(f"Lat {i + 1}", value=41.0082, format="%.6f", key=f"la{i}")
            lo = st.number_input(f"Lon {i + 1}", value=28.9784, format="%.6f", key=f"lo{i}")
        waypoints.append((float(la), float(lo)))

    if use_now:
        now = dt.datetime.now()
        dep_sec = now.hour * 3600 + now.minute * 60 + now.second
    else:
        dep_sec = dep_time.hour * 3600 + dep_time.minute * 60 + dep_time.second

    pkg = st.session_state.get("_plan_package")
    if pkg is not None:
        if tuple(pkg.get("waypoints", ())) != tuple(waypoints):
            st.session_state.pop("_plan_package", None)
            pkg = None

    if st.button("Plan", type="primary"):
        for k in list(st.session_state.keys()):
            if isinstance(k, str) and k.startswith("opt_pick_"):
                del st.session_state[k]
        if "show_planner_route_map" in st.session_state:
            st.session_state["show_planner_route_map"] = False

        sig = _plan_run_signature(
            feed_cache_key,
            service_date,
            tuple(waypoints),
            dep_sec,
            int(snap_m),
            int(max_alt),
            int(min_xfer),
            int(leg_buf),
            int(n_wp),
            use_now,
        )
        ttl: Dict[tuple, Tuple[float, MultiLegPlan]] = st.session_state.setdefault(
            "_plan_ttl_cache", {}
        )
        now = time.time()
        for ck, (ts, _) in list(ttl.items()):
            if now - ts > _PLAN_TTL_SEC:
                del ttl[ck]

        if sig in ttl and now - ttl[sig][0] <= _PLAN_TTL_SEC:
            plan = ttl[sig][1]
        else:
            with timed_phase("plan_multi (all legs)"):
                plan = plan_multi(
                    ctx,
                    waypoints,
                    service_date,
                    dep_sec,
                    snap_radius_m=float(snap_m),
                    min_leg_transfer_sec=int(leg_buf),
                    min_transfer_sec=int(min_xfer),
                    max_pareto=int(max_alt),
                    nearby_stops_fn=nearby_stops_fn,
                )
            ttl[sig] = (now, plan)

        st.session_state["_plan_package"] = {
            "waypoints": list(waypoints),
            "dep_sec": dep_sec,
            "plan": plan,
        }
        pkg = st.session_state["_plan_package"]

    if pkg is not None:
        _render_plan_results(
            ctx,
            shapes,
            pkg["waypoints"],
            int(pkg["dep_sec"]),
            pkg["plan"],
        )


def main() -> None:
    st.set_page_config(page_title="GTFS planner", page_icon="🧭", layout="wide")
    st.title("🧭 GTFS trip planner")
    st.markdown("---")

    with st.sidebar:
        st.markdown("### Data source")
        if use_database():
            st.info("Using **PostgreSQL** (`DATABASE_URL`). Local `gtfs/` is not read.")
        else:
            st.caption("CSV mode: unset `DATABASE_URL` to use files under the folder below.")
        gtfs_dir = (
            "gtfs"
            if use_database()
            else st.text_input("GTFS directory", value="gtfs", key="planner_gtfs_dir")
        )
        d = st.date_input("Service date", value=dt.date.today())
        use_now = st.checkbox("Depart now (ignore time below)", value=False)
        t = st.time_input("Departure time", value=dt.time(9, 0))
        min_xfer = st.slider("Min transfer (s)", 30, 300, 90)
        leg_buf = st.slider("Extra buffer between waypoints (s)", 0, 600, 120)
        snap_m = st.slider("Snap radius (m)", 200, 800, 450)
        max_alt = st.slider("Alternatives per leg", 1, 5, 3)
        n_wp = st.number_input("Waypoints (ordered)", min_value=2, max_value=8, value=2)

    if use_database():
        feed_cache_key = f"pg:{database_url_fingerprint()}"
        gtfs_for_load = "."  # unused when DATABASE_URL is set
    else:
        path = Path(gtfs_dir).expanduser()
        try:
            path = path.resolve()
        except OSError:
            path = Path(gtfs_dir).expanduser()
        feed_cache_key = f"csv:{path}"
        gtfs_for_load = str(path)

    feed_key = f"{feed_cache_key}|{d.isoformat()}"
    prev_feed = st.session_state.get("_planner_feed_key")
    show_network_spinner = prev_feed != feed_key
    if show_network_spinner:
        st.session_state["_planner_feed_key"] = feed_key
        st.session_state.pop("_plan_package", None)

    options_key = (
        int(n_wp),
        int(min_xfer),
        int(leg_buf),
        int(snap_m),
        int(max_alt),
        use_now,
        t.hour,
        t.minute,
    )
    if st.session_state.get("_planner_options_key") != options_key:
        st.session_state["_planner_options_key"] = options_key
        st.session_state.pop("_plan_package", None)

    try:
        if use_database():
            if show_network_spinner:
                with st.spinner("Building network…"):
                    ctx, shapes, pg_repo = _cached_postgres_routing_bundle(
                        database_url_fingerprint(), d
                    )
            else:
                ctx, shapes, pg_repo = _cached_postgres_routing_bundle(
                    database_url_fingerprint(), d
                )
            nearby_fn = _make_nearby_stops_fn(pg_repo, ctx)
        else:
            if show_network_spinner:
                with st.spinner("Building network…"):
                    ctx, shapes = _cached_routing_bundle(feed_cache_key, d, gtfs_for_load)
            else:
                ctx, shapes = _cached_routing_bundle(feed_cache_key, d, gtfs_for_load)
            nearby_fn = None
    except MissingTransfersError as e:
        st.error(str(e))
        st.stop()
    except FileNotFoundError as e:
        st.error(f"GTFS path not found: {e}")
        st.stop()
    except Exception as e:
        st.exception(e)
        st.stop()

    _plan_fragment(
        ctx,
        shapes,
        feed_cache_key=feed_key,
        nearby_stops_fn=nearby_fn,
        service_date=d,
        use_now=use_now,
        dep_time=t,
        min_xfer=int(min_xfer),
        leg_buf=int(leg_buf),
        snap_m=int(snap_m),
        max_alt=int(max_alt),
        n_wp=int(n_wp),
    )


if __name__ == "__main__":
    main()
