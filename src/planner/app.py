"""
Streamlit trip planner: multi-waypoint coordinates, connection-based routing, Folium map.

Expects ``{gtfs_dir}/transfers.txt`` next to the other GTFS tables. The scraper writes it when
it saves the feed. This app only reads GTFS.

Run: ``python -m planner`` or ``streamlit run .../planner/app.py``.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Dict, List, Tuple

import folium
import pandas as pd
import streamlit as st
from streamlit_folium import st_folium

from planner.journey import MultiLegPlan, merge_chosen_journeys, plan_multi
from planner.preprocess import RaptorContext, build_raptor_context
from planner.repository import CsvGtfsRepository, MissingTransfersError
from planner.raptor import Journey
from planner.timeutil import seconds_to_gtfs_time


def _duration_label(sec: int) -> str:
    if sec < 3600:
        return f"{sec // 60} min"
    h, r = divmod(sec, 3600)
    m = r // 60
    return f"{h} h {m} min"


def _load_shapes(gtfs_dir: Path) -> Dict[str, List[Tuple[float, float]]]:
    p = gtfs_dir / "shapes.txt"
    if not p.exists():
        return {}
    df = pd.read_csv(p)
    out: Dict[str, List[Tuple[float, float]]] = {}
    for sid, g in df.groupby("shape_id"):
        g2 = g.sort_values("shape_pt_sequence")
        out[str(sid)] = list(zip(g2["shape_pt_lat"], g2["shape_pt_lon"]))
    return out


@st.cache_data(show_spinner=False)
def _cached_raptor_context(gtfs_resolved: str, service_date: dt.date) -> RaptorContext:
    """Rebuild only when GTFS path or calendar day changes (see st.cache_data + st.fragment)."""
    repo = CsvGtfsRepository(Path(gtfs_resolved))
    repo.load()
    return build_raptor_context(repo, service_date)


@st.cache_data(show_spinner=False)
def _cached_shapes(gtfs_resolved: str) -> Dict[str, List[Tuple[float, float]]]:
    return _load_shapes(Path(gtfs_resolved))


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
        tab_labels = [f"Option {i + 1}" for i in range(len(seg.options))]
        tabs = st.tabs(tab_labels)
        for ti, tab in enumerate(tabs):
            with tab:
                _render_journey(ctx, seg.options[ti], f"Leg {si + 1} option {ti + 1}")

    merged = merge_chosen_journeys(plan.segments)
    if merged is not None:
        total = merged.arrival_sec - dep_sec
        st.success(
            f"Primary chain: total elapsed **{_duration_label(total)}**, "
            f"final arrival **{seconds_to_gtfs_time(merged.arrival_sec)}**"
        )
        fm = _journey_map(ctx, merged, shapes, waypoints)
        st_folium(fm, width=None, height=520, returned_objects=[])


@st.fragment
def _plan_fragment(
    ctx: RaptorContext,
    shapes: Dict[str, List[Tuple[float, float]]],
    *,
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
        plan = plan_multi(
            ctx,
            waypoints,
            service_date,
            dep_sec,
            snap_radius_m=float(snap_m),
            min_leg_transfer_sec=int(leg_buf),
            min_transfer_sec=int(min_xfer),
            max_pareto=int(max_alt),
        )
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
        st.markdown("### GTFS")
        gtfs_dir = st.text_input("GTFS directory", value="gtfs")
        d = st.date_input("Service date", value=dt.date.today())
        use_now = st.checkbox("Depart now (ignore time below)", value=False)
        t = st.time_input("Departure time", value=dt.time(9, 0))
        min_xfer = st.slider("Min transfer (s)", 30, 300, 90)
        leg_buf = st.slider("Extra buffer between waypoints (s)", 0, 600, 120)
        snap_m = st.slider("Snap radius (m)", 200, 800, 450)
        max_alt = st.slider("Alternatives per leg", 1, 5, 3)
        n_wp = st.number_input("Waypoints (ordered)", min_value=2, max_value=8, value=2)

    path = Path(gtfs_dir).expanduser()
    try:
        path = path.resolve()
    except OSError:
        pass

    feed_key = f"{path}|{d.isoformat()}"
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
        if show_network_spinner:
            with st.spinner("Building network…"):
                ctx = _cached_raptor_context(str(path), d)
                shapes = _cached_shapes(str(path))
        else:
            ctx = _cached_raptor_context(str(path), d)
            shapes = _cached_shapes(str(path))
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
