"""Streamlit GTFS route viewer (run via ``python -m viewer`` or ``streamlit run .../viewer/app.py``)."""

from __future__ import annotations

import streamlit as st
from streamlit_folium import st_folium

try:
    from viewer.visualizer import GTFSVisualizer
except ImportError:
    # streamlit run .../viewer/app.py executes this file as __main__ (no package context)
    from visualizer import GTFSVisualizer


def main() -> None:
    st.set_page_config(
        page_title="GTFS viewer",
        page_icon="🚍",
        layout="wide",
    )

    st.title("🚍 GTFS viewer")
    st.markdown("---")

    try:
        if "visualizer" not in st.session_state:
            st.session_state.visualizer = GTFSVisualizer("gtfs")
        viz = st.session_state.visualizer

        st.sidebar.markdown("## Route")

        routes_list = viz.routes.sort_values("route_short_name")
        route_options = {
            f"{row['route_short_name']} - {row['route_long_name']}": row["route_id"]
            for _, row in routes_list.iterrows()
        }

        selected_route_name = st.sidebar.selectbox(
            "Route",
            options=list(route_options.keys()),
            index=0,
        )

        selected_route_id = route_options[selected_route_name]

        directions = viz.get_route_directions(selected_route_id)

        if not directions:
            st.error("No direction information for this route.")
            return

        direction_options = {
            f"{direction}: {headsign}": direction for direction, headsign in directions.items()
        }

        selected_direction_name = st.sidebar.selectbox(
            "Direction / pattern",
            options=list(direction_options.keys()),
            index=0,
        )

        selected_direction = direction_options[selected_direction_name]

        route_info = viz.routes[viz.routes["route_id"] == selected_route_id].iloc[0]

        st.markdown(f"### 🚌 {route_info['route_short_name']} - {route_info['route_long_name']}")
        st.markdown(f"**Headsign:** {directions[selected_direction]}")

        col1, col2 = st.columns([1, 1])

        with col1:
            st.markdown("### Departures")

            schedule = viz.get_direction_schedule(selected_route_id, selected_direction)
            schedule_df = viz.format_schedule_table(schedule)

            if schedule_df.empty or (schedule_df == "").all().all():
                st.info("No schedule information for this direction.")
            else:
                st.dataframe(
                    schedule_df,
                    use_container_width=True,
                    height=400,
                )

                st.markdown("#### Statistics")
                stat_col1, stat_col2, stat_col3 = st.columns(3)

                with stat_col1:
                    weekday_count = len([x for x in schedule["weekday"] if x])
                    st.metric("Weekday trips", weekday_count)

                with stat_col2:
                    saturday_count = len([x for x in schedule["saturday"] if x])
                    st.metric("Saturday trips", saturday_count)

                with stat_col3:
                    sunday_count = len([x for x in schedule["sunday"] if x])
                    st.metric("Sunday trips", sunday_count)

        with col2:
            st.markdown("### Stops along pattern")

            stops_df = viz.get_direction_stops(selected_route_id, selected_direction)
            with st.expander(f"{len(stops_df)} stops", expanded=False):
                if stops_df.empty:
                    st.info("No stop list for this direction.")
                else:
                    for idx, stop in stops_df.iterrows():
                        sid = str(stop["stop_id"]).split("_")[-1]
                        st.markdown(f"**{int(stop['stop_sequence'])}.** {stop['stop_name']} ({sid})")

        st.markdown("---")
        st.markdown("### Map")

        route_map = viz.create_route_map(selected_route_id, selected_direction)

        if route_map:
            st_folium(
                route_map,
                width=None,
                height=600,
                returned_objects=[],
            )
        else:
            st.info("Could not build a map for this direction.")

    except Exception as e:
        st.error(f"Error: {str(e)}")
        st.exception(e)


if __name__ == "__main__":
    main()
