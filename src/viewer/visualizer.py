from __future__ import annotations

from pathlib import Path

import folium
import pandas as pd
import streamlit as st


class GTFSVisualizer:
    def __init__(self, gtfs_dir: str):
        self.gtfs_dir = Path(gtfs_dir)
        self.load_data()

    def load_data(self):
        """Load GTFS CSV files from gtfs_dir."""
        try:
            self.agencies = pd.read_csv(self.gtfs_dir / "agency.txt")
            self.stops = pd.read_csv(self.gtfs_dir / "stops.txt")
            self.routes = pd.read_csv(self.gtfs_dir / "routes.txt")
            self.trips = pd.read_csv(self.gtfs_dir / "trips.txt")
            self.stop_times = pd.read_csv(self.gtfs_dir / "stop_times.txt")
            self.calendar = pd.read_csv(self.gtfs_dir / "calendar.txt")
            self.shapes = pd.read_csv(self.gtfs_dir / "shapes.txt")

            freq_file = self.gtfs_dir / "frequencies.txt"
            if freq_file.exists():
                self.frequencies = pd.read_csv(freq_file)
            else:
                self.frequencies = pd.DataFrame()

            st.success("GTFS data loaded successfully.")
        except Exception as e:
            st.error(f"Error loading GTFS data: {str(e)}")
            raise

    def get_route_directions(self, route_id: str):
        """Return direction_key -> trip_headsign for a route."""
        route_trips = self.trips[self.trips["route_id"] == route_id]

        directions = {}
        for _, trip in route_trips.iterrows():
            trip_id = trip["trip_id"]
            headsign = trip["trip_headsign"]

            parts = trip_id.split("_")

            direction_key = None
            for i, part in enumerate(parts):
                if part in ["forward", "backward"] or part.startswith("secondary"):
                    if part.startswith("secondary"):
                        direction_key = (
                            f"{part}_{parts[i + 1]}_{parts[i + 2]}" if i + 2 < len(parts) else part
                        )
                    else:
                        direction_key = part
                    break

            if direction_key and direction_key not in directions:
                directions[direction_key] = headsign

        return directions

    def get_direction_stops(self, route_id: str, direction: str):
        """Ordered stops for one direction (one representative trip)."""
        route_trips = self.trips[self.trips["route_id"] == route_id]

        direction_trip = None
        for _, trip in route_trips.iterrows():
            if direction in trip["trip_id"]:
                direction_trip = trip
                break

        if direction_trip is None:
            return pd.DataFrame()

        trip_stops = self.stop_times[self.stop_times["trip_id"] == direction_trip["trip_id"]].sort_values(
            "stop_sequence"
        )

        trip_stops = trip_stops.merge(
            self.stops[["stop_id", "stop_name", "stop_lat", "stop_lon"]],
            on="stop_id",
            how="left",
        )

        return trip_stops

    def get_direction_schedule(self, route_id: str, direction: str):
        """Collect departure times by service day bucket for a direction."""
        route_trips = self.trips[self.trips["route_id"] == route_id]
        direction_trips = route_trips[route_trips["trip_id"].str.contains(direction)]

        if direction_trips.empty:
            return {"weekday": [], "saturday": [], "sunday": []}

        schedule = {"weekday": [], "saturday": [], "sunday": []}

        for _, trip in direction_trips.iterrows():
            trip_id = trip["trip_id"]
            service_id = trip["service_id"]

            if not self.frequencies.empty:
                freq = self.frequencies[self.frequencies["trip_id"] == trip_id]
                if not freq.empty:
                    freq_row = freq.iloc[0]
                    service_info = self.calendar[self.calendar["service_id"] == service_id]

                    if not service_info.empty:
                        service_row = service_info.iloc[0]
                        freq_str = (
                            f"{freq_row['start_time']} - {freq_row['end_time']} "
                            f"(every {freq_row['headway_secs'] // 60} min)"
                        )

                        if service_row["monday"] == 1:
                            schedule["weekday"].append(freq_str)
                        if service_row["saturday"] == 1:
                            schedule["saturday"].append(freq_str)
                        if service_row["sunday"] == 1:
                            schedule["sunday"].append(freq_str)
                    continue

            trip_times = self.stop_times[self.stop_times["trip_id"] == trip_id]
            if trip_times.empty:
                continue

            first_stop = trip_times.sort_values("stop_sequence").iloc[0]
            departure_time = first_stop["departure_time"]

            service_info = self.calendar[self.calendar["service_id"] == service_id]

            if service_info.empty:
                continue

            service_row = service_info.iloc[0]

            if service_row["monday"] == 1:
                schedule["weekday"].append(departure_time)

            if service_row["saturday"] == 1:
                schedule["saturday"].append(departure_time)

            if service_row["sunday"] == 1:
                schedule["sunday"].append(departure_time)

        for key in schedule:
            schedule[key] = sorted(list(set(schedule[key])))

        return schedule

    def get_route_shape(self, route_id: str, direction: str):
        """Shape points for a direction."""
        route_trips = self.trips[self.trips["route_id"] == route_id]
        direction_trip = route_trips[route_trips["trip_id"].str.contains(direction)]

        if direction_trip.empty:
            return pd.DataFrame()

        shape_id = direction_trip.iloc[0]["shape_id"]

        shape_points = self.shapes[self.shapes["shape_id"] == shape_id].sort_values("shape_pt_sequence")

        return shape_points

    def create_route_map(self, route_id: str, direction: str):
        """Folium map with polyline and stop markers."""
        stops_df = self.get_direction_stops(route_id, direction)

        if stops_df.empty:
            return None

        center_lat = stops_df["stop_lat"].mean()
        center_lon = stops_df["stop_lon"].mean()

        m = folium.Map(location=[center_lat, center_lon], zoom_start=12, tiles="OpenStreetMap")

        shape_points = self.get_route_shape(route_id, direction)
        if not shape_points.empty:
            coordinates = list(zip(shape_points["shape_pt_lat"], shape_points["shape_pt_lon"]))
            folium.PolyLine(
                coordinates,
                color="blue",
                weight=4,
                opacity=0.7,
                tooltip="Route shape",
            ).add_to(m)

        for idx, stop in stops_df.iterrows():
            if idx == stops_df.index[0]:
                folium.Marker(
                    location=[stop["stop_lat"], stop["stop_lon"]],
                    popup=(f"<b>Start:</b><br>{stop['stop_name']}<br>{stop['departure_time']}"),
                    tooltip=stop["stop_name"],
                    icon=folium.Icon(color="green", icon="play", prefix="fa"),
                ).add_to(m)
            elif idx == stops_df.index[-1]:
                folium.Marker(
                    location=[stop["stop_lat"], stop["stop_lon"]],
                    popup=f"<b>End:</b><br>{stop['stop_name']}<br>{stop['arrival_time']}",
                    tooltip=stop["stop_name"],
                    icon=folium.Icon(color="red", icon="stop", prefix="fa"),
                ).add_to(m)
            else:
                folium.CircleMarker(
                    location=[stop["stop_lat"], stop["stop_lon"]],
                    radius=6,
                    popup=(
                        f"<b>{stop['stop_name']}</b><br>Seq: {stop['stop_sequence']}<br>"
                        f"{stop['arrival_time']}"
                    ),
                    tooltip=stop["stop_name"],
                    color="blue",
                    fill=True,
                    fillColor="lightblue",
                    fillOpacity=0.8,
                ).add_to(m)

        return m

    def format_schedule_table(self, schedule: dict):
        """Wide dataframe: weekday / Saturday / Sunday columns."""
        max_len = max(len(schedule["weekday"]), len(schedule["saturday"]), len(schedule["sunday"]))

        weekday = schedule["weekday"] + [""] * (max_len - len(schedule["weekday"]))
        saturday = schedule["saturday"] + [""] * (max_len - len(schedule["saturday"]))
        sunday = schedule["sunday"] + [""] * (max_len - len(schedule["sunday"]))

        df = pd.DataFrame(
            {
                "Weekday (Mon–Fri)": weekday,
                "Saturday": saturday,
                "Sunday": sunday,
            }
        )

        return df
