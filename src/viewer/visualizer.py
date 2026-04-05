from __future__ import annotations

from pathlib import Path

import folium
import pandas as pd
import streamlit as st

from gtfs_source import database_url

_STOP_TIME_COLS = (
    "trip_id",
    "arrival_time",
    "departure_time",
    "stop_id",
    "stop_sequence",
)
_SHAPE_COLS = ("shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence")


class GTFSVisualizer:
    def __init__(self, gtfs_dir: str):
        self.gtfs_dir = Path(gtfs_dir)
        self.load_data()

    def load_data(self):
        """Load GTFS CSV files from gtfs_dir (minimal columns on huge tables)."""
        try:
            d = self.gtfs_dir
            self.agencies = pd.read_csv(d / "agency.txt")
            self.stops = pd.read_csv(d / "stops.txt", dtype={"stop_id": str})
            self.routes = pd.read_csv(d / "routes.txt")
            self.trips = pd.read_csv(d / "trips.txt", dtype={"trip_id": str, "route_id": str})
            st_path = d / "stop_times.txt"
            try:
                self.stop_times = pd.read_csv(
                    st_path,
                    dtype={"trip_id": str, "stop_id": str},
                    usecols=list(_STOP_TIME_COLS),
                    low_memory=False,
                )
            except ValueError:
                self.stop_times = pd.read_csv(
                    st_path,
                    dtype={"trip_id": str, "stop_id": str},
                    low_memory=False,
                )
            self.calendar = pd.read_csv(d / "calendar.txt")
            shapes_path = d / "shapes.txt"
            if shapes_path.is_file():
                try:
                    self.shapes = pd.read_csv(
                        shapes_path,
                        dtype={"shape_id": str},
                        usecols=list(_SHAPE_COLS),
                        low_memory=False,
                    )
                except ValueError:
                    self.shapes = pd.read_csv(shapes_path, dtype={"shape_id": str}, low_memory=False)
            else:
                self.shapes = pd.DataFrame(columns=list(_SHAPE_COLS))

            freq_file = d / "frequencies.txt"
            if freq_file.exists():
                self.frequencies = pd.read_csv(freq_file)
            else:
                self.frequencies = pd.DataFrame()

            st.success("GTFS data loaded successfully (CSV).")
        except Exception as e:
            st.error(f"Error loading GTFS data: {str(e)}")
            raise


class DatabaseGTFSVisualizer:
    """GTFS Visualizer using PostgreSQL database."""
    
    def __init__(self, database_url: str):
        """Initialize visualizer with PostgreSQL connection.
        
        Args:
            database_url: PostgreSQL connection string
        """
        try:
            from sqlalchemy import create_engine
        except ImportError:
            raise ImportError(
                "SQLAlchemy required for database support. "
                "Install with: pip install -e '.[db]'"
            )
        
        self.database_url = database_url
        self.engine = create_engine(database_url, echo=False)
        self.load_data()
    
    def load_data(self):
        """Load GTFS data from PostgreSQL."""
        try:
            self.agencies = pd.read_sql("SELECT * FROM agency", self.engine)
            self.stops = pd.read_sql("SELECT * FROM stops", self.engine)
            self.routes = pd.read_sql("SELECT * FROM routes", self.engine)
            self.trips = pd.read_sql("SELECT * FROM trips", self.engine)
            self.calendar = pd.read_sql("SELECT * FROM calendar", self.engine)
            
            # Load stop_times with converted times
            stop_times_query = """
                SELECT 
                    trip_id,
                    stop_id,
                    stop_sequence,
                    seconds_to_gtfs_time(arrival_time) as arrival_time,
                    seconds_to_gtfs_time(departure_time) as departure_time,
                    stop_headsign,
                    pickup_type,
                    drop_off_type
                FROM stop_times
                ORDER BY trip_id, stop_sequence
            """
            self.stop_times = pd.read_sql(stop_times_query, self.engine)
            
            # Load shapes
            self.shapes = pd.read_sql(
                "SELECT shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence FROM shapes",
                self.engine
            )
            
            # Optional: frequencies
            try:
                self.frequencies = pd.read_sql("SELECT * FROM frequencies", self.engine)
            except Exception:
                self.frequencies = pd.DataFrame()
            
            st.success("✓ GTFS data loaded from PostgreSQL database")
            
        except Exception as e:
            st.error(f"Error loading from database: {str(e)}")
            raise
    
    # Reuse all the visualization methods from GTFSVisualizer
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

        merged = trip_stops.merge(self.stops, on="stop_id", how="left")
        return merged

    def get_route_shape(self, route_id: str, direction: str):
        """Route shape coordinates for a direction."""
        route_trips = self.trips[self.trips["route_id"] == route_id]

        for _, trip in route_trips.iterrows():
            if direction in trip["trip_id"] and pd.notna(trip.get("shape_id")):
                shape_id = trip["shape_id"]
                shape_points = self.shapes[self.shapes["shape_id"] == shape_id].sort_values("shape_pt_sequence")

                if not shape_points.empty:
                    return shape_points[["shape_pt_lat", "shape_pt_lon"]].values.tolist()

        return []

    def get_route_schedule(self, route_id: str, direction: str):
        """Departure times from first stop for a direction."""
        route_trips = self.trips[self.trips["route_id"] == route_id]

        direction_trips = []
        for _, trip in route_trips.iterrows():
            if direction in trip["trip_id"]:
                direction_trips.append(trip["trip_id"])

        if not direction_trips:
            return pd.DataFrame()

        trip_stop_times = self.stop_times[self.stop_times["trip_id"].isin(direction_trips)]

        first_stops = trip_stop_times.loc[trip_stop_times.groupby("trip_id")["stop_sequence"].idxmin()]

        schedule = first_stops[["trip_id", "departure_time"]].copy()
        schedule = schedule.sort_values("departure_time")

        return schedule


def create_visualizer(database_url_override: str | None = None, gtfs_dir: str = "gtfs"):
    """PostgreSQL **or** CSV — mutually exclusive (same rule as ``gtfs_source``).

    If ``DATABASE_URL`` is set (or ``database_url_override``), returns
    :class:`~viewer.optimized_visualizer.OptimizedDatabaseVisualizer`.
    Otherwise loads :class:`GTFSVisualizer` from ``gtfs_dir`` only.
    """
    url = (database_url_override or database_url() or "").strip()
    if url:
        try:
            from viewer.optimized_visualizer import OptimizedDatabaseVisualizer

            return OptimizedDatabaseVisualizer(url)
        except ImportError as e:
            raise ImportError(
                "Database extras required: pip install -e '.[db]' "
                "(sqlalchemy, psycopg2-binary, etc.)"
            ) from e
    return GTFSVisualizer(gtfs_dir)
