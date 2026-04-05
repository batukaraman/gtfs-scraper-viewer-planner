"""
Optimized GTFS Visualizer using PostgreSQL with server-side queries.

High-performance visualizer that leverages database for:
- Server-side aggregations
- Spatial queries
- Efficient joins
- Minimal data transfer
"""

from __future__ import annotations

import folium
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool


class OptimizedDatabaseVisualizer:
    """
    High-performance GTFS visualizer using PostgreSQL.
    
    Optimizations:
    - Connection pooling
    - Server-side joins and aggregations
    - Lazy loading (only fetch what's needed)
    - Spatial queries via PostGIS
    - Minimal data transfer
    """
    
    def __init__(self, database_url: str):
        """Initialize with PostgreSQL connection.
        
        Args:
            database_url: PostgreSQL connection string
        """
        self.database_url = database_url
        self.engine = create_engine(
            database_url,
            echo=False,
            poolclass=QueuePool,
            pool_size=3,
            max_overflow=5,
            pool_pre_ping=True,
        )
        
        # Cache frequently accessed data
        self._routes_cache = None
        self._agencies_cache = None
        
        self.load_reference_data()
    
    def load_reference_data(self):
        """Load small reference tables (cached)."""
        try:
            # These are small, load once
            self.agencies = pd.read_sql("SELECT * FROM agency", self.engine)
            self.routes = pd.read_sql(
                "SELECT * FROM routes ORDER BY route_short_name",
                self.engine
            )
            self._routes_cache = self.routes
            self._agencies_cache = self.agencies
            
            st.success(f"✓ Connected to PostgreSQL database ({len(self.routes)} routes)")
            
        except Exception as e:
            st.error(f"Error connecting to database: {str(e)}")
            raise
    
    def get_route_directions(self, route_id: str) -> dict:
        """Get directions for a route (server-side distinct)."""
        
        query = text("""
            SELECT DISTINCT
                trip_headsign,
                direction_id
            FROM trips
            WHERE route_id = :route_id
            AND trip_headsign IS NOT NULL
            ORDER BY direction_id, trip_headsign
        """)
        
        df = pd.read_sql(query, self.engine, params={"route_id": route_id})
        
        directions = {}
        for _, row in df.iterrows():
            # Create direction key from trip pattern
            direction_key = f"dir_{row['direction_id']}" if pd.notna(row['direction_id']) else "default"
            if direction_key not in directions:
                directions[direction_key] = row['trip_headsign']
        
        return directions
    
    def get_direction_stops(self, route_id: str, direction: str) -> pd.DataFrame:
        """Get stops for a direction (server-side join + aggregation)."""
        
        # Find a representative trip for this direction
        trip_query = text("""
            SELECT trip_id, shape_id
            FROM trips
            WHERE route_id = :route_id
            AND (
                trip_id LIKE :direction_pattern
                OR direction_id = :direction_id
            )
            LIMIT 1
        """)
        
        direction_id = direction.replace("dir_", "") if "dir_" in direction else "0"
        
        trip_df = pd.read_sql(
            trip_query,
            self.engine,
            params={
                "route_id": route_id,
                "direction_pattern": f"%{direction}%",
                "direction_id": direction_id
            }
        )
        
        if trip_df.empty:
            return pd.DataFrame()
        
        trip_id = trip_df.iloc[0]['trip_id']
        
        # Get stops for this trip (server-side join!)
        stops_query = text("""
            SELECT 
                st.stop_sequence,
                st.stop_id,
                s.stop_name,
                s.stop_lat,
                s.stop_lon,
                seconds_to_gtfs_time(st.arrival_time) as arrival_time,
                seconds_to_gtfs_time(st.departure_time) as departure_time
            FROM stop_times st
            JOIN stops s ON st.stop_id = s.stop_id
            WHERE st.trip_id = :trip_id
            ORDER BY st.stop_sequence
        """)
        
        return pd.read_sql(stops_query, self.engine, params={"trip_id": trip_id})
    
    def get_route_shape(self, route_id: str, direction: str) -> list:
        """Get route shape (server-side)."""
        
        # Find a trip with shape
        trip_query = text("""
            SELECT shape_id
            FROM trips
            WHERE route_id = :route_id
            AND shape_id IS NOT NULL
            AND (
                trip_id LIKE :direction_pattern
                OR direction_id = :direction_id
            )
            LIMIT 1
        """)
        
        direction_id = direction.replace("dir_", "") if "dir_" in direction else "0"
        
        trip_df = pd.read_sql(
            trip_query,
            self.engine,
            params={
                "route_id": route_id,
                "direction_pattern": f"%{direction}%",
                "direction_id": direction_id
            }
        )
        
        if trip_df.empty or pd.isna(trip_df.iloc[0]['shape_id']):
            return []
        
        shape_id = trip_df.iloc[0]['shape_id']
        
        # Get shape points (server-side order!)
        shape_query = text("""
            SELECT shape_pt_lat, shape_pt_lon
            FROM shapes
            WHERE shape_id = :shape_id
            ORDER BY shape_pt_sequence
        """)
        
        shape_df = pd.read_sql(shape_query, self.engine, params={"shape_id": shape_id})
        
        return shape_df[["shape_pt_lat", "shape_pt_lon"]].values.tolist()

    def get_direction_schedule(self, route_id: str, direction: str) -> dict:
        """Same shape as :class:`GTFSVisualizer.get_direction_schedule` (weekday / saturday / sunday lists)."""
        direction_id = direction.replace("dir_", "") if "dir_" in direction else "0"
        query = text(
            """
            WITH dir_trips AS (
                SELECT trip_id, service_id
                FROM trips
                WHERE route_id = :route_id
                AND (
                    trip_id LIKE :direction_pattern
                    OR CAST(direction_id AS TEXT) = :direction_id
                )
            ),
            first_stops AS (
                SELECT DISTINCT ON (st.trip_id)
                    st.trip_id,
                    st.departure_time
                FROM stop_times st
                INNER JOIN dir_trips d ON d.trip_id = st.trip_id
                ORDER BY st.trip_id, st.stop_sequence
            )
            SELECT
                c.monday,
                c.saturday,
                c.sunday,
                seconds_to_gtfs_time(fs.departure_time) AS departure_time
            FROM first_stops fs
            INNER JOIN dir_trips d ON d.trip_id = fs.trip_id
            INNER JOIN calendar c ON c.service_id = d.service_id
            WHERE fs.departure_time IS NOT NULL
            ORDER BY fs.departure_time
            """
        )
        df = pd.read_sql(
            query,
            self.engine,
            params={
                "route_id": route_id,
                "direction_pattern": f"%{direction}%",
                "direction_id": str(direction_id),
            },
        )
        schedule: dict = {"weekday": [], "saturday": [], "sunday": []}
        if df.empty:
            return schedule
        for _, row in df.iterrows():
            dep = row["departure_time"]
            try:
                if int(row["monday"]) == 1:
                    schedule["weekday"].append(dep)
                if int(row["saturday"]) == 1:
                    schedule["saturday"].append(dep)
                if int(row["sunday"]) == 1:
                    schedule["sunday"].append(dep)
            except (TypeError, ValueError):
                continue
        for key in schedule:
            schedule[key] = sorted(set(schedule[key]))
        return schedule

    def format_schedule_table(self, schedule: dict) -> pd.DataFrame:
        max_len = max(
            len(schedule["weekday"]),
            len(schedule["saturday"]),
            len(schedule["sunday"]),
        )
        weekday = schedule["weekday"] + [""] * (max_len - len(schedule["weekday"]))
        saturday = schedule["saturday"] + [""] * (max_len - len(schedule["saturday"]))
        sunday = schedule["sunday"] + [""] * (max_len - len(schedule["sunday"]))
        return pd.DataFrame(
            {
                "Weekday (Mon–Fri)": weekday,
                "Saturday": saturday,
                "Sunday": sunday,
            }
        )

    def create_route_map(self, route_id: str, direction: str):
        """Folium map with polyline and stop markers (same role as CSV visualizer)."""
        stops_df = self.get_direction_stops(route_id, direction)
        if stops_df.empty:
            return None
        center_lat = stops_df["stop_lat"].mean()
        center_lon = stops_df["stop_lon"].mean()
        m = folium.Map(location=[center_lat, center_lon], zoom_start=12, tiles="OpenStreetMap")
        coords = self.get_route_shape(route_id, direction)
        if coords and len(coords) >= 2:
            folium.PolyLine(
                coords,
                color="blue",
                weight=4,
                opacity=0.7,
                tooltip="Route shape",
            ).add_to(m)
        for idx, stop in stops_df.iterrows():
            if idx == stops_df.index[0]:
                folium.Marker(
                    location=[stop["stop_lat"], stop["stop_lon"]],
                    popup=(
                        f"<b>Start:</b><br>{stop['stop_name']}<br>{stop['departure_time']}"
                    ),
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

    def get_route_schedule(self, route_id: str, direction: str) -> pd.DataFrame:
        """Get schedule for a direction (server-side aggregation)."""
        
        # Get first stop departure times for all trips in this direction
        query = text("""
            WITH direction_trips AS (
                SELECT trip_id
                FROM trips
                WHERE route_id = :route_id
                AND (
                    trip_id LIKE :direction_pattern
                    OR direction_id = :direction_id
                )
            ),
            first_stops AS (
                SELECT 
                    st.trip_id,
                    st.departure_time,
                    ROW_NUMBER() OVER (PARTITION BY st.trip_id ORDER BY st.stop_sequence) as rn
                FROM stop_times st
                WHERE st.trip_id IN (SELECT trip_id FROM direction_trips)
            )
            SELECT 
                trip_id,
                seconds_to_gtfs_time(departure_time) as departure_time
            FROM first_stops
            WHERE rn = 1
            AND departure_time IS NOT NULL
            ORDER BY departure_time
        """)
        
        direction_id = direction.replace("dir_", "") if "dir_" in direction else "0"
        
        return pd.read_sql(
            query,
            self.engine,
            params={
                "route_id": route_id,
                "direction_pattern": f"%{direction}%",
                "direction_id": direction_id
            }
        )
    
    def get_nearby_stops(self, lat: float, lon: float, radius_m: int = 500) -> pd.DataFrame:
        """Find nearby stops using PostGIS (spatial index)."""
        
        query = text("""
            SELECT 
                stop_id,
                stop_name,
                stop_lat,
                stop_lon,
                ROUND(
                    ST_Distance(
                        geom::geography,
                        ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography
                    )::numeric,
                    2
                ) as distance_meters
            FROM stops
            WHERE geom IS NOT NULL
            AND ST_DWithin(
                geom::geography,
                ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography,
                :radius
            )
            ORDER BY distance_meters
            LIMIT 50
        """)
        
        return pd.read_sql(
            query,
            self.engine,
            params={"lat": lat, "lon": lon, "radius": radius_m}
        )
    
    def get_route_statistics(self) -> pd.DataFrame:
        """Get route statistics (server-side aggregation)."""
        
        query = text("""
            SELECT 
                r.route_short_name,
                r.route_long_name,
                r.route_type,
                COUNT(DISTINCT t.trip_id) as total_trips,
                COUNT(DISTINCT st.stop_id) as total_stops,
                MIN(st.departure_time) as first_departure,
                MAX(st.arrival_time) as last_arrival
            FROM routes r
            LEFT JOIN trips t ON r.route_id = t.route_id
            LEFT JOIN stop_times st ON t.trip_id = st.trip_id
            GROUP BY r.route_id, r.route_short_name, r.route_long_name, r.route_type
            HAVING COUNT(DISTINCT t.trip_id) > 0
            ORDER BY r.route_short_name
        """)
        
        return pd.read_sql(query, self.engine)
    
    def close(self):
        """Close database connections."""
        self.engine.dispose()
