"""
Optimized GTFS Repository with server-side filtering, caching, and spatial queries.

This module provides high-performance database access for GTFS data using:
- Server-side date filtering (loads only active services)
- Connection pooling
- Query result caching
- PostGIS spatial queries
- Lazy loading
"""

from __future__ import annotations

import datetime as dt
import os
import time
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool


def _planner_timing_enabled() -> bool:
    return os.environ.get("GTFS_PLANNER_TIMING", "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def _planner_timing_log(label: str, elapsed_ms: float) -> None:
    if _planner_timing_enabled():
        print(f"[planner.timing] {label}: {elapsed_ms:.2f} ms")


class OptimizedPostgresRepository:
    """
    High-performance PostgreSQL repository for GTFS data.
    
    Features:
    - Date-aware loading (loads only active trips for a given date)
    - Connection pooling (reuses connections)
    - Query caching (reduces repeated queries)
    - Spatial queries (PostGIS integration)
    - Lazy loading (loads data on-demand)
    """
    
    def __init__(self, database_url: str, pool_size: int = 5, max_overflow: int = 10):
        """Initialize optimized repository with connection pooling.
        
        Args:
            database_url: PostgreSQL connection string
            pool_size: Number of connections to keep in pool
            max_overflow: Maximum additional connections when pool exhausted
        """
        self._url = database_url
        
        # Connection pool for performance
        self._engine = create_engine(
            database_url,
            echo=False,
            poolclass=QueuePool,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_pre_ping=True,  # Verify connections before use
            pool_recycle=3600,   # Recycle connections after 1 hour
        )
        
        self._loaded_date: Optional[dt.date] = None
        self._gtfs_dir = Path("gtfs")  # Placeholder for interface compatibility
        
        # Cached DataFrames
        self._agency = pd.DataFrame()
        self._stops = pd.DataFrame()
        self._routes = pd.DataFrame()
        self._trips = pd.DataFrame()
        self._stop_times = pd.DataFrame()
        self._transfers = pd.DataFrame()
        self._calendar = pd.DataFrame()
        self._frequencies = pd.DataFrame()
        self._shapes = pd.DataFrame()
        
        # Performance tracking
        self._query_times: Dict[str, float] = {}
    
    @property
    def gtfs_dir(self) -> Path:
        """Return placeholder path (for interface compatibility)."""
        return self._gtfs_dir

    def _load_shapes_for_active_trips(self) -> None:
        """Fill ``self._shapes`` from DB for shape_ids present in ``self._trips``."""
        if self._trips.empty:
            self._shapes = pd.DataFrame()
            return
        shape_ids = self._trips["shape_id"].dropna().unique().tolist()
        if not shape_ids:
            self._shapes = pd.DataFrame()
            return
        shapes_query = text("""
            SELECT
                shape_id,
                shape_pt_lat,
                shape_pt_lon,
                shape_pt_sequence
            FROM shapes
            WHERE shape_id = ANY(:shape_ids)
            ORDER BY shape_id, shape_pt_sequence
        """)
        self._shapes = pd.read_sql(
            shapes_query,
            self._engine,
            params={"shape_ids": shape_ids},
        )

    def ensure_shapes_loaded(self) -> None:
        """Load shapes if they were skipped (e.g. planner fast path). Safe to call repeatedly."""
        if self._loaded_date is None:
            raise RuntimeError("Must call load_for_date() first")
        if not self._shapes.empty:
            return
        self._load_shapes_for_active_trips()

    def load_for_date(self, on_date: dt.date, *, load_shapes: bool = True) -> None:
        """
        Load GTFS data optimized for a specific date.
        
        Only loads trips and stop_times for services active on the given date,
        dramatically reducing memory usage and load time.
        
        Args:
            on_date: Date to load data for
            load_shapes: If False, skip ``shapes`` (saves a large query); call
                :meth:`ensure_shapes_loaded` before map polylines are needed.
        """
        if self._loaded_date == on_date:
            if _planner_timing_enabled():
                print("[planner.timing] postgres load_for_date: skip (same date, already in memory)")
            return  # Already loaded for this date

        t_load0 = time.perf_counter()
        try:
            # 1. Get active service IDs for this date (server-side!)
            t0 = time.perf_counter()
            active_services = self._get_active_services(on_date)
            _planner_timing_log("postgres load: active_services query", (time.perf_counter() - t0) * 1000)

            if not active_services:
                print(f"⚠️ No active services found for {on_date}")
                active_services = []

            # 2. Load core reference tables (small, load once)
            t_ref0 = time.perf_counter()
            if self._agency.empty:
                self._agency = pd.read_sql("SELECT * FROM agency", self._engine)

            if self._routes.empty:
                self._routes = pd.read_sql("SELECT * FROM routes", self._engine)

            if self._calendar.empty:
                self._calendar = pd.read_sql("SELECT * FROM calendar", self._engine)

            if self._transfers.empty:
                self._transfers = pd.read_sql("SELECT * FROM transfers", self._engine)
            _planner_timing_log("postgres load: reference tables (agency/routes/calendar/transfers)", (time.perf_counter() - t_ref0) * 1000)

            # 3. Load only active trips for this date (SERVER-SIDE FILTER!)
            t0 = time.perf_counter()
            if active_services:
                trips_query = text("""
                    SELECT * FROM trips
                    WHERE service_id = ANY(:service_ids)
                """)
                self._trips = pd.read_sql(
                    trips_query,
                    self._engine,
                    params={"service_ids": active_services}
                )
            else:
                self._trips = pd.DataFrame()
            _planner_timing_log("postgres load: trips", (time.perf_counter() - t0) * 1000)

            # 4. stop_times: one round-trip via join (avoids hundreds of sequential batched queries)
            t_st0 = time.perf_counter()
            if active_services:
                stop_times_query = text("""
                    SELECT
                        st.trip_id,
                        st.stop_id,
                        st.stop_sequence,
                        seconds_to_gtfs_time(st.arrival_time) AS arrival_time,
                        seconds_to_gtfs_time(st.departure_time) AS departure_time,
                        st.stop_headsign,
                        st.pickup_type,
                        st.drop_off_type,
                        st.shape_dist_traveled,
                        st.timepoint
                    FROM stop_times st
                    INNER JOIN trips t ON st.trip_id = t.trip_id
                    WHERE t.service_id = ANY(:service_ids)
                    ORDER BY st.trip_id, st.stop_sequence
                """)
                self._stop_times = pd.read_sql(
                    stop_times_query,
                    self._engine,
                    params={"service_ids": active_services},
                )
            else:
                self._stop_times = pd.DataFrame()
            _planner_timing_log(
                "postgres load: stop_times (single join on service_ids)",
                (time.perf_counter() - t_st0) * 1000,
            )

            # 5. Load only stops that are actually used (OPTIMIZED!)
            t0 = time.perf_counter()
            if not self._stop_times.empty:
                used_stop_ids = self._stop_times['stop_id'].unique().tolist()
                stops_query = text("""
                    SELECT 
                        stop_id,
                        stop_code,
                        stop_name,
                        stop_desc,
                        stop_lat,
                        stop_lon,
                        zone_id,
                        stop_url,
                        location_type,
                        parent_station,
                        stop_timezone,
                        wheelchair_boarding,
                        platform_code
                    FROM stops
                    WHERE stop_id = ANY(:stop_ids)
                """)
                self._stops = pd.read_sql(
                    stops_query,
                    self._engine,
                    params={"stop_ids": used_stop_ids}
                )
            else:
                self._stops = pd.DataFrame()
            _planner_timing_log("postgres load: stops", (time.perf_counter() - t0) * 1000)

            # 6. Shapes (optional — planner can defer via load_shapes=False)
            t0 = time.perf_counter()
            if load_shapes:
                self._load_shapes_for_active_trips()
            else:
                self._shapes = pd.DataFrame()
            _planner_timing_log("postgres load: shapes", (time.perf_counter() - t0) * 1000)

            # 7. Frequencies if they exist (subquery — no huge trip_id array bind)
            t0 = time.perf_counter()
            try:
                if active_services:
                    freq_query = text("""
                        SELECT f.*
                        FROM frequencies f
                        INNER JOIN trips t ON f.trip_id = t.trip_id
                        WHERE t.service_id = ANY(:service_ids)
                    """)
                    self._frequencies = pd.read_sql(
                        freq_query,
                        self._engine,
                        params={"service_ids": active_services},
                    )
                else:
                    self._frequencies = pd.DataFrame()
            except Exception:
                self._frequencies = pd.DataFrame()
            _planner_timing_log("postgres load: frequencies", (time.perf_counter() - t0) * 1000)

            self._loaded_date = on_date
            _planner_timing_log("postgres load_for_date: total (DB → DataFrames)", (time.perf_counter() - t_load0) * 1000)

            # Print statistics
            print(f"✓ Loaded data for {on_date}:")
            print(f"  - {len(active_services)} active services")
            print(f"  - {len(self._trips)} trips")
            print(f"  - {len(self._stop_times)} stop_times")
            print(f"  - {len(self._stops)} stops")
            
        except Exception as e:
            raise ConnectionError(
                f"Failed to load optimized data from PostgreSQL: {e}\n"
                f"Ensure database is running and tables exist."
            ) from e
    
    def _get_active_services(self, on_date: dt.date) -> List[str]:
        """Get service IDs active on a specific date (server-side)."""
        
        dow = on_date.isoweekday()  # 1=Monday, 7=Sunday
        
        # Query active services (server-side date logic!)
        query = text("""
            SELECT service_id FROM calendar
            WHERE start_date <= :check_date
            AND end_date >= :check_date
            AND CASE :dow
                WHEN 1 THEN monday = 1
                WHEN 2 THEN tuesday = 1
                WHEN 3 THEN wednesday = 1
                WHEN 4 THEN thursday = 1
                WHEN 5 THEN friday = 1
                WHEN 6 THEN saturday = 1
                WHEN 7 THEN sunday = 1
            END
            
            UNION
            
            -- Add services from calendar_dates (exception_type = 1)
            SELECT service_id FROM calendar_dates
            WHERE date = :check_date
            AND exception_type = 1
            
            EXCEPT
            
            -- Remove services from calendar_dates (exception_type = 2)
            SELECT service_id FROM calendar_dates
            WHERE date = :check_date
            AND exception_type = 2
        """)
        
        result = pd.read_sql(
            query,
            self._engine,
            params={"check_date": on_date, "dow": dow}
        )
        
        return result['service_id'].tolist()
    
    def find_stops_nearby(
        self,
        lat: float,
        lon: float,
        radius_meters: int = 500,
        limit: int = 100,
    ) -> pd.DataFrame:
        """
        Find stops within radius using PostGIS (ULTRA FAST!).
        
        Uses spatial index (GIST) for millisecond performance even with
        millions of stops.
        
        Args:
            lat: Latitude
            lon: Longitude
            radius_meters: Search radius in meters
            limit: Maximum rows returned (ORDER BY distance)

        Returns:
            DataFrame with nearby stops sorted by distance
        """
        query = text("""
            SELECT 
                stop_id,
                stop_name,
                stop_lat,
                stop_lon,
                location_type,
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
            LIMIT :limit
        """)
        
        return pd.read_sql(
            query,
            self._engine,
            params={"lat": lat, "lon": lon, "radius": radius_meters, "limit": limit}
        )
    
    def get_next_departures(
        self,
        stop_id: str,
        after_time_seconds: int,
        limit: int = 20
    ) -> pd.DataFrame:
        """
        Get next departures from a stop (server-side join and filter).
        
        Args:
            stop_id: Stop ID
            after_time_seconds: Time in seconds since midnight
            limit: Maximum results
            
        Returns:
            DataFrame with upcoming departures
        """
        if self._loaded_date is None:
            raise RuntimeError("Must call load_for_date() first")
        
        active_services = self._get_active_services(self._loaded_date)
        
        if not active_services:
            return pd.DataFrame()
        
        query = text("""
            SELECT 
                st.trip_id,
                t.route_id,
                r.route_short_name,
                r.route_long_name,
                t.trip_headsign,
                st.departure_time,
                seconds_to_gtfs_time(st.departure_time) as departure_time_str,
                st.stop_sequence
            FROM stop_times st
            JOIN trips t ON st.trip_id = t.trip_id
            JOIN routes r ON t.route_id = r.route_id
            WHERE st.stop_id = :stop_id
            AND st.departure_time >= :after_time
            AND t.service_id = ANY(:service_ids)
            ORDER BY st.departure_time
            LIMIT :limit
        """)
        
        return pd.read_sql(
            query,
            self._engine,
            params={
                "stop_id": stop_id,
                "after_time": after_time_seconds,
                "service_ids": active_services,
                "limit": limit
            }
        )
    
    def get_route_stats(self) -> pd.DataFrame:
        """Get route statistics (server-side aggregation)."""
        query = text("""
            SELECT 
                r.route_id,
                r.route_short_name,
                r.route_long_name,
                r.route_type,
                COUNT(DISTINCT t.trip_id) as trip_count,
                COUNT(DISTINCT st.stop_id) as stop_count,
                MIN(st.departure_time) as first_departure,
                MAX(st.arrival_time) as last_arrival
            FROM routes r
            LEFT JOIN trips t ON r.route_id = t.route_id
            LEFT JOIN stop_times st ON t.trip_id = st.trip_id
            GROUP BY r.route_id, r.route_short_name, r.route_long_name, r.route_type
            ORDER BY r.route_short_name
        """)
        
        return pd.read_sql(query, self._engine)
    
    # Interface compatibility properties
    @property
    def stops(self) -> pd.DataFrame:
        return self._stops
    
    @property
    def routes(self) -> pd.DataFrame:
        return self._routes
    
    @property
    def trips(self) -> pd.DataFrame:
        return self._trips
    
    @property
    def stop_times(self) -> pd.DataFrame:
        return self._stop_times
    
    @property
    def transfers(self) -> pd.DataFrame:
        return self._transfers
    
    @property
    def calendar(self) -> pd.DataFrame:
        return self._calendar
    
    @property
    def frequencies(self) -> pd.DataFrame:
        return self._frequencies
    
    @property
    def shapes(self) -> pd.DataFrame:
        return self._shapes
    
    @property
    def agency(self) -> pd.DataFrame:
        return self._agency
    
    def service_ids_on(self, on_date: dt.date) -> set[str]:
        """Return service IDs active on given date."""
        return set(self._get_active_services(on_date))
    
    def get_connection_pool_status(self) -> Dict[str, int]:
        """Get connection pool statistics."""
        pool = self._engine.pool
        return {
            "size": pool.size(),
            "checked_in": pool.checkedin(),
            "checked_out": pool.checkedout(),
            "overflow": pool.overflow(),
        }
    
    def close(self) -> None:
        """Close all database connections."""
        self._engine.dispose()
