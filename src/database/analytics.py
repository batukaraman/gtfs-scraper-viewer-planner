"""
Advanced analytics and performance monitoring for GTFS database.

Provides helper functions for:
- Performance monitoring
- Query optimization
- Spatial analytics
- Service patterns analysis
"""

from __future__ import annotations

import time
from contextlib import contextmanager
from typing import Generator

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


class GTFSAnalytics:
    """Advanced analytics for GTFS database."""
    
    def __init__(self, database_url: str):
        """Initialize analytics engine.
        
        Args:
            database_url: PostgreSQL connection string
        """
        self.engine = create_engine(database_url, echo=False)
    
    @contextmanager
    def measure_query(self, query_name: str) -> Generator[None, None, None]:
        """Context manager to measure query execution time.
        
        Usage:
            with analytics.measure_query("get_routes"):
                df = pd.read_sql(query, engine)
        """
        start = time.time()
        try:
            yield
        finally:
            elapsed = time.time() - start
            print(f"⏱️ {query_name}: {elapsed*1000:.2f}ms")
    
    def get_database_stats(self) -> pd.DataFrame:
        """Get comprehensive database statistics."""
        query = text("""
            SELECT 
                schemaname,
                tablename,
                pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS total_size,
                pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) AS table_size,
                pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename) - 
                              pg_relation_size(schemaname||'.'||tablename)) AS indexes_size,
                (SELECT COUNT(*) FROM information_schema.columns 
                 WHERE table_name = tablename) as column_count
            FROM pg_tables
            WHERE schemaname = 'public'
            ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
        """)
        
        return pd.read_sql(query, self.engine)
    
    def get_index_usage(self) -> pd.DataFrame:
        """Get index usage statistics to identify unused indexes."""
        query = text("""
            SELECT 
                schemaname,
                tablename,
                indexname,
                idx_scan as scans,
                idx_tup_read as tuples_read,
                idx_tup_fetch as tuples_fetched,
                pg_size_pretty(pg_relation_size(indexrelid)) as index_size
            FROM pg_stat_user_indexes
            WHERE schemaname = 'public'
            ORDER BY idx_scan DESC
        """)
        
        return pd.read_sql(query, self.engine)
    
    def get_slow_queries(self, min_duration_ms: float = 100) -> pd.DataFrame:
        """Get slow queries from pg_stat_statements (if enabled)."""
        try:
            query = text("""
                SELECT 
                    query,
                    calls,
                    ROUND(total_exec_time::numeric, 2) as total_time_ms,
                    ROUND(mean_exec_time::numeric, 2) as avg_time_ms,
                    ROUND(max_exec_time::numeric, 2) as max_time_ms
                FROM pg_stat_statements
                WHERE mean_exec_time > :min_duration
                ORDER BY mean_exec_time DESC
                LIMIT 20
            """)
            
            return pd.read_sql(query, self.engine, params={"min_duration": min_duration_ms})
        except Exception:
            return pd.DataFrame({"message": ["pg_stat_statements not enabled"]})
    
    def analyze_route_coverage(self) -> pd.DataFrame:
        """Analyze geographic coverage by route type."""
        query = text("""
            SELECT 
                r.route_type,
                CASE r.route_type
                    WHEN 0 THEN 'Tram'
                    WHEN 1 THEN 'Subway'
                    WHEN 2 THEN 'Rail'
                    WHEN 3 THEN 'Bus'
                    WHEN 4 THEN 'Ferry'
                    WHEN 5 THEN 'Cable Car'
                    WHEN 6 THEN 'Gondola'
                    WHEN 7 THEN 'Funicular'
                    ELSE 'Other'
                END as route_type_name,
                COUNT(DISTINCT r.route_id) as route_count,
                COUNT(DISTINCT t.trip_id) as trip_count,
                COUNT(DISTINCT st.stop_id) as stop_count,
                ROUND(
                    ST_Area(ST_ConvexHull(ST_Collect(s.geom))::geography)::numeric / 1000000,
                    2
                ) as coverage_area_km2
            FROM routes r
            LEFT JOIN trips t ON r.route_id = t.route_id
            LEFT JOIN stop_times st ON t.trip_id = st.trip_id
            LEFT JOIN stops s ON st.stop_id = s.stop_id
            WHERE s.geom IS NOT NULL
            GROUP BY r.route_type
            ORDER BY route_count DESC
        """)
        
        return pd.read_sql(query, self.engine)
    
    def get_service_patterns(self) -> pd.DataFrame:
        """Analyze service patterns (weekday/weekend/etc)."""
        query = text("""
            SELECT 
                service_id,
                start_date,
                end_date,
                CASE 
                    WHEN monday=1 AND tuesday=1 AND wednesday=1 AND thursday=1 AND friday=1 
                         AND saturday=0 AND sunday=0 THEN 'Weekdays'
                    WHEN monday=0 AND tuesday=0 AND wednesday=0 AND thursday=0 AND friday=0 
                         AND saturday=1 AND sunday=1 THEN 'Weekends'
                    WHEN monday=1 AND tuesday=1 AND wednesday=1 AND thursday=1 AND friday=1 
                         AND saturday=1 AND sunday=1 THEN 'Daily'
                    ELSE 'Custom'
                END as pattern,
                (SELECT COUNT(*) FROM trips WHERE service_id = calendar.service_id) as trip_count
            FROM calendar
            ORDER BY trip_count DESC
        """)
        
        return pd.read_sql(query, self.engine)
    
    def get_busiest_stops(self, limit: int = 20) -> pd.DataFrame:
        """Get busiest stops by departure count (server-side)."""
        query = text("""
            SELECT 
                s.stop_id,
                s.stop_name,
                s.stop_lat,
                s.stop_lon,
                COUNT(DISTINCT st.trip_id) as trip_count,
                COUNT(*) as departure_count
            FROM stops s
            JOIN stop_times st ON s.stop_id = st.stop_id
            GROUP BY s.stop_id, s.stop_name, s.stop_lat, s.stop_lon
            ORDER BY departure_count DESC
            LIMIT :limit
        """)
        
        return pd.read_sql(query, self.engine, params={"limit": limit})
    
    def get_transfer_network(self, min_transfer_time: int = 300) -> pd.DataFrame:
        """Get transfer network with distances (PostGIS)."""
        query = text("""
            SELECT 
                t.from_stop_id,
                t.to_stop_id,
                s1.stop_name as from_stop_name,
                s2.stop_name as to_stop_name,
                t.transfer_type,
                t.min_transfer_time,
                ROUND(
                    ST_Distance(s1.geom::geography, s2.geom::geography)::numeric,
                    2
                ) as distance_meters
            FROM transfers t
            JOIN stops s1 ON t.from_stop_id = s1.stop_id
            JOIN stops s2 ON t.to_stop_id = s2.stop_id
            WHERE t.min_transfer_time <= :max_time
            AND s1.geom IS NOT NULL
            AND s2.geom IS NOT NULL
            ORDER BY distance_meters
            LIMIT 1000
        """)
        
        return pd.read_sql(query, self.engine, params={"max_time": min_transfer_time})
    
    def vacuum_analyze(self) -> None:
        """Run VACUUM ANALYZE to optimize database performance."""
        with self.engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.execute(text("VACUUM ANALYZE"))
        print("✓ Database optimized (VACUUM ANALYZE complete)")
    
    def explain_query(self, query_sql: str) -> str:
        """Get query execution plan for optimization."""
        query = text(f"EXPLAIN ANALYZE {query_sql}")
        result = self.engine.execute(query)
        return "\n".join([row[0] for row in result])


def get_performance_report(database_url: str) -> Dict[str, pd.DataFrame]:
    """Generate comprehensive performance report.
    
    Args:
        database_url: PostgreSQL connection string
        
    Returns:
        Dictionary of DataFrames with various performance metrics
    """
    analytics = GTFSAnalytics(database_url)
    
    return {
        "database_stats": analytics.get_database_stats(),
        "index_usage": analytics.get_index_usage(),
        "service_patterns": analytics.get_service_patterns(),
        "busiest_stops": analytics.get_busiest_stops(),
        "route_coverage": analytics.analyze_route_coverage(),
    }
