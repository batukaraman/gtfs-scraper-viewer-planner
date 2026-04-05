#!/usr/bin/env python3
"""
Quick test script to verify database connection and schema.
"""

import os
import sys
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pandas as pd


def test_connection(database_url: str = None):
    """Test database connection and verify schema.
    
    Args:
        database_url: PostgreSQL connection string. If None, reads from DATABASE_URL env var.
    """
    # Load environment
    load_dotenv()
    
    if database_url is None:
        database_url = os.getenv('DATABASE_URL')
    
    if not database_url:
        print("[ERROR] DATABASE_URL not set in .env file")
        sys.exit(1)
    
    print("Testing Database Connection")
    print("=" * 60)
    
    try:
        # Connect
        engine = create_engine(database_url)
        
        with engine.connect() as conn:
            # Test 1: Check PostgreSQL version
            result = conn.execute(text("SELECT version()"))
            version = result.fetchone()[0]
            print(f"[OK] PostgreSQL: {version.split(',')[0]}")
            
            # Test 2: Check PostGIS
            result = conn.execute(text("SELECT PostGIS_Version()"))
            postgis_version = result.fetchone()[0]
            print(f"[OK] PostGIS: {postgis_version}")
            
            # Test 3: List tables
            result = conn.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                ORDER BY table_name
            """))
            tables = [row[0] for row in result]
            print(f"[OK] Tables found: {len(tables)}")
            for table in tables:
                print(f"   - {table}")
            
            # Test 4: Check for data
            print("\nData Summary:")
            print("-" * 60)
            
            for table in ['agency', 'stops', 'routes', 'trips', 'stop_times']:
                if table in tables:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                    count = result.fetchone()[0]
                    print(f"   {table:15s} {count:>12,} rows")
            
            # Test 5: Test spatial function
            print("\nTesting PostGIS Functions:")
            print("-" * 60)
            
            result = conn.execute(text("""
                SELECT COUNT(*) FROM stops WHERE geom IS NOT NULL
            """))
            geom_count = result.fetchone()[0]
            print(f"   Stops with geometry: {geom_count:,}")
            
            if geom_count > 0:
                # Find a sample stop
                result = conn.execute(text("""
                    SELECT stop_id, stop_name, stop_lat, stop_lon,
                           ST_AsText(geom) as wkt
                    FROM stops 
                    WHERE geom IS NOT NULL 
                    LIMIT 1
                """))
                sample = result.fetchone()
                print(f"\n   Sample stop:")
                print(f"   - ID: {sample[0]}")
                print(f"   - Name: {sample[1]}")
                print(f"   - Coordinates: ({sample[2]}, {sample[3]})")
                print(f"   - WKT: {sample[4]}")
                
                # Test nearby search
                result = conn.execute(text(f"""
                    SELECT * FROM find_stops_nearby(
                        {sample[2]}, {sample[3]}, 1000
                    ) LIMIT 5
                """))
                nearby = result.fetchall()
                print(f"\n   Stops within 1km: {len(nearby)}")
                for stop in nearby[:3]:
                    print(f"   - {stop[1]} ({stop[4]:.0f}m)")
            
            # Test 6: Test helper functions
            print("\nTesting Helper Functions:")
            print("-" * 60)
            
            # Time conversion
            result = conn.execute(text("""
                SELECT 
                    gtfs_time_to_seconds('09:30:00') as seconds,
                    seconds_to_gtfs_time(34200) as time_str,
                    gtfs_time_to_seconds('25:30:00') as overnight
            """))
            times = result.fetchone()
            print(f"   Time conversions:")
            print(f"   - 09:30:00 = {times[0]} seconds")
            print(f"   - 34200 seconds = {times[1]}")
            print(f"   - 25:30:00 (post-midnight) = {times[2]} seconds")
            
            # Database stats
            result = conn.execute(text("SELECT * FROM get_gtfs_stats()"))
            stats = result.fetchall()
            print(f"\n   Database statistics:")
            total_rows = sum(row[1] for row in stats)
            print(f"   Total rows across all tables: {total_rows:,}")
        
        print("\n" + "=" * 60)
        print("[SUCCESS] All tests passed! Database is ready.")
        print("=" * 60)
        
    except Exception as e:
        print("\n" + "=" * 60)
        print(f"[ERROR] {e}")
        print("=" * 60)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    test_connection()
