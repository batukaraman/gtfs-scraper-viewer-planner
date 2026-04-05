-- ============================================================================
-- Advanced GTFS Database Functions
-- High-performance routing and analytics functions
-- ============================================================================

-- ============================================================================
-- FUNCTION: Get active services for a date (optimized)
-- Used by optimized repositories for date-filtered loading
-- ============================================================================

CREATE OR REPLACE FUNCTION get_active_services_for_date(check_date DATE)
RETURNS TABLE(service_id TEXT) AS $$
BEGIN
    RETURN QUERY
    -- Regular calendar services
    SELECT c.service_id
    FROM calendar c
    WHERE c.start_date <= check_date
    AND c.end_date >= check_date
    AND CASE EXTRACT(ISODOW FROM check_date)
        WHEN 1 THEN c.monday = 1
        WHEN 2 THEN c.tuesday = 1
        WHEN 3 THEN c.wednesday = 1
        WHEN 4 THEN c.thursday = 1
        WHEN 5 THEN c.friday = 1
        WHEN 6 THEN c.saturday = 1
        WHEN 7 THEN c.sunday = 1
    END
    
    UNION
    
    -- Add services from calendar_dates (exception_type = 1)
    SELECT cd.service_id
    FROM calendar_dates cd
    WHERE cd.date = check_date
    AND cd.exception_type = 1
    
    EXCEPT
    
    -- Remove services from calendar_dates (exception_type = 2)
    SELECT cd.service_id
    FROM calendar_dates cd
    WHERE cd.date = check_date
    AND cd.exception_type = 2;
END;
$$ LANGUAGE plpgsql STABLE;

COMMENT ON FUNCTION get_active_services_for_date(DATE) IS 
    'Get all service_ids active on a specific date (handles calendar + exceptions)';

-- ============================================================================
-- FUNCTION: Get trips active on a date (single query)
-- Joins with routes for complete trip info
-- ============================================================================

CREATE OR REPLACE FUNCTION get_active_trips_on_date(check_date DATE)
RETURNS TABLE(
    trip_id TEXT,
    route_id TEXT,
    service_id TEXT,
    trip_headsign TEXT,
    direction_id INTEGER,
    shape_id TEXT,
    route_short_name TEXT,
    route_long_name TEXT,
    route_type INTEGER
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        t.trip_id,
        t.route_id,
        t.service_id,
        t.trip_headsign,
        t.direction_id,
        t.shape_id,
        r.route_short_name,
        r.route_long_name,
        r.route_type
    FROM trips t
    JOIN routes r ON t.route_id = r.route_id
    WHERE t.service_id IN (
        SELECT * FROM get_active_services_for_date(check_date)
    );
END;
$$ LANGUAGE plpgsql STABLE;

COMMENT ON FUNCTION get_active_trips_on_date(DATE) IS 
    'Get all trips active on a date with route information (optimized single query)';

-- ============================================================================
-- FUNCTION: Get next N departures from stop after time
-- Optimized with proper indexes
-- ============================================================================

CREATE OR REPLACE FUNCTION get_next_departures_optimized(
    p_stop_id TEXT,
    p_after_time INTEGER,
    p_date DATE DEFAULT CURRENT_DATE,
    p_limit INTEGER DEFAULT 20
)
RETURNS TABLE(
    trip_id TEXT,
    route_id TEXT,
    route_short_name TEXT,
    route_long_name TEXT,
    route_type INTEGER,
    trip_headsign TEXT,
    departure_time INTEGER,
    departure_time_str TEXT,
    stop_sequence INTEGER,
    shape_id TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        st.trip_id,
        t.route_id,
        r.route_short_name,
        r.route_long_name,
        r.route_type,
        t.trip_headsign,
        st.departure_time,
        seconds_to_gtfs_time(st.departure_time) AS departure_time_str,
        st.stop_sequence,
        t.shape_id
    FROM stop_times st
    JOIN trips t ON st.trip_id = t.trip_id
    JOIN routes r ON t.route_id = r.route_id
    WHERE st.stop_id = p_stop_id
    AND st.departure_time >= p_after_time
    AND t.service_id IN (
        SELECT * FROM get_active_services_for_date(p_date)
    )
    ORDER BY st.departure_time
    LIMIT p_limit;
END;
$$ LANGUAGE plpgsql STABLE;

COMMENT ON FUNCTION get_next_departures_optimized(TEXT, INTEGER, DATE, INTEGER) IS 
    'Get next departures with full route info (optimized with date filtering)';

-- ============================================================================
-- FUNCTION: Route frequency analysis
-- Calculate average headway between trips
-- ============================================================================

CREATE OR REPLACE FUNCTION analyze_route_frequency(p_route_id TEXT, p_date DATE DEFAULT CURRENT_DATE)
RETURNS TABLE(
    hour_of_day INTEGER,
    trip_count INTEGER,
    avg_headway_minutes NUMERIC
) AS $$
BEGIN
    RETURN QUERY
    WITH active_trips AS (
        SELECT t.trip_id
        FROM trips t
        WHERE t.route_id = p_route_id
        AND t.service_id IN (
            SELECT * FROM get_active_services_for_date(p_date)
        )
    ),
    first_departures AS (
        SELECT 
            st.trip_id,
            MIN(st.departure_time) as first_departure
        FROM stop_times st
        WHERE st.trip_id IN (SELECT trip_id FROM active_trips)
        GROUP BY st.trip_id
    ),
    hourly_deps AS (
        SELECT 
            (first_departure / 3600) as hour,
            first_departure,
            LAG(first_departure) OVER (ORDER BY first_departure) as prev_departure
        FROM first_departures
    )
    SELECT 
        hour::INTEGER,
        COUNT(*)::INTEGER as trips,
        ROUND(AVG((first_departure - prev_departure) / 60.0)::numeric, 1) as avg_headway_min
    FROM hourly_deps
    WHERE prev_departure IS NOT NULL
    GROUP BY hour
    ORDER BY hour;
END;
$$ LANGUAGE plpgsql STABLE;

COMMENT ON FUNCTION analyze_route_frequency(TEXT, DATE) IS 
    'Analyze route frequency by hour (average headway in minutes)';

-- ============================================================================
-- FUNCTION: Find transfer hubs (stations with many transfers)
-- ============================================================================

CREATE OR REPLACE FUNCTION find_transfer_hubs(min_transfers INTEGER DEFAULT 10)
RETURNS TABLE(
    stop_id TEXT,
    stop_name TEXT,
    stop_lat NUMERIC,
    stop_lon NUMERIC,
    transfer_count BIGINT,
    route_count BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        s.stop_id,
        s.stop_name,
        s.stop_lat,
        s.stop_lon,
        COUNT(DISTINCT t.from_stop_id) + COUNT(DISTINCT t.to_stop_id) as transfers,
        COUNT(DISTINCT st.trip_id) as routes
    FROM stops s
    LEFT JOIN transfers t ON s.stop_id = t.from_stop_id OR s.stop_id = t.to_stop_id
    LEFT JOIN stop_times st ON s.stop_id = st.stop_id
    GROUP BY s.stop_id, s.stop_name, s.stop_lat, s.stop_lon
    HAVING COUNT(DISTINCT t.from_stop_id) + COUNT(DISTINCT t.to_stop_id) >= min_transfers
    ORDER BY transfers DESC;
END;
$$ LANGUAGE plpgsql STABLE;

COMMENT ON FUNCTION find_transfer_hubs(INTEGER) IS 
    'Find major transfer hubs with many transfer options';

-- ============================================================================
-- FUNCTION: Calculate travel time between two stops
-- Finds fastest route considering all trips
-- ============================================================================

CREATE OR REPLACE FUNCTION calculate_min_travel_time(
    p_from_stop_id TEXT,
    p_to_stop_id TEXT,
    p_date DATE DEFAULT CURRENT_DATE
)
RETURNS TABLE(
    trip_id TEXT,
    route_short_name TEXT,
    travel_time_minutes INTEGER,
    departure_time_str TEXT,
    arrival_time_str TEXT
) AS $$
BEGIN
    RETURN QUERY
    WITH active_services AS (
        SELECT * FROM get_active_services_for_date(p_date)
    ),
    matching_trips AS (
        SELECT DISTINCT st1.trip_id
        FROM stop_times st1
        JOIN stop_times st2 ON st1.trip_id = st2.trip_id
        JOIN trips t ON st1.trip_id = t.trip_id
        WHERE st1.stop_id = p_from_stop_id
        AND st2.stop_id = p_to_stop_id
        AND st2.stop_sequence > st1.stop_sequence
        AND t.service_id IN (SELECT * FROM active_services)
    )
    SELECT 
        st1.trip_id,
        r.route_short_name,
        ((st2.arrival_time - st1.departure_time) / 60)::INTEGER as travel_minutes,
        seconds_to_gtfs_time(st1.departure_time) as departure,
        seconds_to_gtfs_time(st2.arrival_time) as arrival
    FROM stop_times st1
    JOIN stop_times st2 ON st1.trip_id = st2.trip_id
    JOIN trips t ON st1.trip_id = t.trip_id
    JOIN routes r ON t.route_id = r.route_id
    WHERE st1.stop_id = p_from_stop_id
    AND st2.stop_id = p_to_stop_id
    AND st2.stop_sequence > st1.stop_sequence
    AND st1.trip_id IN (SELECT trip_id FROM matching_trips)
    ORDER BY travel_minutes
    LIMIT 10;
END;
$$ LANGUAGE plpgsql STABLE;

COMMENT ON FUNCTION calculate_min_travel_time(TEXT, TEXT, DATE) IS 
    'Find fastest travel times between two stops on a given date';

-- ============================================================================
-- FUNCTION: Spatial heatmap data
-- Get stop density in grid cells
-- ============================================================================

CREATE OR REPLACE FUNCTION get_stop_density_grid(
    min_lat NUMERIC,
    min_lon NUMERIC,
    max_lat NUMERIC,
    max_lon NUMERIC,
    grid_size_km NUMERIC DEFAULT 1.0
)
RETURNS TABLE(
    cell_lat NUMERIC,
    cell_lon NUMERIC,
    stop_count BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        ROUND((stop_lat / grid_size_km)::numeric, 2) * grid_size_km as lat,
        ROUND((stop_lon / grid_size_km)::numeric, 2) * grid_size_km as lon,
        COUNT(*) as stops
    FROM stops
    WHERE stop_lat BETWEEN min_lat AND max_lat
    AND stop_lon BETWEEN min_lon AND max_lon
    AND geom IS NOT NULL
    GROUP BY lat, lon
    HAVING COUNT(*) > 0
    ORDER BY stops DESC;
END;
$$ LANGUAGE plpgsql STABLE;

COMMENT ON FUNCTION get_stop_density_grid(NUMERIC, NUMERIC, NUMERIC, NUMERIC, NUMERIC) IS 
    'Generate grid-based stop density for heatmaps';

-- ============================================================================
-- Create indexes for new functions
-- ============================================================================

-- Service date lookups (if not already exists)
CREATE INDEX IF NOT EXISTS idx_calendar_date_range 
    ON calendar(start_date, end_date) 
    WHERE (monday + tuesday + wednesday + thursday + friday + saturday + sunday) > 0;

CREATE INDEX IF NOT EXISTS idx_calendar_dates_lookup 
    ON calendar_dates(date, service_id, exception_type);

-- Trip lookups by service
CREATE INDEX IF NOT EXISTS idx_trips_service_route 
    ON trips(service_id, route_id);

-- Stop times fast lookups
CREATE INDEX IF NOT EXISTS idx_stop_times_stop_time 
    ON stop_times(stop_id, departure_time) 
    WHERE departure_time IS NOT NULL;

-- ============================================================================
-- Success message
-- ============================================================================
DO $$
BEGIN
    RAISE NOTICE '✓ Advanced helper functions created';
    RAISE NOTICE '✓ Performance-optimized queries ready';
    RAISE NOTICE '✓ Spatial analytics functions available';
END $$;
