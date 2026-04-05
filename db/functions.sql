-- ============================================================================
-- GTFS Database Helper Functions
-- Utility functions for common GTFS operations
-- ============================================================================

-- ============================================================================
-- FUNCTION: Convert GTFS time string (HH:MM:SS) to seconds
-- Handles post-midnight times like "25:30:00"
-- ============================================================================

CREATE OR REPLACE FUNCTION gtfs_time_to_seconds(time_str TEXT)
RETURNS INTEGER AS $$
DECLARE
    parts TEXT[];
    hours INTEGER;
    minutes INTEGER;
    seconds INTEGER;
BEGIN
    IF time_str IS NULL OR time_str = '' THEN
        RETURN NULL;
    END IF;
    
    -- Split by colon
    parts := string_to_array(time_str, ':');
    
    IF array_length(parts, 1) != 3 THEN
        RAISE WARNING 'Invalid time format: %', time_str;
        RETURN NULL;
    END IF;
    
    hours := parts[1]::INTEGER;
    minutes := parts[2]::INTEGER;
    seconds := parts[3]::INTEGER;
    
    -- Validate ranges (allow hours > 23 for post-midnight)
    IF minutes < 0 OR minutes > 59 OR seconds < 0 OR seconds > 59 THEN
        RAISE WARNING 'Invalid time components: %', time_str;
        RETURN NULL;
    END IF;
    
    RETURN (hours * 3600) + (minutes * 60) + seconds;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

COMMENT ON FUNCTION gtfs_time_to_seconds(TEXT) IS 
    'Convert GTFS time string (HH:MM:SS) to seconds since midnight. Supports post-midnight times (25:30:00).';

-- ============================================================================
-- FUNCTION: Convert seconds back to GTFS time string
-- ============================================================================

CREATE OR REPLACE FUNCTION seconds_to_gtfs_time(sec INTEGER)
RETURNS TEXT AS $$
DECLARE
    hours INTEGER;
    minutes INTEGER;
    seconds INTEGER;
BEGIN
    IF sec IS NULL THEN
        RETURN NULL;
    END IF;
    
    hours := sec / 3600;
    minutes := (sec % 3600) / 60;
    seconds := sec % 60;
    
    RETURN lpad(hours::TEXT, 2, '0') || ':' || 
           lpad(minutes::TEXT, 2, '0') || ':' || 
           lpad(seconds::TEXT, 2, '0');
END;
$$ LANGUAGE plpgsql IMMUTABLE;

COMMENT ON FUNCTION seconds_to_gtfs_time(INTEGER) IS 
    'Convert seconds since midnight to GTFS time string (HH:MM:SS). Supports post-midnight times.';

-- ============================================================================
-- FUNCTION: Find stops within radius (meters)
-- Returns stops within N meters of a point
-- ============================================================================

CREATE OR REPLACE FUNCTION find_stops_nearby(
    lat NUMERIC,
    lon NUMERIC,
    radius_meters INTEGER DEFAULT 500
)
RETURNS TABLE(
    stop_id TEXT,
    stop_name TEXT,
    stop_lat NUMERIC,
    stop_lon NUMERIC,
    distance_meters NUMERIC,
    location_type INTEGER
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        s.stop_id,
        s.stop_name,
        s.stop_lat,
        s.stop_lon,
        ST_Distance(
            s.geom::geography,
            ST_SetSRID(ST_MakePoint(lon, lat), 4326)::geography
        )::NUMERIC AS distance_meters,
        s.location_type
    FROM stops s
    WHERE s.geom IS NOT NULL
    AND ST_DWithin(
        s.geom::geography,
        ST_SetSRID(ST_MakePoint(lon, lat), 4326)::geography,
        radius_meters
    )
    ORDER BY distance_meters;
END;
$$ LANGUAGE plpgsql STABLE;

COMMENT ON FUNCTION find_stops_nearby(NUMERIC, NUMERIC, INTEGER) IS 
    'Find stops within radius_meters of a point (lat, lon). Returns ordered by distance.';

-- ============================================================================
-- FUNCTION: Check if service is active on a date
-- ============================================================================

CREATE OR REPLACE FUNCTION is_service_active(
    p_service_id TEXT,
    p_date DATE
)
RETURNS BOOLEAN AS $$
DECLARE
    dow INTEGER;
    is_regular_service BOOLEAN := FALSE;
    exception_type INTEGER;
BEGIN
    -- Check calendar_dates for exceptions first
    SELECT cd.exception_type INTO exception_type
    FROM calendar_dates cd
    WHERE cd.service_id = p_service_id
    AND cd.date = p_date;
    
    IF FOUND THEN
        -- 1 = service added, 2 = service removed
        RETURN (exception_type = 1);
    END IF;
    
    -- Check regular calendar
    dow := EXTRACT(ISODOW FROM p_date); -- 1=Monday, 7=Sunday
    
    SELECT 
        CASE dow
            WHEN 1 THEN c.monday
            WHEN 2 THEN c.tuesday
            WHEN 3 THEN c.wednesday
            WHEN 4 THEN c.thursday
            WHEN 5 THEN c.friday
            WHEN 6 THEN c.saturday
            WHEN 7 THEN c.sunday
        END = 1
        AND p_date BETWEEN c.start_date AND c.end_date
    INTO is_regular_service
    FROM calendar c
    WHERE c.service_id = p_service_id;
    
    RETURN COALESCE(is_regular_service, FALSE);
END;
$$ LANGUAGE plpgsql STABLE;

COMMENT ON FUNCTION is_service_active(TEXT, DATE) IS 
    'Check if a service_id is active on a specific date, considering calendar and calendar_dates.';

-- ============================================================================
-- FUNCTION: Get next departures from a stop
-- ============================================================================

CREATE OR REPLACE FUNCTION get_next_departures(
    p_stop_id TEXT,
    p_after_time INTEGER,  -- seconds since midnight
    p_service_id TEXT DEFAULT NULL,
    p_limit INTEGER DEFAULT 10
)
RETURNS TABLE(
    trip_id TEXT,
    route_id TEXT,
    route_short_name TEXT,
    route_long_name TEXT,
    trip_headsign TEXT,
    departure_time INTEGER,
    departure_time_str TEXT,
    stop_sequence INTEGER
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        st.trip_id,
        r.route_id,
        r.route_short_name,
        r.route_long_name,
        t.trip_headsign,
        st.departure_time,
        seconds_to_gtfs_time(st.departure_time) AS departure_time_str,
        st.stop_sequence
    FROM stop_times st
    JOIN trips t ON st.trip_id = t.trip_id
    JOIN routes r ON t.route_id = r.route_id
    WHERE st.stop_id = p_stop_id
    AND st.departure_time >= p_after_time
    AND (p_service_id IS NULL OR t.service_id = p_service_id)
    ORDER BY st.departure_time
    LIMIT p_limit;
END;
$$ LANGUAGE plpgsql STABLE;

COMMENT ON FUNCTION get_next_departures(TEXT, INTEGER, TEXT, INTEGER) IS 
    'Get next N departures from a stop after a given time (seconds since midnight).';

-- ============================================================================
-- FUNCTION: Get route shape as LineString
-- Aggregates shape points into a PostGIS LineString
-- ============================================================================

CREATE OR REPLACE FUNCTION get_route_shape(p_shape_id TEXT)
RETURNS GEOMETRY AS $$
BEGIN
    RETURN (
        SELECT ST_MakeLine(geom ORDER BY shape_pt_sequence)
        FROM shapes
        WHERE shape_id = p_shape_id
        AND geom IS NOT NULL
    );
END;
$$ LANGUAGE plpgsql STABLE;

COMMENT ON FUNCTION get_route_shape(TEXT) IS 
    'Get route shape as PostGIS LineString geometry.';

-- ============================================================================
-- FUNCTION: Calculate trip duration (first to last stop)
-- ============================================================================

CREATE OR REPLACE FUNCTION get_trip_duration(p_trip_id TEXT)
RETURNS INTEGER AS $$
DECLARE
    first_time INTEGER;
    last_time INTEGER;
BEGIN
    SELECT MIN(departure_time), MAX(arrival_time)
    INTO first_time, last_time
    FROM stop_times
    WHERE trip_id = p_trip_id;
    
    IF first_time IS NULL OR last_time IS NULL THEN
        RETURN NULL;
    END IF;
    
    RETURN last_time - first_time;
END;
$$ LANGUAGE plpgsql STABLE;

COMMENT ON FUNCTION get_trip_duration(TEXT) IS 
    'Calculate trip duration in seconds (from first departure to last arrival).';

-- ============================================================================
-- FUNCTION: Get stops for a trip in order
-- ============================================================================

CREATE OR REPLACE FUNCTION get_trip_stops(p_trip_id TEXT)
RETURNS TABLE(
    stop_sequence INTEGER,
    stop_id TEXT,
    stop_name TEXT,
    arrival_time INTEGER,
    departure_time INTEGER,
    stop_lat NUMERIC,
    stop_lon NUMERIC
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        st.stop_sequence,
        s.stop_id,
        s.stop_name,
        st.arrival_time,
        st.departure_time,
        s.stop_lat,
        s.stop_lon
    FROM stop_times st
    JOIN stops s ON st.stop_id = s.stop_id
    WHERE st.trip_id = p_trip_id
    ORDER BY st.stop_sequence;
END;
$$ LANGUAGE plpgsql STABLE;

COMMENT ON FUNCTION get_trip_stops(TEXT) IS 
    'Get all stops for a trip in sequence order.';

-- ============================================================================
-- FUNCTION: Find routes between two stops
-- Simple direct connection finder
-- ============================================================================

CREATE OR REPLACE FUNCTION find_routes_between_stops(
    p_from_stop_id TEXT,
    p_to_stop_id TEXT
)
RETURNS TABLE(
    route_id TEXT,
    route_short_name TEXT,
    route_long_name TEXT,
    route_type INTEGER,
    trip_count BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT
        r.route_id,
        r.route_short_name,
        r.route_long_name,
        r.route_type,
        COUNT(DISTINCT t.trip_id) AS trip_count
    FROM trips t
    JOIN routes r ON t.route_id = r.route_id
    WHERE EXISTS (
        SELECT 1 FROM stop_times st1
        WHERE st1.trip_id = t.trip_id
        AND st1.stop_id = p_from_stop_id
    )
    AND EXISTS (
        SELECT 1 FROM stop_times st2
        WHERE st2.trip_id = t.trip_id
        AND st2.stop_id = p_to_stop_id
        AND st2.stop_sequence > (
            SELECT stop_sequence FROM stop_times
            WHERE trip_id = t.trip_id AND stop_id = p_from_stop_id
            LIMIT 1
        )
    )
    GROUP BY r.route_id, r.route_short_name, r.route_long_name, r.route_type
    ORDER BY trip_count DESC;
END;
$$ LANGUAGE plpgsql STABLE;

COMMENT ON FUNCTION find_routes_between_stops(TEXT, TEXT) IS 
    'Find routes that connect two stops (from -> to in correct order).';

-- ============================================================================
-- FUNCTION: Database statistics
-- ============================================================================

CREATE OR REPLACE FUNCTION get_gtfs_stats()
RETURNS TABLE(
    table_name TEXT,
    row_count BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 'agencies'::TEXT, COUNT(*)::BIGINT FROM agency
    UNION ALL
    SELECT 'stops', COUNT(*) FROM stops
    UNION ALL
    SELECT 'routes', COUNT(*) FROM routes
    UNION ALL
    SELECT 'trips', COUNT(*) FROM trips
    UNION ALL
    SELECT 'stop_times', COUNT(*) FROM stop_times
    UNION ALL
    SELECT 'calendar', COUNT(*) FROM calendar
    UNION ALL
    SELECT 'calendar_dates', COUNT(*) FROM calendar_dates
    UNION ALL
    SELECT 'shapes', COUNT(*) FROM shapes
    UNION ALL
    SELECT 'transfers', COUNT(*) FROM transfers
    UNION ALL
    SELECT 'fare_attributes', COUNT(*) FROM fare_attributes
    UNION ALL
    SELECT 'fare_rules', COUNT(*) FROM fare_rules
    UNION ALL
    SELECT 'frequencies', COUNT(*) FROM frequencies;
END;
$$ LANGUAGE plpgsql STABLE;

COMMENT ON FUNCTION get_gtfs_stats() IS 
    'Get row counts for all GTFS tables.';

-- ============================================================================
-- Success message
-- ============================================================================
DO $$
BEGIN
    RAISE NOTICE '✓ All helper functions created successfully';
    RAISE NOTICE '✓ Time conversion functions ready';
    RAISE NOTICE '✓ Spatial query functions ready';
    RAISE NOTICE '✓ Trip planning functions ready';
END $$;
