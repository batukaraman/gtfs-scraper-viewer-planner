-- ============================================================================
-- GTFS Database - Example Queries
-- Collection of useful queries for common transit operations
-- ============================================================================

-- ============================================================================
-- BASIC QUERIES
-- ============================================================================

-- 1. List all agencies
SELECT 
    agency_id,
    agency_name,
    agency_timezone,
    agency_url
FROM agency
ORDER BY agency_name;

-- 2. Count records in each table
SELECT * FROM get_gtfs_stats()
ORDER BY row_count DESC;

-- 3. Find routes by agency
SELECT 
    a.agency_name,
    r.route_short_name,
    r.route_long_name,
    r.route_type,
    COUNT(DISTINCT t.trip_id) as trip_count
FROM routes r
JOIN agency a ON r.agency_id = a.agency_id
LEFT JOIN trips t ON r.route_id = t.route_id
GROUP BY a.agency_name, r.route_id, r.route_short_name, r.route_long_name, r.route_type
ORDER BY a.agency_name, r.route_short_name;

-- ============================================================================
-- SPATIAL QUERIES (PostGIS)
-- ============================================================================

-- 4. Find stops near a point (within 500m)
-- Example: Near Taksim Square, Istanbul (41.0369, 28.9850)
SELECT * FROM find_stops_nearby(41.0369, 28.9850, 500)
LIMIT 20;

-- 5. Find all stops in a bounding box
-- Example: Central Istanbul
SELECT 
    stop_id,
    stop_name,
    stop_lat,
    stop_lon,
    location_type
FROM stops
WHERE geom && ST_MakeEnvelope(28.95, 41.00, 29.05, 41.10, 4326)  -- bbox
ORDER BY stop_name;

-- 6. Calculate distance between two stops
SELECT 
    s1.stop_name as from_stop,
    s2.stop_name as to_stop,
    ROUND(
        ST_Distance(s1.geom::geography, s2.geom::geography)::NUMERIC, 2
    ) as distance_meters
FROM stops s1
CROSS JOIN stops s2
WHERE s1.stop_id = 'stop_A'  -- Replace with actual stop_id
AND s2.stop_id = 'stop_B'    -- Replace with actual stop_id
AND s1.geom IS NOT NULL
AND s2.geom IS NOT NULL;

-- 7. Find stops along a route (with geometry)
SELECT 
    st.stop_sequence,
    s.stop_id,
    s.stop_name,
    s.stop_lat,
    s.stop_lon,
    ST_AsGeoJSON(s.geom) as geojson
FROM stop_times st
JOIN stops s ON st.stop_id = s.stop_id
WHERE st.trip_id = 'trip_123'  -- Replace with actual trip_id
ORDER BY st.stop_sequence;

-- ============================================================================
-- TIME-BASED QUERIES
-- ============================================================================

-- 8. Next departures from a stop (after 10:00 AM)
SELECT * FROM get_next_departures(
    'stop_123',        -- stop_id (replace with actual)
    36000,             -- 10:00:00 in seconds
    NULL,              -- any service_id
    10                 -- limit 10 results
);

-- 9. Find trips active in a time window
SELECT 
    t.trip_id,
    r.route_short_name,
    t.trip_headsign,
    seconds_to_gtfs_time(MIN(st.departure_time)) as first_departure,
    seconds_to_gtfs_time(MAX(st.arrival_time)) as last_arrival,
    get_trip_duration(t.trip_id) as duration_seconds
FROM trips t
JOIN routes r ON t.route_id = r.route_id
JOIN stop_times st ON t.trip_id = st.trip_id
WHERE st.departure_time BETWEEN 25200 AND 39600  -- 07:00 - 11:00
GROUP BY t.trip_id, r.route_short_name, t.trip_headsign
ORDER BY first_departure
LIMIT 50;

-- 10. Check if service is active on a date
SELECT 
    service_id,
    is_service_active(service_id, '2026-04-05') as is_active
FROM calendar;

-- 11. Find services running on weekdays
SELECT 
    service_id,
    start_date,
    end_date,
    CASE 
        WHEN monday=1 AND tuesday=1 AND wednesday=1 AND thursday=1 AND friday=1 
             AND saturday=0 AND sunday=0 THEN 'Weekdays Only'
        WHEN monday=0 AND tuesday=0 AND wednesday=0 AND thursday=0 AND friday=0 
             AND saturday=1 AND sunday=1 THEN 'Weekends Only'
        ELSE 'Mixed'
    END as service_pattern
FROM calendar
ORDER BY service_id;

-- ============================================================================
-- ROUTE PLANNING QUERIES
-- ============================================================================

-- 12. Find direct routes between two stops
SELECT * FROM find_routes_between_stops(
    'stop_A',  -- from (replace with actual)
    'stop_B'   -- to (replace with actual)
)
LIMIT 10;

-- 13. Find transfer opportunities at a stop
SELECT 
    t.from_stop_id,
    t.to_stop_id,
    s1.stop_name as from_stop_name,
    s2.stop_name as to_stop_name,
    t.transfer_type,
    t.min_transfer_time,
    ROUND(
        ST_Distance(s1.geom::geography, s2.geom::geography)::NUMERIC, 2
    ) as walking_distance_meters
FROM transfers t
JOIN stops s1 ON t.from_stop_id = s1.stop_id
JOIN stops s2 ON t.to_stop_id = s2.stop_id
WHERE t.from_stop_id = 'stop_123'  -- Replace with actual stop_id
ORDER BY walking_distance_meters;

-- 14. Find trips with specific stop sequence
-- (e.g., trips that go from Stop A to Stop B to Stop C in order)
WITH stop_sequences AS (
    SELECT 
        trip_id,
        stop_id,
        stop_sequence
    FROM stop_times
    WHERE stop_id IN ('stop_A', 'stop_B', 'stop_C')
)
SELECT 
    t.trip_id,
    r.route_short_name,
    t.trip_headsign,
    array_agg(ss.stop_id ORDER BY ss.stop_sequence) as stop_sequence
FROM trips t
JOIN routes r ON t.route_id = r.route_id
JOIN stop_sequences ss ON t.trip_id = ss.trip_id
GROUP BY t.trip_id, r.route_short_name, t.trip_headsign
HAVING COUNT(*) = 3  -- All three stops present
AND array_agg(ss.stop_sequence ORDER BY ss.stop_sequence) = 
    ARRAY[(SELECT stop_sequence FROM stop_sequences WHERE trip_id = t.trip_id AND stop_id = 'stop_A'),
          (SELECT stop_sequence FROM stop_sequences WHERE trip_id = t.trip_id AND stop_id = 'stop_B'),
          (SELECT stop_sequence FROM stop_sequences WHERE trip_id = t.trip_id AND stop_id = 'stop_C')];

-- ============================================================================
-- STATISTICS & ANALYSIS
-- ============================================================================

-- 15. Route frequency analysis
SELECT 
    r.route_short_name,
    r.route_long_name,
    COUNT(DISTINCT t.trip_id) as total_trips,
    COUNT(DISTINCT t.service_id) as service_patterns,
    COUNT(DISTINCT st.stop_id) as unique_stops
FROM routes r
JOIN trips t ON r.route_id = t.route_id
JOIN stop_times st ON t.trip_id = st.trip_id
GROUP BY r.route_id, r.route_short_name, r.route_long_name
ORDER BY total_trips DESC
LIMIT 20;

-- 16. Busiest stops (by number of departures)
SELECT 
    s.stop_id,
    s.stop_name,
    s.location_type,
    COUNT(DISTINCT st.trip_id) as trip_count,
    COUNT(*) as total_stop_times
FROM stops s
JOIN stop_times st ON s.stop_id = st.stop_id
GROUP BY s.stop_id, s.stop_name, s.location_type
ORDER BY trip_count DESC
LIMIT 20;

-- 17. Average trip duration by route
SELECT 
    r.route_short_name,
    r.route_long_name,
    COUNT(DISTINCT t.trip_id) as trip_count,
    ROUND(AVG(get_trip_duration(t.trip_id))::NUMERIC / 60, 2) as avg_duration_minutes,
    MIN(get_trip_duration(t.trip_id)) / 60 as min_duration_minutes,
    MAX(get_trip_duration(t.trip_id)) / 60 as max_duration_minutes
FROM routes r
JOIN trips t ON r.route_id = t.route_id
GROUP BY r.route_id, r.route_short_name, r.route_long_name
HAVING COUNT(DISTINCT t.trip_id) > 0
ORDER BY avg_duration_minutes DESC
LIMIT 20;

-- 18. Coverage area statistics
SELECT 
    COUNT(*) as total_stops,
    ROUND(MIN(stop_lat)::NUMERIC, 4) as min_lat,
    ROUND(MAX(stop_lat)::NUMERIC, 4) as max_lat,
    ROUND(MIN(stop_lon)::NUMERIC, 4) as min_lon,
    ROUND(MAX(stop_lon)::NUMERIC, 4) as max_lon,
    ST_AsText(ST_Envelope(ST_Collect(geom))) as bounding_box
FROM stops
WHERE geom IS NOT NULL;

-- ============================================================================
-- FARE ANALYSIS
-- ============================================================================

-- 19. Fare summary by currency
SELECT 
    currency_type,
    COUNT(*) as fare_count,
    MIN(price) as min_price,
    ROUND(AVG(price)::NUMERIC, 2) as avg_price,
    MAX(price) as max_price
FROM fare_attributes
GROUP BY currency_type
ORDER BY currency_type;

-- 20. Routes with fare information
SELECT 
    r.route_short_name,
    r.route_long_name,
    fa.price,
    fa.currency_type,
    fa.payment_method,
    fa.transfers
FROM routes r
JOIN fare_rules fr ON r.route_id = fr.route_id
JOIN fare_attributes fa ON fr.fare_id = fa.fare_id
ORDER BY r.route_short_name;

-- ============================================================================
-- DATA QUALITY CHECKS
-- ============================================================================

-- 21. Stops without geometry
SELECT 
    stop_id,
    stop_name,
    stop_lat,
    stop_lon,
    geom
FROM stops
WHERE geom IS NULL
AND stop_lat IS NOT NULL
AND stop_lon IS NOT NULL
LIMIT 20;

-- 22. Trips without stop_times
SELECT 
    t.trip_id,
    r.route_short_name,
    t.trip_headsign
FROM trips t
JOIN routes r ON t.route_id = r.route_id
LEFT JOIN stop_times st ON t.trip_id = st.trip_id
WHERE st.trip_id IS NULL;

-- 23. Stop_times with invalid times
SELECT 
    trip_id,
    stop_id,
    stop_sequence,
    arrival_time,
    departure_time
FROM stop_times
WHERE arrival_time IS NOT NULL
AND departure_time IS NOT NULL
AND arrival_time > departure_time
LIMIT 20;

-- 24. Orphaned records
-- Routes without trips
SELECT r.route_id, r.route_short_name
FROM routes r
LEFT JOIN trips t ON r.route_id = t.route_id
WHERE t.trip_id IS NULL;

-- 25. Duplicate stop names (potential data quality issues)
SELECT 
    stop_name,
    COUNT(*) as count,
    array_agg(stop_id) as stop_ids
FROM stops
GROUP BY stop_name
HAVING COUNT(*) > 5  -- Stops with same name
ORDER BY count DESC
LIMIT 20;

-- ============================================================================
-- EXPORT QUERIES
-- ============================================================================

-- 26. Export route as GeoJSON LineString
SELECT 
    shape_id,
    ST_AsGeoJSON(ST_MakeLine(geom ORDER BY shape_pt_sequence)) as geojson
FROM shapes
WHERE shape_id = 'shape_123'  -- Replace with actual shape_id
GROUP BY shape_id;

-- 27. Export stops as GeoJSON FeatureCollection
SELECT json_build_object(
    'type', 'FeatureCollection',
    'features', json_agg(
        json_build_object(
            'type', 'Feature',
            'id', stop_id,
            'geometry', ST_AsGeoJSON(geom)::json,
            'properties', json_build_object(
                'stop_name', stop_name,
                'location_type', location_type
            )
        )
    )
) as geojson
FROM stops
WHERE geom IS NOT NULL
LIMIT 100;

-- ============================================================================
-- PERFORMANCE OPTIMIZATION
-- ============================================================================

-- 28. Analyze query performance
EXPLAIN ANALYZE
SELECT * FROM get_next_departures('stop_123', 36000, NULL, 10);

-- 29. Index usage statistics
SELECT 
    schemaname,
    tablename,
    indexname,
    idx_scan,
    idx_tup_read,
    idx_tup_fetch
FROM pg_stat_user_indexes
WHERE schemaname = 'public'
ORDER BY idx_scan DESC;

-- 30. Table sizes
SELECT 
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

-- ============================================================================
-- Remember to replace placeholder values like 'stop_123', 'trip_123', etc.
-- with actual IDs from your database!
-- ============================================================================
