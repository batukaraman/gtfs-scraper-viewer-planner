-- ============================================================================
-- GTFS Database Indexes
-- Performance optimization for common queries
-- ============================================================================

-- ============================================================================
-- AGENCY INDEXES
-- ============================================================================
CREATE INDEX IF NOT EXISTS idx_agency_timezone ON agency(agency_timezone);
CREATE INDEX IF NOT EXISTS idx_agency_name ON agency(agency_name);

-- ============================================================================
-- STOPS INDEXES
-- ============================================================================

-- Spatial index (CRITICAL for geospatial queries)
CREATE INDEX IF NOT EXISTS idx_stops_geom ON stops USING GIST(geom);

-- Location queries
CREATE INDEX IF NOT EXISTS idx_stops_location ON stops(stop_lat, stop_lon);

-- Hierarchy queries
CREATE INDEX IF NOT EXISTS idx_stops_parent ON stops(parent_station) WHERE parent_station IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_stops_location_type ON stops(location_type);

-- Search queries
CREATE INDEX IF NOT EXISTS idx_stops_name ON stops(stop_name);
CREATE INDEX IF NOT EXISTS idx_stops_code ON stops(stop_code) WHERE stop_code IS NOT NULL;

-- Zone queries
CREATE INDEX IF NOT EXISTS idx_stops_zone ON stops(zone_id) WHERE zone_id IS NOT NULL;

-- Full-text search (for name searches) - requires pg_trgm extension
-- Handled gracefully at the end of this file

-- ============================================================================
-- ROUTES INDEXES
-- ============================================================================
CREATE INDEX IF NOT EXISTS idx_routes_agency ON routes(agency_id);
CREATE INDEX IF NOT EXISTS idx_routes_type ON routes(route_type);
CREATE INDEX IF NOT EXISTS idx_routes_short_name ON routes(route_short_name);
CREATE INDEX IF NOT EXISTS idx_routes_sort ON routes(route_sort_order) WHERE route_sort_order IS NOT NULL;

-- ============================================================================
-- CALENDAR INDEXES
-- ============================================================================
CREATE INDEX IF NOT EXISTS idx_calendar_dates ON calendar(start_date, end_date);
CREATE INDEX IF NOT EXISTS idx_calendar_service ON calendar(service_id);

-- For finding active services on a specific date
CREATE INDEX IF NOT EXISTS idx_calendar_active ON calendar(start_date, end_date) 
    WHERE (monday + tuesday + wednesday + thursday + friday + saturday + sunday) > 0;

-- ============================================================================
-- CALENDAR_DATES INDEXES
-- ============================================================================
CREATE INDEX IF NOT EXISTS idx_calendar_dates_service ON calendar_dates(service_id);
CREATE INDEX IF NOT EXISTS idx_calendar_dates_date ON calendar_dates(date);
CREATE INDEX IF NOT EXISTS idx_calendar_dates_exception ON calendar_dates(exception_type);

-- Composite for date range queries
CREATE INDEX IF NOT EXISTS idx_calendar_dates_service_date ON calendar_dates(service_id, date);

-- ============================================================================
-- TRIPS INDEXES
-- ============================================================================
CREATE INDEX IF NOT EXISTS idx_trips_route ON trips(route_id);
CREATE INDEX IF NOT EXISTS idx_trips_service ON trips(service_id);
CREATE INDEX IF NOT EXISTS idx_trips_shape ON trips(shape_id) WHERE shape_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_trips_direction ON trips(route_id, direction_id);
CREATE INDEX IF NOT EXISTS idx_trips_block ON trips(block_id) WHERE block_id IS NOT NULL;

-- Composite for common route+service queries
CREATE INDEX IF NOT EXISTS idx_trips_route_service ON trips(route_id, service_id);

-- ============================================================================
-- STOP_TIMES INDEXES (CRITICAL - largest table)
-- ============================================================================

-- Primary query patterns
CREATE INDEX IF NOT EXISTS idx_stop_times_trip ON stop_times(trip_id);
CREATE INDEX IF NOT EXISTS idx_stop_times_stop ON stop_times(stop_id);
CREATE INDEX IF NOT EXISTS idx_stop_times_sequence ON stop_times(trip_id, stop_sequence);

-- Time-based queries (for schedule lookups)
CREATE INDEX IF NOT EXISTS idx_stop_times_arrival ON stop_times(stop_id, arrival_time) WHERE arrival_time IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_stop_times_departure ON stop_times(stop_id, departure_time) WHERE departure_time IS NOT NULL;

-- Composite for finding next departures at a stop
CREATE INDEX IF NOT EXISTS idx_stop_times_stop_departure ON stop_times(stop_id, departure_time, trip_id);

-- For trip planning queries (finding stops on a trip in order)
CREATE INDEX IF NOT EXISTS idx_stop_times_trip_seq ON stop_times(trip_id, stop_sequence, stop_id);

-- ============================================================================
-- SHAPES INDEXES
-- ============================================================================
CREATE INDEX IF NOT EXISTS idx_shapes_id ON shapes(shape_id);
CREATE INDEX IF NOT EXISTS idx_shapes_sequence ON shapes(shape_id, shape_pt_sequence);

-- Spatial index for shape points
CREATE INDEX IF NOT EXISTS idx_shapes_geom ON shapes USING GIST(geom);

-- ============================================================================
-- TRANSFERS INDEXES
-- ============================================================================
CREATE INDEX IF NOT EXISTS idx_transfers_from ON transfers(from_stop_id);
CREATE INDEX IF NOT EXISTS idx_transfers_to ON transfers(to_stop_id);
CREATE INDEX IF NOT EXISTS idx_transfers_type ON transfers(transfer_type);

-- Bidirectional transfer lookups
CREATE INDEX IF NOT EXISTS idx_transfers_both ON transfers(from_stop_id, to_stop_id);

-- ============================================================================
-- FARE_ATTRIBUTES INDEXES
-- ============================================================================
CREATE INDEX IF NOT EXISTS idx_fare_currency ON fare_attributes(currency_type);
CREATE INDEX IF NOT EXISTS idx_fare_agency ON fare_attributes(agency_id) WHERE agency_id IS NOT NULL;

-- ============================================================================
-- FARE_RULES INDEXES
-- ============================================================================
CREATE INDEX IF NOT EXISTS idx_fare_rules_fare ON fare_rules(fare_id);
CREATE INDEX IF NOT EXISTS idx_fare_rules_route ON fare_rules(route_id) WHERE route_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_fare_rules_origin ON fare_rules(origin_id) WHERE origin_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_fare_rules_destination ON fare_rules(destination_id) WHERE destination_id IS NOT NULL;

-- ============================================================================
-- FREQUENCIES INDEXES
-- ============================================================================
CREATE INDEX IF NOT EXISTS idx_frequencies_trip ON frequencies(trip_id);
CREATE INDEX IF NOT EXISTS idx_frequencies_time ON frequencies(start_time, end_time);

-- ============================================================================
-- Enable pg_trgm for fuzzy text search (install if needed)
-- ============================================================================
DO $$
BEGIN
    -- Try to enable pg_trgm extension
    CREATE EXTENSION IF NOT EXISTS pg_trgm;
    
    -- Create fuzzy search index if extension is available
    CREATE INDEX IF NOT EXISTS idx_stops_name_trgm 
        ON stops USING gin(stop_name gin_trgm_ops);
    
    RAISE NOTICE '✓ pg_trgm extension enabled with fuzzy search index';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE '⚠ pg_trgm extension not available (fuzzy search disabled)';
    RAISE NOTICE '  To enable: CREATE EXTENSION pg_trgm;';
END $$;

-- ============================================================================
-- Analyze tables for query planner
-- ============================================================================
ANALYZE agency;
ANALYZE stops;
ANALYZE routes;
ANALYZE calendar;
ANALYZE calendar_dates;
ANALYZE trips;
ANALYZE stop_times;
ANALYZE shapes;
ANALYZE transfers;
ANALYZE fare_attributes;
ANALYZE fare_rules;
ANALYZE frequencies;

-- ============================================================================
-- Success message
-- ============================================================================
DO $$
BEGIN
    RAISE NOTICE '✓ All indexes created successfully';
    RAISE NOTICE '✓ Spatial indexes (GIST) ready for PostGIS queries';
    RAISE NOTICE '✓ Query performance optimized';
END $$;
