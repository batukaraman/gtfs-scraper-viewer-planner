-- ============================================================================
-- GTFS Database Triggers
-- Automatic geometry updates and data validation
-- ============================================================================

-- ============================================================================
-- TRIGGER: Auto-update geom field for stops
-- Automatically creates/updates PostGIS geometry from lat/lon
-- ============================================================================

CREATE OR REPLACE FUNCTION update_stop_geom()
RETURNS TRIGGER AS $$
BEGIN
    -- Only update if lat/lon are provided and valid
    IF NEW.stop_lat IS NOT NULL AND NEW.stop_lon IS NOT NULL THEN
        -- Validate coordinates are within valid ranges
        IF NEW.stop_lat BETWEEN -90 AND 90 AND NEW.stop_lon BETWEEN -180 AND 180 THEN
            NEW.geom := ST_SetSRID(ST_MakePoint(NEW.stop_lon, NEW.stop_lat), 4326);
        ELSE
            RAISE WARNING 'Invalid coordinates for stop %: lat=%, lon=%', 
                NEW.stop_id, NEW.stop_lat, NEW.stop_lon;
            NEW.geom := NULL;
        END IF;
    ELSE
        NEW.geom := NULL;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_stops_geom
    BEFORE INSERT OR UPDATE OF stop_lat, stop_lon
    ON stops
    FOR EACH ROW
    EXECUTE FUNCTION update_stop_geom();

COMMENT ON FUNCTION update_stop_geom() IS 'Automatically updates PostGIS geometry when stop coordinates change';

-- ============================================================================
-- TRIGGER: Auto-update geom field for shapes
-- Automatically creates PostGIS geometry from lat/lon
-- ============================================================================

CREATE OR REPLACE FUNCTION update_shape_geom()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.shape_pt_lat IS NOT NULL AND NEW.shape_pt_lon IS NOT NULL THEN
        IF NEW.shape_pt_lat BETWEEN -90 AND 90 AND NEW.shape_pt_lon BETWEEN -180 AND 180 THEN
            NEW.geom := ST_SetSRID(ST_MakePoint(NEW.shape_pt_lon, NEW.shape_pt_lat), 4326);
        ELSE
            RAISE WARNING 'Invalid shape coordinates: lat=%, lon=%', 
                NEW.shape_pt_lat, NEW.shape_pt_lon;
            NEW.geom := NULL;
        END IF;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_shapes_geom
    BEFORE INSERT OR UPDATE OF shape_pt_lat, shape_pt_lon
    ON shapes
    FOR EACH ROW
    EXECUTE FUNCTION update_shape_geom();

-- ============================================================================
-- TRIGGER: Auto-update updated_at timestamp
-- Updates the updated_at field whenever a row is modified
-- ============================================================================

CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at := NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply to all tables with updated_at column
CREATE TRIGGER trg_agency_updated_at
    BEFORE UPDATE ON agency
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER trg_stops_updated_at
    BEFORE UPDATE ON stops
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER trg_routes_updated_at
    BEFORE UPDATE ON routes
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER trg_calendar_updated_at
    BEFORE UPDATE ON calendar
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER trg_calendar_dates_updated_at
    BEFORE UPDATE ON calendar_dates
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER trg_trips_updated_at
    BEFORE UPDATE ON trips
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER trg_transfers_updated_at
    BEFORE UPDATE ON transfers
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER trg_fare_attributes_updated_at
    BEFORE UPDATE ON fare_attributes
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER trg_feed_info_updated_at
    BEFORE UPDATE ON feed_info
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at();

-- ============================================================================
-- TRIGGER: Validate time formats
-- Ensures arrival/departure times are in valid ranges
-- ============================================================================

CREATE OR REPLACE FUNCTION validate_stop_times()
RETURNS TRIGGER AS $$
BEGIN
    -- Validate arrival_time (seconds since midnight, can be > 86400)
    IF NEW.arrival_time IS NOT NULL AND NEW.arrival_time < 0 THEN
        RAISE EXCEPTION 'Invalid arrival_time for trip %: % (cannot be negative)', 
            NEW.trip_id, NEW.arrival_time;
    END IF;
    
    -- Validate departure_time
    IF NEW.departure_time IS NOT NULL AND NEW.departure_time < 0 THEN
        RAISE EXCEPTION 'Invalid departure_time for trip %: % (cannot be negative)', 
            NEW.trip_id, NEW.departure_time;
    END IF;
    
    -- Ensure arrival <= departure at same stop
    IF NEW.arrival_time IS NOT NULL AND NEW.departure_time IS NOT NULL THEN
        IF NEW.arrival_time > NEW.departure_time THEN
            RAISE WARNING 'Arrival time after departure time for trip % at stop %', 
                NEW.trip_id, NEW.stop_id;
        END IF;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_validate_stop_times
    BEFORE INSERT OR UPDATE ON stop_times
    FOR EACH ROW
    EXECUTE FUNCTION validate_stop_times();

-- ============================================================================
-- TRIGGER: Validate parent station references
-- Ensures location_type hierarchy is correct
-- ============================================================================

CREATE OR REPLACE FUNCTION validate_parent_station()
RETURNS TRIGGER AS $$
DECLARE
    parent_location_type INTEGER;
BEGIN
    -- If parent_station is set, validate it exists and has correct type
    IF NEW.parent_station IS NOT NULL THEN
        SELECT location_type INTO parent_location_type
        FROM stops
        WHERE stop_id = NEW.parent_station;
        
        IF NOT FOUND THEN
            RAISE EXCEPTION 'Parent station % does not exist for stop %', 
                NEW.parent_station, NEW.stop_id;
        END IF;
        
        -- Parent must be a station (location_type = 1)
        IF parent_location_type != 1 THEN
            RAISE WARNING 'Parent station % for stop % is not a station (location_type should be 1, got %)', 
                NEW.parent_station, NEW.stop_id, parent_location_type;
        END IF;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_validate_parent_station
    BEFORE INSERT OR UPDATE OF parent_station
    ON stops
    FOR EACH ROW
    EXECUTE FUNCTION validate_parent_station();

-- ============================================================================
-- Success message
-- ============================================================================
DO $$
BEGIN
    RAISE NOTICE '✓ All triggers created successfully';
    RAISE NOTICE '✓ Automatic geometry updates enabled';
    RAISE NOTICE '✓ Data validation triggers active';
END $$;
