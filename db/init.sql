-- ============================================================================
-- GTFS Transit Database Schema
-- Global-ready PostgreSQL + PostGIS implementation
-- ============================================================================

-- Enable PostGIS extension
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;

-- Set timezone to UTC for consistency (individual agencies have their own TZ)
SET timezone = 'UTC';

-- ============================================================================
-- TABLE: agency
-- Transit agencies with timezone and contact info
-- ============================================================================
CREATE TABLE IF NOT EXISTS agency (
    internal_id BIGSERIAL,
    agency_id TEXT PRIMARY KEY,
    agency_name TEXT NOT NULL,
    agency_url TEXT NOT NULL,
    agency_timezone TEXT NOT NULL,  -- IANA timezone (e.g., Europe/Istanbul)
    agency_lang TEXT,               -- ISO 639-1 language code
    agency_phone TEXT,
    agency_fare_url TEXT,
    agency_email TEXT,
    
    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE agency IS 'Transit agencies operating in the feed';
COMMENT ON COLUMN agency.agency_timezone IS 'IANA timezone identifier for agency operations';

-- ============================================================================
-- TABLE: stops
-- Stations, stops, entrances with PostGIS geometry
-- ============================================================================
CREATE TABLE IF NOT EXISTS stops (
    internal_id BIGSERIAL,
    stop_id TEXT PRIMARY KEY,
    stop_code TEXT,
    stop_name TEXT NOT NULL,
    stop_desc TEXT,
    
    -- Geographic coordinates
    stop_lat NUMERIC(10, 8),        -- Decimal degrees
    stop_lon NUMERIC(11, 8),        -- Decimal degrees
    geom GEOMETRY(Point, 4326),     -- PostGIS point geometry (WGS84)
    
    -- Hierarchy and zones
    zone_id TEXT,
    stop_url TEXT,
    location_type INTEGER DEFAULT 0, -- 0=stop, 1=station, 2=entrance, 3=node, 4=boarding
    parent_station TEXT,             -- Foreign key to parent stop
    stop_timezone TEXT,              -- Override agency timezone if needed
    wheelchair_boarding INTEGER,     -- 0=no info, 1=accessible, 2=not accessible
    
    -- Platform info
    platform_code TEXT,
    level_id TEXT,
    
    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    CONSTRAINT fk_parent_station FOREIGN KEY (parent_station) 
        REFERENCES stops(stop_id) ON DELETE CASCADE
);

COMMENT ON TABLE stops IS 'Stop locations with PostGIS geometry for spatial queries';
COMMENT ON COLUMN stops.geom IS 'Spatial point geometry (SRID 4326 - WGS84)';
COMMENT ON COLUMN stops.location_type IS '0=stop/platform, 1=station, 2=entrance, 3=generic node, 4=boarding area';

-- ============================================================================
-- TABLE: routes
-- Transit routes (lines)
-- ============================================================================
CREATE TABLE IF NOT EXISTS routes (
    internal_id BIGSERIAL,
    route_id TEXT PRIMARY KEY,
    agency_id TEXT NOT NULL,
    route_short_name TEXT,
    route_long_name TEXT,
    route_desc TEXT,
    route_type INTEGER NOT NULL,    -- GTFS route type (0-7, 100-1700)
    route_url TEXT,
    route_color TEXT DEFAULT 'FFFFFF',
    route_text_color TEXT DEFAULT '000000',
    route_sort_order INTEGER,
    
    -- Extended types
    continuous_pickup INTEGER,
    continuous_drop_off INTEGER,
    
    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    CONSTRAINT fk_agency FOREIGN KEY (agency_id) 
        REFERENCES agency(agency_id) ON DELETE CASCADE
);

COMMENT ON TABLE routes IS 'Transit routes/lines operated by agencies';
COMMENT ON COLUMN routes.route_type IS 'GTFS route type: 0=tram, 1=subway, 2=rail, 3=bus, 4=ferry, 7=funicular, etc.';

-- ============================================================================
-- TABLE: calendar
-- Service schedules (which days services run)
-- ============================================================================
CREATE TABLE IF NOT EXISTS calendar (
    internal_id BIGSERIAL,
    service_id TEXT PRIMARY KEY,
    monday INTEGER NOT NULL DEFAULT 0,
    tuesday INTEGER NOT NULL DEFAULT 0,
    wednesday INTEGER NOT NULL DEFAULT 0,
    thursday INTEGER NOT NULL DEFAULT 0,
    friday INTEGER NOT NULL DEFAULT 0,
    saturday INTEGER NOT NULL DEFAULT 0,
    sunday INTEGER NOT NULL DEFAULT 0,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    
    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    CONSTRAINT valid_date_range CHECK (end_date >= start_date)
);

COMMENT ON TABLE calendar IS 'Regular service schedules';

-- ============================================================================
-- TABLE: calendar_dates
-- Service exceptions (holidays, special events)
-- ============================================================================
CREATE TABLE IF NOT EXISTS calendar_dates (
    internal_id BIGSERIAL,
    service_id TEXT NOT NULL,
    date DATE NOT NULL,
    exception_type INTEGER NOT NULL, -- 1=service added, 2=service removed
    
    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    PRIMARY KEY (service_id, date),
    CONSTRAINT fk_service FOREIGN KEY (service_id) 
        REFERENCES calendar(service_id) ON DELETE CASCADE
);

COMMENT ON TABLE calendar_dates IS 'Service exceptions (holidays, special events)';
COMMENT ON COLUMN calendar_dates.exception_type IS '1=service added on this date, 2=service removed';

-- ============================================================================
-- TABLE: trips
-- Individual vehicle journeys
-- ============================================================================
CREATE TABLE IF NOT EXISTS trips (
    internal_id BIGSERIAL,
    trip_id TEXT PRIMARY KEY,
    route_id TEXT NOT NULL,
    service_id TEXT NOT NULL,
    trip_headsign TEXT,
    trip_short_name TEXT,
    direction_id INTEGER,           -- 0=outbound, 1=inbound
    block_id TEXT,
    shape_id TEXT,
    wheelchair_accessible INTEGER,  -- 0=no info, 1=yes, 2=no
    bikes_allowed INTEGER,          -- 0=no info, 1=yes, 2=no
    
    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    CONSTRAINT fk_route FOREIGN KEY (route_id) 
        REFERENCES routes(route_id) ON DELETE CASCADE,
    CONSTRAINT fk_service FOREIGN KEY (service_id) 
        REFERENCES calendar(service_id) ON DELETE CASCADE
);

COMMENT ON TABLE trips IS 'Individual vehicle trips/journeys';
COMMENT ON COLUMN trips.direction_id IS '0=outbound/one direction, 1=inbound/opposite direction';

-- ============================================================================
-- TABLE: stop_times
-- Trip schedules (when trips arrive at each stop)
-- CRITICAL: Uses INTEGER for times to support post-midnight (25:00:00+)
-- ============================================================================
CREATE TABLE IF NOT EXISTS stop_times (
    internal_id BIGSERIAL,
    trip_id TEXT NOT NULL,
    stop_id TEXT NOT NULL,
    stop_sequence INTEGER NOT NULL,
    
    -- Time as seconds since midnight (supports 24:00:00+ for overnight service)
    arrival_time INTEGER,           -- Seconds since midnight (can exceed 86400)
    departure_time INTEGER,         -- Seconds since midnight (can exceed 86400)
    
    -- Display times (stored for compatibility, but use INTEGER for queries)
    arrival_time_str TEXT,          -- HH:MM:SS format (may be 25:30:00)
    departure_time_str TEXT,        -- HH:MM:SS format
    
    stop_headsign TEXT,
    pickup_type INTEGER DEFAULT 0,
    drop_off_type INTEGER DEFAULT 0,
    continuous_pickup INTEGER,
    continuous_drop_off INTEGER,
    shape_dist_traveled NUMERIC,
    timepoint INTEGER,
    
    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),
    
    PRIMARY KEY (trip_id, stop_sequence),
    CONSTRAINT fk_trip FOREIGN KEY (trip_id) 
        REFERENCES trips(trip_id) ON DELETE CASCADE,
    CONSTRAINT fk_stop FOREIGN KEY (stop_id) 
        REFERENCES stops(stop_id) ON DELETE CASCADE
);

COMMENT ON TABLE stop_times IS 'Trip schedules - when vehicles arrive/depart stops';
COMMENT ON COLUMN stop_times.arrival_time IS 'Seconds since midnight (can exceed 86400 for post-midnight service)';
COMMENT ON COLUMN stop_times.departure_time IS 'Seconds since midnight (supports 25:00:00+ overnight times)';

-- ============================================================================
-- TABLE: shapes
-- Route path geometries
-- ============================================================================
CREATE TABLE IF NOT EXISTS shapes (
    internal_id BIGSERIAL PRIMARY KEY,
    shape_id TEXT NOT NULL,
    shape_pt_lat NUMERIC(10, 8) NOT NULL,
    shape_pt_lon NUMERIC(11, 8) NOT NULL,
    shape_pt_sequence INTEGER NOT NULL,
    shape_dist_traveled NUMERIC,
    
    -- PostGIS geometry
    geom GEOMETRY(Point, 4326),
    
    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),
    
    UNIQUE (shape_id, shape_pt_sequence)
);

COMMENT ON TABLE shapes IS 'Geographic path shapes for routes';

-- ============================================================================
-- TABLE: transfers
-- Transfer rules between stops
-- ============================================================================
CREATE TABLE IF NOT EXISTS transfers (
    internal_id BIGSERIAL PRIMARY KEY,
    from_stop_id TEXT NOT NULL,
    to_stop_id TEXT NOT NULL,
    transfer_type INTEGER NOT NULL DEFAULT 0, -- 0=recommended, 1=timed, 2=min time, 3=not possible
    min_transfer_time INTEGER,                -- Seconds
    
    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    UNIQUE (from_stop_id, to_stop_id),
    CONSTRAINT fk_from_stop FOREIGN KEY (from_stop_id) 
        REFERENCES stops(stop_id) ON DELETE CASCADE,
    CONSTRAINT fk_to_stop FOREIGN KEY (to_stop_id) 
        REFERENCES stops(stop_id) ON DELETE CASCADE
);

COMMENT ON TABLE transfers IS 'Transfer rules between stops';
COMMENT ON COLUMN transfers.transfer_type IS '0=recommended, 1=timed transfer, 2=min time required, 3=not possible';

-- ============================================================================
-- TABLE: fare_attributes
-- Fare information
-- ============================================================================
CREATE TABLE IF NOT EXISTS fare_attributes (
    internal_id BIGSERIAL,
    fare_id TEXT PRIMARY KEY,
    price NUMERIC(10, 2) NOT NULL,
    currency_type TEXT NOT NULL,    -- ISO 4217 currency code
    payment_method INTEGER NOT NULL, -- 0=on board, 1=before boarding
    transfers INTEGER,              -- Number of transfers allowed (null=unlimited)
    transfer_duration INTEGER,      -- Transfer window in seconds
    agency_id TEXT,
    
    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    CONSTRAINT fk_fare_agency FOREIGN KEY (agency_id) 
        REFERENCES agency(agency_id) ON DELETE SET NULL
);

COMMENT ON TABLE fare_attributes IS 'Fare prices and payment rules';
COMMENT ON COLUMN fare_attributes.currency_type IS 'ISO 4217 currency code (e.g., TRY, USD, EUR)';

-- ============================================================================
-- TABLE: fare_rules
-- Fare rules for routes/zones
-- ============================================================================
CREATE TABLE IF NOT EXISTS fare_rules (
    internal_id BIGSERIAL PRIMARY KEY,
    fare_id TEXT NOT NULL,
    route_id TEXT,
    origin_id TEXT,
    destination_id TEXT,
    contains_id TEXT,
    
    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),
    
    CONSTRAINT fk_fare FOREIGN KEY (fare_id) 
        REFERENCES fare_attributes(fare_id) ON DELETE CASCADE,
    CONSTRAINT fk_fare_route FOREIGN KEY (route_id) 
        REFERENCES routes(route_id) ON DELETE CASCADE
);

COMMENT ON TABLE fare_rules IS 'Rules for applying fares to routes and zones';

-- ============================================================================
-- TABLE: feed_info
-- Feed metadata
-- ============================================================================
CREATE TABLE IF NOT EXISTS feed_info (
    internal_id BIGSERIAL PRIMARY KEY,
    feed_publisher_name TEXT NOT NULL,
    feed_publisher_url TEXT NOT NULL,
    feed_lang TEXT NOT NULL,
    default_lang TEXT,
    feed_start_date DATE,
    feed_end_date DATE,
    feed_version TEXT,
    feed_contact_email TEXT,
    feed_contact_url TEXT,
    feed_id TEXT UNIQUE,
    feed_license TEXT,
    
    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE feed_info IS 'GTFS feed metadata';

-- ============================================================================
-- TABLE: frequencies (optional GTFS file)
-- Headway-based service
-- ============================================================================
CREATE TABLE IF NOT EXISTS frequencies (
    internal_id BIGSERIAL PRIMARY KEY,
    trip_id TEXT NOT NULL,
    start_time INTEGER NOT NULL,   -- Seconds since midnight
    end_time INTEGER NOT NULL,     -- Seconds since midnight
    headway_secs INTEGER NOT NULL,
    exact_times INTEGER DEFAULT 0,
    
    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),
    
    CONSTRAINT fk_freq_trip FOREIGN KEY (trip_id) 
        REFERENCES trips(trip_id) ON DELETE CASCADE
);

COMMENT ON TABLE frequencies IS 'Headway-based service (frequency schedules)';

-- ============================================================================
-- Success message
-- ============================================================================
DO $$
BEGIN
    RAISE NOTICE '✓ GTFS schema initialized successfully';
    RAISE NOTICE '✓ PostGIS extension enabled';
    RAISE NOTICE '✓ Ready for global multi-timezone data';
END $$;
