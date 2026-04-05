# GTFS Transit Database - PostgreSQL + PostGIS

Complete guide for the GTFS database system with PostGIS spatial extensions.

## Table of Contents

- [Features](#features)
- [Quick Start](#quick-start)
- [Architecture](#architecture)
- [Setup](#setup)
- [Usage](#usage)
- [Helper Functions](#helper-functions)
- [Example Queries](#example-queries)
- [Performance](#performance)
- [Troubleshooting](#troubleshooting)

---

## Features

✅ **PostGIS Spatial Support** - Efficient geographic queries for stops and shapes  
✅ **Global Timezone Support** - Multiple timezones (IANA identifiers)  
✅ **Post-Midnight Times** - Handles overnight service (25:00:00+)  
✅ **Upsert Support** - Re-run data loader without conflicts  
✅ **40+ Optimized Indexes** - Fast queries on all tables  
✅ **Automatic Geometry** - Triggers auto-populate PostGIS fields  
✅ **10+ Helper Functions** - Common GTFS operations built-in  
✅ **Multi-Agency** - Single database for multiple transit operators  

---

## Quick Start

### 1. Start Database

```bash
# From project root
docker-compose up -d

# Check status (wait for "healthy")
docker-compose ps
```

### 2. Test Connection

```bash
python -m database test
```

### 3. Load GTFS Data

```bash
# Ensure GTFS files are in gtfs/ directory
python -m database load
```

That's it! Database is ready.

---

## Architecture

### Database Schema

```
agency              - Transit operators (İETT, Metro, etc.)
├── routes          - Transit lines (bus, metro, ferry)
│   └── trips       - Vehicle journeys
│       └── stop_times - Schedules (INTEGER seconds format)
│
stops               - Stop locations + PostGIS geometry
├── transfers       - Transfer rules between stops
│
calendar            - Service schedules (weekday patterns)
└── calendar_dates  - Service exceptions (holidays)

shapes              - Route path geometries (PostGIS LineString)
fare_attributes     - Fare prices (multi-currency)
fare_rules          - Fare application rules
```

### Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| **INTEGER for times** | Supports post-midnight (25:00:00+) |
| **GEOMETRY(Point, 4326)** | WGS84 for global compatibility |
| **Natural + Internal PKs** | GTFS keys + BIGSERIAL |
| **GIST indexes** | Spatial query optimization |
| **UTF-8 encoding** | Global language support |

### Tables Overview

| Table | Primary Key | Purpose |
|-------|-------------|---------|
| `agency` | agency_id | Transit operators |
| `stops` | stop_id | Stop locations + geometry |
| `routes` | route_id | Transit routes/lines |
| `trips` | trip_id | Individual vehicle trips |
| `stop_times` | (trip_id, stop_sequence) | Arrival/departure times |
| `calendar` | service_id | Weekly service patterns |
| `calendar_dates` | (service_id, date) | Special days/holidays |
| `shapes` | (shape_id, shape_pt_sequence) | Route geometries |
| `transfers` | (from_stop_id, to_stop_id) | Transfer rules |
| `fare_attributes` | fare_id | Fare prices |
| `fare_rules` | (fare_id, route_id, ...) | Fare application |

---

## Setup

### Prerequisites

- Docker Desktop (Windows/Mac) or Docker Engine (Linux)
- Python 3.8+
- 4GB+ RAM (8GB recommended)
- 2GB+ disk space

### Automated Setup

**Windows (PowerShell):**

```powershell
cd scripts
.\setup.ps1
```

**Linux/Mac (Bash):**

```bash
cd scripts
chmod +x setup.sh
./setup.sh
```

### Manual Setup

#### 1. Configure Environment

```bash
# Copy example
cp .env.example .env

# Edit passwords
nano .env  # or notepad .env on Windows
```

**Important:** Change default passwords!

#### 2. Start Services

```bash
docker-compose up -d
```

Wait 30-60 seconds for initialization. Check logs:

```bash
docker-compose logs -f gtfs-postgres
```

Look for:
```
✓ GTFS schema initialized successfully
✓ PostGIS extension enabled
✓ All indexes created successfully
✓ All triggers created successfully
✓ All helper functions created successfully
```

#### 3. Verify Installation

```bash
python -m database test
```

Expected output:
```
✅ PostgreSQL: PostgreSQL 16.x
✅ PostGIS: 3.4.x
✅ Tables found: 13
✅ All tests passed! Database is ready.
```

---

## Usage

### Loading Data

```bash
# Load all GTFS files from gtfs/ directory
python -m database load

# Specify custom directory
python -m database load --gtfs-dir /path/to/gtfs

# Specify custom database URL
python -m database load --database-url postgresql://user:pass@host:5432/db
```

### Database Access

**psql (Command Line):**

```bash
psql -h localhost -U gtfs_admin -d gtfs_transit
```

**pgAdmin (Web UI):**

```
URL: http://localhost:5050
Email: admin@gtfs.local
Password: (from .env)

Add Server:
  Host: gtfs-postgres
  Port: 5432
  User: gtfs_admin
  Password: (from .env)
```

**Python (SQLAlchemy):**

```python
from sqlalchemy import create_engine
import pandas as pd

engine = create_engine('postgresql://gtfs_admin:password@localhost:5432/gtfs_transit')

# Query to DataFrame
df = pd.read_sql("SELECT * FROM stops LIMIT 10", engine)
```

---

## Helper Functions

### Time Conversion

```sql
-- String to seconds
SELECT gtfs_time_to_seconds('09:30:00');  -- Returns: 34200
SELECT gtfs_time_to_seconds('25:30:00');  -- Post-midnight: 91800

-- Seconds to string
SELECT seconds_to_gtfs_time(34200);  -- Returns: '09:30:00'
```

### Spatial Queries

```sql
-- Find stops within 500m
SELECT * FROM find_stops_nearby(41.0369, 28.9850, 500);

-- Arguments: (latitude, longitude, radius_meters)
```

### Service Queries

```sql
-- Check if service runs on a date
SELECT is_service_active('weekday_service', '2026-04-05');

-- Returns: true or false
```

### Next Departures

```sql
-- Get next 10 departures after 08:00
SELECT * FROM get_next_departures(
    'stop_123',    -- stop_id
    28800,         -- 08:00:00 in seconds
    NULL,          -- all services
    10             -- limit
);
```

### Route Analysis

```sql
-- Get route as LineString
SELECT get_route_shape('shape_123');

-- Calculate trip duration
SELECT get_trip_duration('trip_456');  -- Returns seconds

-- Get all stops for a trip
SELECT * FROM get_trip_stops('trip_789');

-- Find routes between two stops
SELECT * FROM find_routes_between_stops('stop_A', 'stop_B');
```

### Statistics

```sql
-- Database statistics
SELECT * FROM get_gtfs_stats();
```

---

## Example Queries

### Basic Queries

**List all agencies:**

```sql
SELECT agency_id, agency_name, agency_timezone
FROM agency
ORDER BY agency_name;
```

**Top 10 busiest stops:**

```sql
SELECT 
    s.stop_name,
    COUNT(*) as departures
FROM stop_times st
JOIN stops s ON st.stop_id = s.stop_id
GROUP BY s.stop_id, s.stop_name
ORDER BY departures DESC
LIMIT 10;
```

### Spatial Queries

**Stops in a bounding box:**

```sql
SELECT stop_id, stop_name, stop_lat, stop_lon
FROM stops
WHERE geom && ST_MakeEnvelope(28.95, 41.00, 29.05, 41.10, 4326)
ORDER BY stop_name;
```

**Distance between two stops:**

```sql
SELECT 
    ROUND(ST_Distance(
        s1.geom::geography,
        s2.geom::geography
    )::NUMERIC, 2) as distance_meters
FROM stops s1, stops s2
WHERE s1.stop_id = 'stop_A'
AND s2.stop_id = 'stop_B';
```

### Time-Based Queries

**Active trips right now:**

```sql
WITH current_time AS (
    SELECT EXTRACT(EPOCH FROM LOCALTIME)::INTEGER AS seconds
)
SELECT 
    r.route_short_name,
    t.trip_headsign,
    s.stop_name,
    seconds_to_gtfs_time(st.departure_time) AS departure
FROM stop_times st
JOIN trips t ON st.trip_id = t.trip_id
JOIN routes r ON t.route_id = r.route_id
JOIN stops s ON st.stop_id = s.stop_id
CROSS JOIN current_time
WHERE st.departure_time >= current_time.seconds
AND st.departure_time < current_time.seconds + 3600
ORDER BY st.departure_time
LIMIT 20;
```

**Services running on weekdays only:**

```sql
SELECT service_id, start_date, end_date
FROM calendar
WHERE monday=1 AND tuesday=1 AND wednesday=1 
  AND thursday=1 AND friday=1
  AND saturday=0 AND sunday=0;
```

### Route Planning

**Transfer opportunities at a stop:**

```sql
SELECT 
    s1.stop_name as from_stop,
    s2.stop_name as to_stop,
    t.transfer_type,
    t.min_transfer_time,
    ROUND(ST_Distance(
        s1.geom::geography,
        s2.geom::geography
    )::NUMERIC, 2) as walking_distance_m
FROM transfers t
JOIN stops s1 ON t.from_stop_id = s1.stop_id
JOIN stops s2 ON t.to_stop_id = s2.stop_id
WHERE t.from_stop_id = 'stop_123'
ORDER BY walking_distance_m;
```

See `db/examples.sql` for 30+ more queries!

---

## Performance

### Optimization Tips

**1. Regular Maintenance:**

```sql
-- After bulk updates
VACUUM ANALYZE;

-- Rebuild statistics
ANALYZE;

-- Rebuild indexes
REINDEX DATABASE gtfs_transit;
```

**2. Custom Indexes:**

```sql
-- For your specific query patterns
CREATE INDEX idx_custom ON stop_times(stop_id, departure_time)
WHERE pickup_type = 0;
```

**3. Query Optimization:**

```sql
-- Check query plan
EXPLAIN ANALYZE SELECT ...;

-- Look for Seq Scan (bad) vs Index Scan (good)
```

### Large Dataset Configuration

For 10M+ records in `stop_times`, edit `docker-compose.yml`:

```yaml
environment:
  POSTGRES_SHARED_BUFFERS: 2GB      # 25% of RAM
  POSTGRES_WORK_MEM: 128MB
  POSTGRES_MAINTENANCE_WORK_MEM: 512MB
  POSTGRES_EFFECTIVE_CACHE_SIZE: 6GB  # 50-75% of RAM
```

### Backup & Restore

**Backup:**

```bash
# Full backup
docker exec gtfs_db pg_dump -U gtfs_admin gtfs_transit > backup.sql

# Compressed
docker exec gtfs_db pg_dump -U gtfs_admin gtfs_transit | gzip > backup.sql.gz
```

**Restore:**

```bash
# From backup
docker exec -i gtfs_db psql -U gtfs_admin gtfs_transit < backup.sql

# From compressed
gunzip -c backup.sql.gz | docker exec -i gtfs_db psql -U gtfs_admin gtfs_transit
```

---

## Troubleshooting

### Database Won't Start

```bash
# Check logs
docker-compose logs gtfs-postgres

# Common issues:
# - Port 5432 already in use → change POSTGRES_PORT in .env
# - Insufficient memory → increase Docker memory limit
# - Corrupted volume → docker-compose down -v && docker-compose up -d
```

### Connection Refused

```bash
# Wait for health check
docker-compose ps

# Should show "(healthy)" status
# First startup takes 30-60 seconds

# Manual test
docker exec gtfs_db pg_isready -U gtfs_admin
```

### Data Load Fails

```bash
# Validate GTFS files first
# Use: https://gtfs-validator.mobilitydata.org/

# Check logs
python -m database load > load.log 2>&1

# Common issues:
# - Invalid CSV format
# - Missing required columns
# - Invalid coordinates (lat/lon out of range)
# - Foreign key violations (wrong load order)
```

### Slow Queries

```sql
-- Check missing indexes
SELECT tablename, attname, n_distinct, correlation
FROM pg_stats
WHERE schemaname = 'public'
AND correlation < 0.1;

-- Table sizes
SELECT 
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename))
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
```

### Out of Memory

```bash
# Reduce buffers in docker-compose.yml
# Or increase Docker memory (Docker Desktop → Settings → Resources)
```

---

## Advanced Topics

### Multi-City Support

```sql
-- Filter by city/agency
SELECT * FROM routes WHERE agency_id LIKE 'istanbul_%';
SELECT * FROM routes WHERE agency_id LIKE 'berlin_%';

-- Get stops by timezone
SELECT * FROM stops 
WHERE stop_timezone = 'Europe/Istanbul';
```

### GeoJSON Export

```sql
-- Export stops as GeoJSON
SELECT json_build_object(
    'type', 'FeatureCollection',
    'features', json_agg(
        json_build_object(
            'type', 'Feature',
            'id', stop_id,
            'geometry', ST_AsGeoJSON(geom)::json,
            'properties', json_build_object(
                'name', stop_name,
                'type', location_type
            )
        )
    )
) FROM stops WHERE geom IS NOT NULL LIMIT 100;
```

### Data Quality Checks

```sql
-- Stops without geometry
SELECT COUNT(*) FROM stops 
WHERE geom IS NULL AND stop_lat IS NOT NULL;

-- Trips without stop_times
SELECT COUNT(*) FROM trips t
LEFT JOIN stop_times st ON t.trip_id = st.trip_id
WHERE st.trip_id IS NULL;

-- Invalid time sequences
SELECT COUNT(*) FROM stop_times
WHERE arrival_time > departure_time;
```

---

## Files Reference

| File | Description |
|------|-------------|
| `db/init.sql` | Table schemas |
| `db/indexes.sql` | Performance indexes |
| `db/triggers.sql` | Automatic geometry updates |
| `db/functions.sql` | Helper functions |
| `db/examples.sql` | 30+ example queries |
| `src/database/loader.py` | Data loading script |
| `src/database/test.py` | Connection test |

---

## GTFS Specification

This database implements [GTFS Static Specification](https://gtfs.org/schedule/reference/).

**Supported Files:** agency, stops, routes, trips, stop_times, calendar, calendar_dates, shapes, transfers, fare_attributes, fare_rules, frequencies, feed_info

**Extensions:** PostGIS geometry, integer time format, multi-timezone

---

## Support

- **SQL Files:** `db/`
- **Example Queries:** `db/examples.sql`
- **GTFS Validator:** https://gtfs-validator.mobilitydata.org/
- **PostgreSQL Docs:** https://www.postgresql.org/docs/
- **PostGIS Docs:** https://postgis.net/docs/

---

**Happy Transit Planning! 🚇🚌🚊**
