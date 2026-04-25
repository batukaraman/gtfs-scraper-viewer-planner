# GTFS Scraper Pipeline

Multi-city GTFS data collection pipeline with on-demand OpenTripPlanner integration.

## Features

- **Multi-source GTFS collection**: Scrape from EasyWay API, static GTFS feeds, or custom sources
- **Multi-city support**: Configure and process multiple cities independently
- **Source merging**: Combine data from multiple sources per city
- **Automatic OSM preparation**: Download country-level OSM and extract city bounding boxes
- **Tiered OTP runtime**: `hot` cities stay online, `warm` cities are prewarmed by time windows, `cold` cities start on-demand
- **Unified API**: Single `/api/plan` endpoint routes to correct city automatically
- **Built-in caching**: Short TTL plan cache for repeated searches
- **Extensible**: Easy to add new data sources

## Architecture

```
┌─────────────────┐
│  Client/App     │
└────────┬────────┘
         │ POST /api/plan
         │ {from: {lat, lon}, to: {lat, lon}}
         ▼
┌─────────────────┐
│  Transit API    │ ← Koordinatlardan şehri tespit
│  Gateway :8000  │ ← Tiered container + cache yönetimi
└────────┬────────┘
         │
    ┌────┴────┬────────┐
    ▼         ▼        ▼
┌───────┐ ┌───────┐ ┌───────┐
│ OTP   │ │ OTP   │ │ OTP   │  ← tier politikasına göre açık/kapalı
│ :8080 │ │ :8081 │ │ :8082 │     (hot/warm/cold)
│İst.   │ │Ankara │ │Berlin │
└───────┘ └───────┘ └───────┘
```

## Quick Start

### 1. Setup Environment

```bash
# Clone or navigate to project
cd gtfs-scraper-viewer-planner

# Create virtual environment (recommended)
python -m venv .venv

# Activate virtual environment
# Windows (Git Bash / MINGW64):
source .venv/Scripts/activate
# Windows (CMD):
.venv\Scripts\activate
# Linux/Mac:
source .venv/bin/activate

# Install
pip install -e .
```

### 2. Verify Installation

```bash
gtfs-scraper list
```

Output:

```
Configured cities (1):

ID              Name        Country    Sources    OTP Port
----------------------------------------------------------------------
istanbul        İstanbul    turkey     easyway    8080
```

### 3. Configure Cities

Edit `config/cities.yaml`:

```yaml
countries:
  turkey:
    name: Türkiye
    osm:
      source: https://download.geofabrik.de/europe/turkey-latest.osm.pbf
      filename: turkey-latest.osm.pbf
    cities:
      istanbul:
        name: İstanbul
        timezone: Europe/Istanbul
        language: tr
        bbox:
          min_lon: 27.90
          min_lat: 40.72
          max_lon: 30.12
          max_lat: 41.58
        sources:
          - type: easyway
            city_code: istanbul
            base_url: https://tr.easyway.info/ajax/tr
        otp:
          memory: 8g
          port: 8080
```

### 4. Run Full Pipeline

```bash
# Full pipeline: GTFS scrape + OSM download/extract + OTP graph build
gtfs-scraper full --city istanbul
```

**Estimated times:**
| Step | Duration |
|------|----------|
| GTFS Scraping | 10-30 min |
| OSM Download (Turkey ~1GB) | 10-20 min |
| OSM City Extract | 2-5 min |
| OTP Graph Build | 5-15 min |
| **Total** | **30-70 min** |

### 5. Start Gateway

```bash
transit-gateway
```

Output:

```
INFO - Loading config from config/cities.yaml
INFO - Gateway started with 1 cities
INFO - Uvicorn running on http://0.0.0.0:8000
```

### 6. Plan a Trip

```bash
curl -X POST http://localhost:8000/api/plan \
  -H "Content-Type: application/json" \
  -d '{
    "from": {"lat": 41.0082, "lon": 28.9784},
    "to": {"lat": 41.0422, "lon": 29.0083}
  }'
```

Cold cities may take 20-60 seconds on first request (OTP startup).  
Hot/warm cities and cache hits typically respond in ~1-5 seconds.

---

## CLI Reference

### Commands

```bash
gtfs-scraper <command> [options]
```

| Command  | Description                         |
| -------- | ----------------------------------- |
| `list`   | List configured cities              |
| `scrape` | Scrape GTFS data only               |
| `build`  | Build OTP graph only                |
| `full`   | Full pipeline: scrape + OSM + build |

### Options

| Option                  | Description                        |
| ----------------------- | ---------------------------------- |
| `--city <id>`           | Process single city                |
| `--all`                 | Process all configured cities      |
| `--skip-osm`            | Skip OSM download/extract          |
| `--skip-build`          | Skip OTP graph build (full only)   |
| `--force`               | Overwrite existing files           |
| `-c, --config <path>`   | Custom config file path            |
| `-d, --base-dir <path>` | Base directory (data/, logs/)      |
| `-v, --verbose`         | Enable debug logging               |
| `--memory <size>`       | JVM heap size for build (e.g., 8g) |

### Examples

```bash
# List all cities
gtfs-scraper list

# Full pipeline for Istanbul
gtfs-scraper full --city istanbul

# Scrape only (no OSM, no build)
gtfs-scraper scrape --city istanbul --skip-osm

# Build graph only (GTFS must exist)
gtfs-scraper build --city istanbul

# Process all cities
gtfs-scraper full --all

# Force rebuild everything
gtfs-scraper full --city istanbul --force

# Custom memory for large cities
gtfs-scraper build --city istanbul --memory 12g
```

---

## API Reference

### POST /api/plan

Plan a transit trip. City is auto-detected from coordinates.

**Request Body:**

```json
{
  "from": { "lat": 41.0082, "lon": 28.9784 },
  "to": { "lat": 41.0422, "lon": 29.0083 },
  "date": "2025-01-15",
  "time": "09:00",
  "arrive_by": false,
  "mode": "TRANSIT,WALK",
  "num_itineraries": 5
}
```

| Field             | Type     | Required | Default      | Description             |
| ----------------- | -------- | -------- | ------------ | ----------------------- |
| `from`            | Location | Yes      | -            | Origin coordinates      |
| `to`              | Location | Yes      | -            | Destination coordinates |
| `date`            | string   | No       | today        | Date (YYYY-MM-DD)       |
| `time`            | string   | No       | now          | Time (HH:MM)            |
| `arrive_by`       | boolean  | No       | false        | Time is arrival time    |
| `mode`            | string   | No       | TRANSIT,WALK | Transport modes         |
| `num_itineraries` | int      | No       | 5            | Number of routes (1-10) |

**Query Parameters:**

- `city` (optional): Force specific city instead of auto-detection

**Response:**

```json
{
  "city": "istanbul",
  "request_time_ms": 1234,
  "data": {
    "data": {
      "plan": {
        "itineraries": [
          {
            "startTime": 1705312800000,
            "endTime": 1705314600000,
            "duration": 1800,
            "legs": [...]
          }
        ]
      }
    }
  }
}
```

### GET /cities

List available cities.

**Response:**

```json
{
  "istanbul": {
    "name": "İstanbul",
    "tier": "hot",
    "bbox": {
      "min_lon": 27.9,
      "min_lat": 40.72,
      "max_lon": 30.12,
      "max_lat": 41.58
    },
    "has_graph": true
  }
}
```

### GET /status

Get container status.

**Response:**

```json
{
  "status": "ok",
  "cities": {
    "istanbul": {
      "status": "running",
      "port": 8080,
      "tier": "hot",
      "idle_seconds": 45
    }
  },
  "timestamp": "2025-01-15T10:30:00"
}
```

### GET /health

Health check endpoint. Returns `{"status": "ok"}`.

### POST /cities/{city_id}/warmup

Warm up a city's OTP container proactively (useful right after user city selection).

---

## Project Structure

```
gtfs-scraper/
├── config/
│   └── cities.yaml              # City and source configuration
├── src/
│   ├── scraper/                 # GTFS data collection
│   │   ├── __main__.py          # CLI entry point
│   │   ├── config.py            # Config loader
│   │   ├── pipeline.py          # Main orchestration
│   │   ├── sources/             # Data source implementations
│   │   │   ├── base.py          # Abstract base class
│   │   │   └── easyway.py       # EasyWay API scraper
│   │   ├── gtfs/                # GTFS file utilities
│   │   │   ├── writer.py        # CSV/ZIP writer
│   │   │   ├── validator.py     # Data validation
│   │   │   └── transfers.py     # Walking transfers
│   │   └── osm/                 # OSM utilities
│   │       ├── downloader.py    # Geofabrik download
│   │       └── extractor.py     # Bbox extraction
│   └── gateway/                 # Transit API Gateway
│       ├── app.py               # FastAPI application
│       ├── config.py            # Gateway configuration
│       └── container_manager.py # On-demand Docker management
├── data/                        # All city data (gitignored artifacts)
│   ├── osm/                     # Shared OSM cache
│   │   └── turkey-latest.osm.pbf
│   └── {city}/                  # City data (GTFS + OTP)
│       ├── gtfs/                # GTFS .txt files (raw)
│       ├── gtfs.zip             # GTFS for OTP
│       ├── {city}.osm.pbf       # City OSM extract
│       ├── build-config.json    # OTP build config
│       ├── router-config.json   # OTP router config
│       └── graph.obj            # OTP graph
├── logs/                        # Progress logs (gitignored)
├── docker-compose.yml
├── Dockerfile.gateway
├── pyproject.toml
└── README.md
```

---

## Adding New Cities

### 1. Add to Config

Edit `config/cities.yaml`:

```yaml
countries:
  turkey:
    cities:
      # Existing city...
      istanbul:
        # ...

      # New city
      ankara:
        name: Ankara
        timezone: Europe/Istanbul
        language: tr
        bbox:
          min_lon: 32.50
          min_lat: 39.70
          max_lon: 33.10
          max_lat: 40.10
        sources:
          - type: easyway
            city_code: ankara
            base_url: https://tr.easyway.info/ajax/tr
        otp:
          memory: 4g
          port: 8081
```

### 2. Run Pipeline

```bash
gtfs-scraper full --city ankara
```

### 3. Done!

Gateway automatically serves the new city. No restart needed.

---

## Adding New Data Sources

### 1. Create Source Class

Create `src/scraper/sources/my_source.py`:

```python
"""My custom GTFS source."""

from __future__ import annotations

import logging
from pathlib import Path

from ..config import CityConfig, SourceConfig
from .base import GTFSData, GTFSSource

logger = logging.getLogger(__name__)


class MySource(GTFSSource):
    """Custom GTFS data source."""

    def __init__(
        self,
        city_config: CityConfig,
        source_config: SourceConfig,
        progress_dir: Path | None = None,
    ):
        self.city = city_config
        self.source = source_config
        # Access config: source_config.url, source_config.extra, etc.

    @property
    def source_type(self) -> str:
        return "my_source"

    def supports_resume(self) -> bool:
        return False  # True if you implement progress saving

    def scrape(self) -> GTFSData:
        """Scrape/download GTFS data."""
        data = GTFSData()

        # Populate data:
        data.agencies["agency_1"] = {
            "agency_id": "agency_1",
            "agency_name": "My Agency",
            "agency_url": "https://example.com",
            "agency_timezone": self.city.timezone,
        }

        data.stops["stop_1"] = {
            "stop_id": "stop_1",
            "stop_name": "Central Station",
            "stop_lat": 41.0,
            "stop_lon": 29.0,
        }

        # ... populate routes, trips, stop_times, etc.

        return data
```

### 2. Register Source

Edit `src/scraper/sources/__init__.py`:

```python
from .base import GTFSSource, GTFSData
from .easyway import EasyWaySource
from .my_source import MySource  # Add import

__all__ = ["GTFSSource", "GTFSData", "EasyWaySource", "MySource"]
```

Edit `src/scraper/pipeline.py`:

```python
from .sources.easyway import EasyWaySource
from .sources.my_source import MySource  # Add import

SOURCE_REGISTRY: dict[str, type[GTFSSource]] = {
    "easyway": EasyWaySource,
    "my_source": MySource,  # Register
}
```

### 3. Use in Config

```yaml
cities:
  my_city:
    sources:
      - type: my_source
        url: https://example.com/gtfs.zip
        extra_param: value
```

---

## Multiple Sources per City

A city can have multiple GTFS sources that get merged:

```yaml
cities:
  istanbul:
    sources:
      - type: easyway
        city_code: istanbul
      - type: static_gtfs
        url: https://data.ibb.gov.tr/gtfs.zip
```

### Merge Behavior

| Data Type  | Merge Strategy                 |
| ---------- | ------------------------------ |
| agencies   | Merge by ID (later overwrites) |
| stops      | Merge by ID (later overwrites) |
| routes     | Merge by ID (later overwrites) |
| trips      | Merge by ID (later overwrites) |
| stop_times | Append all                     |
| calendar   | Merge by ID                    |
| shapes     | Merge by ID                    |
| fare_rules | Append all                     |

**Tip:** Use unique prefixes in IDs to avoid conflicts:

```python
stop_id = f"easyway_{city}_{original_id}"
stop_id = f"ibb_{original_id}"
```

---

## Configuration

### Environment Variables

| Variable           | Default              | Description             |
| ------------------ | -------------------- | ----------------------- |
| `GATEWAY_CONFIG`   | `config/cities.yaml` | Config file path        |
| `GATEWAY_DATA_DIR` | `./data`             | Data directory          |
| `GATEWAY_HOST`     | `0.0.0.0`            | Gateway bind host       |
| `GATEWAY_PORT`     | `8000`               | Gateway port            |

### Container Management Settings

In `config/cities.yaml` (`gateway` section):

```python
container_idle_timeout: 300       # cold-tier idle timeout
warm_city_idle_timeout: 1800      # warm-tier idle timeout
container_startup_timeout: 300    # startup health wait
health_check_interval: 5          # health probe interval
prewarm_poll_interval: 60         # prewarm policy loop
plan_cache_ttl_seconds: 90        # /api/plan cache TTL
```

Per-city runtime policy (`otp` section):

```yaml
otp:
  port: 8080
  memory: 8g
  tier: hot            # hot | warm | cold
  idle_timeout: 1200   # optional override per city
  prewarm_windows:     # for warm tier (optional)
    - "07:00-10:00"
    - "17:00-20:00"
```

---

## Troubleshooting

### "ModuleNotFoundError: No module named 'yaml'"

Virtual environment not activated or packages not installed:

```bash
source .venv/Scripts/activate  # Windows
pip install -e .
```

### "Graph not found for city"

Run the pipeline first:

```bash
gtfs-scraper full --city istanbul
```

### "Docker is required for OSM extraction"

Install and start Docker Desktop.

### OTP container fails to start

Check Docker logs:

```bash
docker logs otp_istanbul
```

Common issues:

- Not enough memory: Increase `otp.memory` in config
- Port already in use: Change `otp.port` in config

### Gateway can't connect to OTP

Ensure Docker socket is accessible and OTP container is running:

```bash
docker ps
curl http://localhost:8080/otp/routers/default
```

---

## Requirements

- Python 3.10+
- Docker (for OTP and OSM extraction)
- ~10GB disk space per city (OSM + GTFS + graph)
- 4-8GB RAM per running OTP instance

---

## License

MIT
