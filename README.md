# GTFS Scraper Viewer Planner

This project downloads public transit data as [GTFS](https://gtfs.org/) tables with the **`scraper`** package, visualizes them in the browser with **`viewer`**, and plans multi-stop trips with **`planner`**.

## Setup

- Python 3.10+ recommended.
- Use a [virtual environment](https://docs.python.org/3/library/venv.html), then install dependencies and register the `src/` packages:

  ```bash
  pip install -r requirements.txt
  pip install -e .
  ```

- Imports: editable install (`pip install -e .`) is enough. Alternatively set **`PYTHONPATH=src`** and skip `-e .` if you prefer.

On Windows, a “file in use” error during `pip install` usually means another process still has the same Python environment open; close it and run `pip` again.

## Scraper (`src/scraper/`)

The scraper pulls agencies, stops, routes, shapes, trips, stop times, calendar, and fare data from the EasyWay AJAX API (`tr.easyway.info`). Code is split into `core.py` (orchestration), `http.py`, `export.py` (CSV + `transfers.txt`), and `transfers_from_stops.py`.

### Usage

```bash
python -m scraper
```

Or from code:

```python
from scraper import GTFSScraper

scraper = GTFSScraper(
    cities=["istanbul"],
    output_dir="gtfs",
    logs_dir="logs",
)
scraper.run()
```

### Output layout

| Path                         | Contents                                                                                                                                                                                                 |
| ---------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `output_dir` (e.g. `gtfs/`)  | `agency.txt`, `stops.txt`, `routes.txt`, `trips.txt`, `stop_times.txt`, `calendar.txt`, `shapes.txt`, `transfers.txt`, `fare_attributes.txt`, `fare_rules.txt`, `frequencies.txt`                         |
| `logs_dir` (default `logs/`) | `progress.json` (resume state), `scraper_YYYYMMDD_HHMMSS.log`                                                                                                                                             |

`transfers.txt` is generated when saving the feed (walking transfers between nearby stops).

### Notes

- Respect the site’s terms of service and rate limits; the script uses short `sleep` delays between calls.
- Completed cities are stored in `logs/progress.json`. To re-scrape a city, remove it from `completed_cities` or delete the file.
- If you previously used a version that wrote `progress.json` under `output_dir`, move it into `logs/` (or merge its contents) so resume state stays consistent.

## Viewer (`src/viewer/`)

Reads the GTFS folder produced by the scraper and shows routes, directions, schedules, stops, and a map.

```bash
python -m viewer
```

Equivalent: `streamlit run src/viewer/app.py` (from repo root; ensure `src` is on `PYTHONPATH` or use editable install).

By default the app loads `gtfs/` next to the **current working directory**. You need `agency.txt`, `stops.txt`, `routes.txt`, `trips.txt`, `stop_times.txt`, `calendar.txt`, and `shapes.txt`; `frequencies.txt` is optional.

## Trip planner (`src/planner/`)

Multi-waypoint routing (coordinates A → B → C …) with walking segments from **`transfers.txt`**, scheduled legs from **`stop_times.txt`**, and Pareto-style alternatives.

```bash
python -m planner
```

The planner **requires** `{gtfs_dir}/transfers.txt`. The scraper writes it when it saves the feed. The planner package does not generate `transfers.txt`. If it is missing, re-run the scraper or call `scraper.transfers_from_stops.write_transfers_file(gtfs_dir)` from Python (optional `max_distance_m`, `walk_speed_mps`).

### Behaviour notes

- **Frequencies:** This feed often expands headway-based service into many timed trips in `stop_times.txt` already; the router uses those rows. Pure frequency-only trips without expanded times are not modeled beyond that.
- **Arrive by:** Not implemented yet; the UI plans from a chosen departure time (or “now”).
- **Database:** Routing uses a small `TransitDataSource` / `CsvGtfsRepository` layer so a SQL-backed implementation can replace CSV reads later.

## Repository layout

```
src/
  scraper/          # EasyWay API → GTFS (+ transfers on save)
  viewer/           # Streamlit + Folium viewer
  planner/          # Repository, preprocess, router, journey chaining, Streamlit app.py
tests/
requirements.txt
pyproject.toml
```

## Tests

```bash
pytest
```

(or `python -m pytest`)

## License and data

Feed content belongs to the respective operators and sites. Use scraped data only in line with their policies and applicable law.
