# GTFS Scraper Viewer Planner

This project downloads public transit data as [GTFS](https://gtfs.org/) tables with **`scraper.py`**, visualizes them in the browser with **`viewer.py`**, and will add a **trip planner** later.

## Requirements

- Python 3.10+ recommended
- Dependencies: `pip install -r requirements.txt`

## Scraper (`scraper.py`)

The scraper pulls agencies, stops, routes, shapes, trips, stop times, calendar, and fare data from the EasyWay AJAX API (`tr.easyway.info`).

### Usage

```bash
python scraper.py
```

Or from code:

```python
from scraper import GTFSScraper

scraper = GTFSScraper(
    cities=["istanbul"],       # city slugs as used in the API path
    output_dir="gtfs",         # GTFS CSV/txt files go here
    logs_dir="logs",           # progress + log files (default: "logs")
)
scraper.run()
```

### Output layout

| Path                         | Contents                                                                                                                                                         |
| ---------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `output_dir` (e.g. `gtfs/`)  | `agency.txt`, `stops.txt`, `routes.txt`, `trips.txt`, `stop_times.txt`, `calendar.txt`, `shapes.txt`, `fare_attributes.txt`, `fare_rules.txt`, `frequencies.txt` |
| `logs_dir` (default `logs/`) | `progress.json` (resume state: completed cities, processed routes), `scraper_YYYYMMDD_HHMMSS.log` (run log)                                                      |

GTFS text files use comma-separated values with a header row. The scraper periodically saves feed files and `progress.json` so a long run can be interrupted and resumed.

### Notes

- Respect the site’s terms of service and rate limits; the script uses short `sleep` delays between calls.
- Completed cities are stored in `logs/progress.json`. To re-scrape a city, remove it from `completed_cities` or delete the file.
- If you previously used a version that wrote `progress.json` under `output_dir`, move it into `logs/` (or merge its contents) so resume state stays consistent.

## Viewer (`viewer.py`)

**`viewer.py`** reads the GTFS folder produced by **`scraper.py`** (same filenames) and shows routes, directions, schedules, stops, and a map.

```bash
streamlit run viewer.py
```

By default it loads data from the `gtfs/` directory next to the script. You need `agency.txt`, `stops.txt`, `routes.txt`, `trips.txt`, `stop_times.txt`, `calendar.txt`, and `shapes.txt`; `frequencies.txt` is optional.

## Roadmap

- **Planner** — trip planning on top of the same GTFS feed (not implemented yet).

## Repository layout

```
scraper.py          # API -> GTFS
viewer.py           # Streamlit + Folium viewer
requirements.txt
```

## License and data

Feed content belongs to the respective operators and sites. Use scraped data only in line with their policies and applicable law.
