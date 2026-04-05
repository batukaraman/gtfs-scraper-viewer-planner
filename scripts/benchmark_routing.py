#!/usr/bin/env python3
"""
Time ``build_raptor_context`` + ``run_routing`` on a small GTFS fixture.

Usage (from repo root, with dev deps installed)::

    python scripts/benchmark_routing.py

Uses the synthetic GTFS layout from ``tests/test_routing_synthetic.py``.
"""

from __future__ import annotations

import csv
import datetime as dt
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


def _write_minimal_gtfs(d: Path) -> None:
    d.mkdir(parents=True, exist_ok=True)
    with open(d / "agency.txt", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "agency_id",
                "agency_name",
                "agency_url",
                "agency_timezone",
                "agency_phone",
                "agency_lang",
            ],
        )
        w.writeheader()
        w.writerow(
            {
                "agency_id": "a1",
                "agency_name": "Bench",
                "agency_url": "http://t",
                "agency_timezone": "Europe/Istanbul",
                "agency_phone": "",
                "agency_lang": "tr",
            }
        )
    with open(d / "stops.txt", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "stop_id",
                "stop_name",
                "stop_lat",
                "stop_lon",
                "location_type",
                "parent_station",
            ],
        )
        w.writeheader()
        for sid, lat, lon in [("s1", 41.0, 29.0), ("s2", 41.004, 29.0), ("s3", 41.05, 29.0)]:
            w.writerow(
                {
                    "stop_id": sid,
                    "stop_name": sid,
                    "stop_lat": lat,
                    "stop_lon": lon,
                    "location_type": 0,
                    "parent_station": "",
                }
            )
    with open(d / "routes.txt", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "route_id",
                "agency_id",
                "route_short_name",
                "route_long_name",
                "route_type",
            ],
        )
        w.writeheader()
        w.writerow(
            {
                "route_id": "r1",
                "agency_id": "a1",
                "route_short_name": "R1",
                "route_long_name": "Line",
                "route_type": 3,
            }
        )
    with open(d / "trips.txt", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "route_id",
                "service_id",
                "trip_id",
                "trip_headsign",
                "direction_id",
                "shape_id",
            ],
        )
        w.writeheader()
        w.writerow(
            {
                "route_id": "r1",
                "service_id": "svc1",
                "trip_id": "t1",
                "trip_headsign": "End",
                "direction_id": 0,
                "shape_id": "",
            }
        )
    with open(d / "stop_times.txt", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "trip_id",
                "arrival_time",
                "departure_time",
                "stop_id",
                "stop_sequence",
            ],
        )
        w.writeheader()
        rows = [
            ("t1", "08:00:00", "08:00:00", "s2", 1),
            ("t1", "08:10:00", "08:10:00", "s3", 2),
        ]
        for row in rows:
            w.writerow(dict(zip(w.fieldnames, row)))
    with open(d / "calendar.txt", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "service_id",
                "monday",
                "tuesday",
                "wednesday",
                "thursday",
                "friday",
                "saturday",
                "sunday",
                "start_date",
                "end_date",
            ],
        )
        w.writeheader()
        w.writerow(
            {
                "service_id": "svc1",
                "monday": 1,
                "tuesday": 1,
                "wednesday": 1,
                "thursday": 1,
                "friday": 1,
                "saturday": 1,
                "sunday": 1,
                "start_date": "20260101",
                "end_date": "20261231",
            }
        )
    with open(d / "transfers.txt", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["from_stop_id", "to_stop_id", "transfer_type", "min_transfer_time"],
        )
        w.writeheader()
        w.writerow(
            {
                "from_stop_id": "s1",
                "to_stop_id": "s2",
                "transfer_type": 2,
                "min_transfer_time": 60,
            }
        )


def main() -> None:
    from planner.preprocess import build_raptor_context
    from planner.raptor import run_routing
    from planner.repository import CsvGtfsRepository

    on_date = dt.date(2026, 3, 29)
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        _write_minimal_gtfs(d)
        repo = CsvGtfsRepository(d)
        t0 = time.perf_counter()
        repo.load()
        t1 = time.perf_counter()
        ctx = build_raptor_context(repo, on_date)
        t2 = time.perf_counter()
        journeys = run_routing(
            ctx,
            {"s1"},
            {"s3"},
            7 * 3600,
            min_transfer_sec=90,
            max_vehicle_legs=12,
            max_pareto=3,
        )
        t3 = time.perf_counter()
    print(f"repo.load:        {(t1 - t0) * 1000:.2f} ms")
    print(f"build_raptor_ctx: {(t2 - t1) * 1000:.2f} ms")
    print(f"run_routing:      {(t3 - t2) * 1000:.2f} ms")
    print(f"journeys found:   {len(journeys)}")


if __name__ == "__main__":
    main()
