"""Minimal GTFS fixture: walk between two stops then one ride."""

import csv
import datetime as dt
import tempfile
from pathlib import Path

import unittest

from planner.journey import plan_multi
from planner.preprocess import build_raptor_context
from planner.repository import CsvGtfsRepository


def _write_gtfs(d: Path) -> None:
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
                "agency_name": "Test",
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
        w.writerow(
            {
                "stop_id": "s1",
                "stop_name": "One",
                "stop_lat": 41.0,
                "stop_lon": 29.0,
                "location_type": 0,
                "parent_station": "",
            }
        )
        w.writerow(
            {
                "stop_id": "s2",
                "stop_name": "Two",
                "stop_lat": 41.004,
                "stop_lon": 29.0,
                "location_type": 0,
                "parent_station": "",
            }
        )
        w.writerow(
            {
                "stop_id": "s3",
                "stop_name": "Three",
                "stop_lat": 41.05,
                "stop_lon": 29.0,
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
                "route_short_name": "T",
                "route_long_name": "Test Line",
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
                "shape_id",
            ],
        )
        w.writeheader()
        w.writerow(
            {
                "route_id": "r1",
                "service_id": "weekday",
                "trip_id": "t1",
                "trip_headsign": "East",
                "shape_id": "sh1",
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
        w.writerow(
            {
                "trip_id": "t1",
                "arrival_time": "10:00:00",
                "departure_time": "10:00:00",
                "stop_id": "s2",
                "stop_sequence": 1,
            }
        )
        w.writerow(
            {
                "trip_id": "t1",
                "arrival_time": "10:05:00",
                "departure_time": "10:05:00",
                "stop_id": "s3",
                "stop_sequence": 2,
            }
        )
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
                "service_id": "weekday",
                "monday": 1,
                "tuesday": 1,
                "wednesday": 1,
                "thursday": 1,
                "friday": 1,
                "saturday": 1,
                "sunday": 1,
                "start_date": "20200101",
                "end_date": "20991231",
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
                "min_transfer_time": 120,
            }
        )
    with open(d / "shapes.txt", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence"],
        )
        w.writeheader()
        w.writerow(
            {"shape_id": "sh1", "shape_pt_lat": 41.004, "shape_pt_lon": 29.0, "shape_pt_sequence": 1}
        )
        w.writerow(
            {"shape_id": "sh1", "shape_pt_lat": 41.05, "shape_pt_lon": 29.0, "shape_pt_sequence": 2}
        )


class TestSyntheticRouting(unittest.TestCase):
    def test_walk_then_ride(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            _write_gtfs(d)
            repo = CsvGtfsRepository(d)
            repo.load()
            ctx = build_raptor_context(repo, dt.date(2026, 3, 29))
            plan = plan_multi(
                ctx,
                [(41.0, 29.0), (41.05, 29.0)],
                dt.date(2026, 3, 29),
                9 * 3600,
                snap_radius_m=400,
                snap_k=3,
                min_transfer_sec=60,
                min_leg_transfer_sec=60,
            )
            self.assertTrue(plan.ok)
            j = plan.segments[0].chosen
            assert j is not None
            self.assertEqual(j.arrival_sec, 10 * 3600 + 5 * 60)
            modes = [leg.mode for leg in j.legs]
            self.assertIn("walk", modes)
            self.assertIn("ride", modes)


if __name__ == "__main__":
    unittest.main()
