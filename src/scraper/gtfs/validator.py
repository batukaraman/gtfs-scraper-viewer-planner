"""GTFS data validation and integrity checks."""

from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

PLACEHOLDER_LAT = 41.0082
PLACEHOLDER_LON = 28.9784


class GTFSValidator:
    """Validate and fix GTFS referential integrity issues."""

    def __init__(
        self,
        gtfs_dir: Path,
        timezone: str = "UTC",
        language: str = "en",
    ):
        self.gtfs_dir = gtfs_dir
        self.timezone = timezone
        self.language = language

    def validate_and_fix(self) -> dict[str, Any]:
        """Run all validation checks and fix issues. Returns report."""
        report: dict[str, Any] = {"gtfs_dir": str(self.gtfs_dir)}

        agency_fn = self.gtfs_dir / "agency.txt"
        routes_fn = self.gtfs_dir / "routes.txt"
        stops_fn = self.gtfs_dir / "stops.txt"
        trips_fn = self.gtfs_dir / "trips.txt"
        calendar_fn = self.gtfs_dir / "calendar.txt"
        stop_times_fn = self.gtfs_dir / "stop_times.txt"
        transfers_fn = self.gtfs_dir / "transfers.txt"
        fare_rules_fn = self.gtfs_dir / "fare_rules.txt"

        af, agencies = self._read_csv(agency_fn)
        rf, routes = self._read_csv(routes_fn)
        sf, stops = self._read_csv(stops_fn)
        tf, trips = self._read_csv(trips_fn)
        cf, calendar = self._read_csv(calendar_fn)

        agency_ids = {r["agency_id"].strip() for r in agencies if r.get("agency_id")}
        route_ids = {r["route_id"].strip() for r in routes if r.get("route_id")}
        stops_by_id = {r["stop_id"].strip(): r for r in stops if r.get("stop_id")}
        calendar_ids = {r["service_id"].strip() for r in calendar if r.get("service_id")}

        added_agencies = self._ensure_agencies(agencies, routes, agency_ids)
        added_calendar = self._ensure_calendar(calendar, trips, calendar_ids)

        trips, dropped_routes, dropped_services = self._filter_trips(
            trips, route_ids, calendar_ids
        )
        valid_trip_ids = {
            r["trip_id"].strip() for r in trips if r.get("trip_id")
        }

        transfer_stop_ids = self._get_transfer_stop_ids(transfers_fn)
        referenced_stops = set(transfer_stop_ids)

        (
            stop_times_kept,
            stop_times_dropped_trip,
            stop_times_dropped_empty,
        ) = self._filter_stop_times(
            stop_times_fn, valid_trip_ids, referenced_stops
        )

        added_stops = self._ensure_stops(stops_by_id, referenced_stops)
        stops = list(stops_by_id.values())

        fare_dropped = self._filter_fare_rules(fare_rules_fn, route_ids)

        self._write_csv(agency_fn, af, agencies)
        self._write_csv(stops_fn, sf, stops)
        self._write_csv(trips_fn, tf, trips)
        self._write_csv(calendar_fn, cf, calendar)

        report.update({
            "added_agencies": added_agencies,
            "added_calendar_services": added_calendar,
            "dropped_trips_bad_route": dropped_routes,
            "dropped_trips_bad_service": dropped_services,
            "stop_times_kept": stop_times_kept,
            "dropped_stop_times_bad_trip": stop_times_dropped_trip,
            "dropped_stop_times_empty_stop": stop_times_dropped_empty,
            "added_stops": added_stops,
            "fare_rules_dropped": fare_dropped,
        })

        for k, v in report.items():
            if k != "gtfs_dir":
                logger.info("Validation: %s = %s", k, v)

        return report

    def _read_csv(self, path: Path) -> tuple[list[str], list[dict[str, str]]]:
        if not path.exists():
            return [], []
        with path.open(encoding="utf-8", newline="") as f:
            r = csv.DictReader(f)
            fieldnames = list(r.fieldnames or [])
            rows = [dict(row) for row in r]
        return fieldnames, rows

    def _write_csv(
        self, path: Path, fieldnames: list[str], rows: list[dict[str, str]]
    ) -> None:
        if not fieldnames or not rows:
            return
        with path.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)

    def _ensure_agencies(
        self,
        agencies: list[dict],
        routes: list[dict],
        agency_ids: set[str],
    ) -> int:
        added = 0
        for row in routes:
            aid = (row.get("agency_id") or "").strip()
            if aid and aid not in agency_ids:
                agencies.append({
                    "agency_id": aid,
                    "agency_name": f"{aid} (Auto-generated)",
                    "agency_url": "https://example.com/transit",
                    "agency_timezone": self.timezone,
                    "agency_phone": "",
                    "agency_lang": self.language,
                })
                agency_ids.add(aid)
                added += 1
        return added

    def _ensure_calendar(
        self,
        calendar: list[dict],
        trips: list[dict],
        calendar_ids: set[str],
    ) -> int:
        added = 0
        service_from_trips = {
            (r.get("service_id") or "").strip()
            for r in trips
            if r.get("service_id")
        }
        for sid in service_from_trips:
            if sid and sid not in calendar_ids:
                calendar.append({
                    "service_id": sid,
                    "monday": "1",
                    "tuesday": "1",
                    "wednesday": "1",
                    "thursday": "1",
                    "friday": "1",
                    "saturday": "1",
                    "sunday": "1",
                    "start_date": "20250101",
                    "end_date": "20261231",
                })
                calendar_ids.add(sid)
                added += 1
        return added

    def _filter_trips(
        self,
        trips: list[dict],
        route_ids: set[str],
        calendar_ids: set[str],
    ) -> tuple[list[dict], int, int]:
        kept = []
        bad_route = 0
        bad_service = 0
        for row in trips:
            rid = (row.get("route_id") or "").strip()
            sid = (row.get("service_id") or "").strip()
            if not sid:
                bad_service += 1
                continue
            if rid not in route_ids:
                bad_route += 1
                continue
            if sid not in calendar_ids:
                bad_service += 1
                continue
            kept.append(row)
        return kept, bad_route, bad_service

    def _get_transfer_stop_ids(self, transfers_fn: Path) -> set[str]:
        stop_ids: set[str] = set()
        if transfers_fn.is_file():
            _, xfer = self._read_csv(transfers_fn)
            for x in xfer:
                a = (x.get("from_stop_id") or "").strip()
                b = (x.get("to_stop_id") or "").strip()
                if a:
                    stop_ids.add(a)
                if b:
                    stop_ids.add(b)
        return stop_ids

    def _filter_stop_times(
        self,
        stop_times_fn: Path,
        valid_trip_ids: set[str],
        referenced_stops: set[str],
    ) -> tuple[int, int, int]:
        if not stop_times_fn.exists():
            return 0, 0, 0

        kept = 0
        dropped_trip = 0
        dropped_empty = 0

        tmp = stop_times_fn.with_suffix(".tmp")
        with (
            stop_times_fn.open(encoding="utf-8", newline="") as fin,
            tmp.open("w", encoding="utf-8", newline="") as fout,
        ):
            reader = csv.DictReader(fin)
            st_fields = reader.fieldnames
            if not st_fields:
                return 0, 0, 0

            writer = csv.DictWriter(fout, fieldnames=list(st_fields), extrasaction="ignore")
            writer.writeheader()

            for row in reader:
                tid = (row.get("trip_id") or "").strip()
                sid = (row.get("stop_id") or "").strip()
                if not sid:
                    dropped_empty += 1
                    continue
                if tid not in valid_trip_ids:
                    dropped_trip += 1
                    continue
                referenced_stops.add(sid)
                writer.writerow(row)
                kept += 1

        tmp.replace(stop_times_fn)
        return kept, dropped_trip, dropped_empty

    def _ensure_stops(
        self,
        stops_by_id: dict[str, dict],
        referenced_stops: set[str],
    ) -> int:
        added = 0
        for sid in referenced_stops:
            if sid not in stops_by_id:
                stops_by_id[sid] = {
                    "stop_id": sid,
                    "stop_code": "",
                    "stop_name": "Unknown Stop",
                    "stop_desc": "",
                    "stop_lat": str(PLACEHOLDER_LAT),
                    "stop_lon": str(PLACEHOLDER_LON),
                    "zone_id": "",
                    "stop_url": "",
                    "location_type": "0",
                    "parent_station": "",
                    "stop_timezone": self.timezone,
                    "wheelchair_boarding": "0",
                    "platform_code": "",
                }
                added += 1
        return added

    def _filter_fare_rules(self, fare_rules_fn: Path, route_ids: set[str]) -> int:
        if not fare_rules_fn.is_file():
            return 0

        ff, fare_rules = self._read_csv(fare_rules_fn)
        kept = []
        dropped = 0

        for row in fare_rules:
            rid = (row.get("route_id") or "").strip()
            if rid and rid not in route_ids:
                dropped += 1
                continue
            kept.append(row)

        self._write_csv(fare_rules_fn, ff, kept)
        return dropped
