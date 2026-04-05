"""GTFS referential integrity helpers for OTP and onebusaway-style loaders.

Run on disk: ``python scripts/validate_and_fix_gtfs.py`` (repo root).
Used in-memory from :func:`export.save_all_files` and during scrape in ``core``.
"""

from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Set, Tuple

# Istanbul centroid — fallback when API does not list coordinates
PLACEHOLDER_LAT = 41.0082
PLACEHOLDER_LON = 28.9784

PLACEHOLDER_AGENCY: Dict[str, Any] = {
    "agency_url": "https://tr.easyway.info",
    "agency_timezone": "Europe/Istanbul",
    "agency_phone": "",
    "agency_lang": "tr",
}


def _placeholder_stop_row(stop_id: str) -> Dict[str, Any]:
    return {
        "stop_id": stop_id,
        "stop_code": "",
        "stop_name": "Eksik durak (feed; koordinat tahmini)",
        "stop_desc": "",
        "stop_lat": PLACEHOLDER_LAT,
        "stop_lon": PLACEHOLDER_LON,
        "zone_id": "",
        "stop_url": "",
        "location_type": 0,
        "parent_station": "",
        "stop_timezone": "Europe/Istanbul",
        "wheelchair_boarding": "0",
        "platform_code": "",
    }


def ensure_agencies_cover_routes(
    agencies: MutableMapping[str, Any],
    routes: Mapping[str, Any],
) -> int:
    """Ensure every ``routes[*].agency_id`` has an agency row. Returns count added."""
    used = {r["agency_id"] for r in routes.values()}
    added = 0
    for agency_id in used:
        if agency_id in agencies:
            continue
        agencies[agency_id] = {
            "agency_id": agency_id,
            "agency_name": f"{agency_id} (Easyway)",
            **PLACEHOLDER_AGENCY,
        }
        added += 1
    return added


def ensure_stops_cover_stop_times(
    stops: MutableMapping[str, Any],
    stop_times: Iterable[Mapping[str, Any]],
) -> int:
    """Ensure every ``stop_times[*].stop_id`` has a stop row. Returns count added."""
    referenced: Set[str] = set()
    for row in stop_times:
        sid = (row.get("stop_id") or "").strip()
        if sid:
            referenced.add(sid)
    added = 0
    for sid in referenced:
        if sid in stops:
            continue
        stops[sid] = _placeholder_stop_row(sid)
        added += 1
    return added


def ensure_stop_from_schedule_payload(
    stops: MutableMapping[str, Any],
    city: str,
    stop_id: str,
    stop: Optional[Mapping[str, Any]],
) -> None:
    """Register ``stop_id`` in ``stops`` using schedule payload if missing from global /stops scrape."""
    if stop_id in stops:
        return
    name = "Durak"
    lat, lon = PLACEHOLDER_LAT, PLACEHOLDER_LON
    if isinstance(stop, dict):
        name = (
            stop.get("n")
            or stop.get("name")
            or stop.get("nm")
            or stop.get("t")
            or name
        )
        if "lat" in stop and "lon" in stop:
            try:
                lat = float(stop["lat"])
                lon = float(stop["lon"])
            except (TypeError, ValueError):
                pass
        elif "la" in stop and "lo" in stop:
            try:
                lat = float(stop["la"])
                lon = float(stop["lo"])
            except (TypeError, ValueError):
                pass
        elif isinstance(stop.get("c"), (list, tuple)) and len(stop["c"]) >= 2:
            try:
                lat = float(stop["c"][0]) / 1_000_000.0
                lon = float(stop["c"][1]) / 1_000_000.0
            except (TypeError, ValueError, IndexError):
                pass
    stops[stop_id] = {
        "stop_id": stop_id,
        "stop_code": "",
        "stop_name": str(name)[:256],
        "stop_desc": "",
        "stop_lat": lat,
        "stop_lon": lon,
        "zone_id": "",
        "stop_url": "",
        "location_type": 0,
        "parent_station": "",
        "stop_timezone": "Europe/Istanbul",
        "wheelchair_boarding": "0",
        "platform_code": "",
    }


def _read_csv(path: Path) -> Tuple[List[str], List[Dict[str, str]]]:
    with path.open(encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        fieldnames = list(r.fieldnames or [])
        rows = [dict(row) for row in r]
    return fieldnames, rows


def _write_csv(path: Path, fieldnames: List[str], rows: List[Dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def _default_calendar_row(service_id: str) -> Dict[str, str]:
    return {
        "service_id": service_id,
        "monday": "1",
        "tuesday": "1",
        "wednesday": "1",
        "thursday": "1",
        "friday": "1",
        "saturday": "1",
        "sunday": "1",
        "start_date": "20250101",
        "end_date": "20261231",
    }


def fix_gtfs_directory(gtfs_dir: Path, logger: Optional[logging.Logger] = None) -> Dict[str, Any]:
    """Repair referential issues OTP commonly rejects. Rewrites CSVs in ``gtfs_dir`` in place.

    - agencies ← routes.agency_id
    - calendar ← trips.service_id (stub row if missing)
    - trips dropped if route_id missing
    - stop_times dropped if trip_id invalid or empty stop_id
    - stops ← all referenced stop_ids (stop_times + transfers)
    - fare_rules rows dropped if route_id missing

    Returns a summary dict with counts. Large ``stop_times.txt`` is streamed (typically 1–3 min).
    """
    log = logger or logging.getLogger(__name__)
    report: Dict[str, Any] = {"gtfs_dir": str(gtfs_dir.resolve())}

    agency_fn = gtfs_dir / "agency.txt"
    routes_fn = gtfs_dir / "routes.txt"
    stops_fn = gtfs_dir / "stops.txt"
    trips_fn = gtfs_dir / "trips.txt"
    calendar_fn = gtfs_dir / "calendar.txt"
    stop_times_fn = gtfs_dir / "stop_times.txt"
    transfers_fn = gtfs_dir / "transfers.txt"
    fare_rules_fn = gtfs_dir / "fare_rules.txt"

    af, agencies = _read_csv(agency_fn)
    rf, routes = _read_csv(routes_fn)
    sf, stops = _read_csv(stops_fn)
    tf, trips = _read_csv(trips_fn)
    cf, calendar = _read_csv(calendar_fn)

    agency_ids = {r["agency_id"].strip() for r in agencies if r.get("agency_id")}
    route_ids = {r["route_id"].strip() for r in routes if r.get("route_id")}
    stops_by_id = {r["stop_id"].strip(): r for r in stops if r.get("stop_id")}
    calendar_ids = {r["service_id"].strip() for r in calendar if r.get("service_id")}

    added_agencies = 0
    for row in routes:
        aid = (row.get("agency_id") or "").strip()
        if aid and aid not in agency_ids:
            agencies.append(
                {
                    "agency_id": aid,
                    "agency_name": f"{aid} (Easyway)",
                    "agency_url": PLACEHOLDER_AGENCY["agency_url"],
                    "agency_timezone": PLACEHOLDER_AGENCY["agency_timezone"],
                    "agency_phone": "",
                    "agency_lang": "tr",
                }
            )
            agency_ids.add(aid)
            added_agencies += 1

    service_from_trips = {(r.get("service_id") or "").strip() for r in trips if r.get("service_id")}
    added_calendar = 0
    for sid in service_from_trips:
        if sid and sid not in calendar_ids:
            calendar.append(_default_calendar_row(sid))
            calendar_ids.add(sid)
            added_calendar += 1

    trips_before = len(trips)
    trips_kept: List[Dict[str, str]] = []
    dropped_bad_route = 0
    dropped_bad_service = 0
    for row in trips:
        rid = (row.get("route_id") or "").strip()
        sid = (row.get("service_id") or "").strip()
        if not sid:
            dropped_bad_service += 1
            continue
        if rid not in route_ids:
            dropped_bad_route += 1
            continue
        if sid not in calendar_ids:
            dropped_bad_service += 1
            continue
        trips_kept.append(row)
    trips = trips_kept
    valid_trip_ids: Set[str] = {(r.get("trip_id") or "").strip() for r in trips if r.get("trip_id")}

    transfer_stop_ids: Set[str] = set()
    if transfers_fn.is_file():
        _, xfer = _read_csv(transfers_fn)
        for x in xfer:
            a = (x.get("from_stop_id") or "").strip()
            b = (x.get("to_stop_id") or "").strip()
            if a:
                transfer_stop_ids.add(a)
            if b:
                transfer_stop_ids.add(b)

    referenced_stops: Set[str] = set(transfer_stop_ids)
    stop_times_kept = 0
    stop_times_dropped_trip = 0
    stop_times_dropped_empty = 0

    tmp_times = gtfs_dir / "stop_times.txt.tmp"
    with stop_times_fn.open(encoding="utf-8", newline="") as fin, tmp_times.open(
        "w", encoding="utf-8", newline=""
    ) as fout:
        reader = csv.DictReader(fin)
        st_fields = reader.fieldnames
        if not st_fields:
            raise ValueError("stop_times.txt has no header")
        writer = csv.DictWriter(fout, fieldnames=list(st_fields), extrasaction="ignore")
        writer.writeheader()
        for row in reader:
            tid = (row.get("trip_id") or "").strip()
            sid = (row.get("stop_id") or "").strip()
            if not sid:
                stop_times_dropped_empty += 1
                continue
            if tid not in valid_trip_ids:
                stop_times_dropped_trip += 1
                continue
            referenced_stops.add(sid)
            writer.writerow(row)
            stop_times_kept += 1

    tmp_times.replace(stop_times_fn)

    added_stops = 0
    for sid in referenced_stops:
        if sid not in stops_by_id:
            pr = _placeholder_stop_row(sid)
            stops_by_id[sid] = {
                "stop_id": sid,
                "stop_code": "",
                "stop_name": str(pr["stop_name"]),
                "stop_desc": "",
                "stop_lat": str(pr["stop_lat"]),
                "stop_lon": str(pr["stop_lon"]),
                "zone_id": "",
                "stop_url": "",
                "location_type": "0",
                "parent_station": "",
                "stop_timezone": "Europe/Istanbul",
                "wheelchair_boarding": "0",
                "platform_code": "",
            }
            added_stops += 1

    stops = list(stops_by_id.values())

    fare_dropped = 0
    if fare_rules_fn.is_file():
        ff, fare_rules = _read_csv(fare_rules_fn)
        fare_kept = []
        for row in fare_rules:
            rid = (row.get("route_id") or "").strip()
            if rid and rid not in route_ids:
                fare_dropped += 1
                continue
            fare_kept.append(row)
        _write_csv(fare_rules_fn, ff, fare_kept)

    _write_csv(agency_fn, af, agencies)
    _write_csv(stops_fn, sf, stops)
    _write_csv(trips_fn, tf, trips)
    _write_csv(calendar_fn, cf, calendar)

    report.update(
        {
            "added_agencies": added_agencies,
            "added_calendar_services": added_calendar,
            "trips_before": trips_before,
            "trips_after": len(trips),
            "dropped_trips_bad_route": dropped_bad_route,
            "dropped_trips_bad_service": dropped_bad_service,
            "stop_times_kept": stop_times_kept,
            "dropped_stop_times_bad_trip": stop_times_dropped_trip,
            "dropped_stop_times_empty_stop": stop_times_dropped_empty,
            "added_stops": added_stops,
            "fare_rules_dropped_bad_route": fare_dropped,
        }
    )

    for k, v in report.items():
        if k != "gtfs_dir":
            log.info("GTFS fix: %s=%s", k, v)
    return report
