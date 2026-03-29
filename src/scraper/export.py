"""Write in-memory GTFS tables to CSV files."""

from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import Any, Iterable, List

from .transfers_from_stops import write_transfers_file


def _save_csv(
    out_dir: Path,
    filename: str,
    data: Iterable[Any],
    fieldnames: List[str],
    logger: logging.Logger,
) -> None:
    rows = list(data)
    if not rows:
        return
    filepath = out_dir / filename
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    logger.info("%s saved (%s rows)", filename, len(rows))


def save_all_files(scraper: Any) -> None:
    """Persist ``GTFSScraper`` state to ``scraper.output_dir``."""
    logger = scraper.logger
    od = scraper.output_dir
    logger.info("Writing GTFS files...")

    _save_csv(
        od,
        "agency.txt",
        scraper.agencies.values(),
        [
            "agency_id",
            "agency_name",
            "agency_url",
            "agency_timezone",
            "agency_phone",
            "agency_lang",
        ],
        logger,
    )
    _save_csv(
        od,
        "stops.txt",
        scraper.stops.values(),
        ["stop_id", "stop_name", "stop_lat", "stop_lon", "location_type", "parent_station"],
        logger,
    )
    _save_csv(
        od,
        "routes.txt",
        scraper.routes.values(),
        ["route_id", "agency_id", "route_short_name", "route_long_name", "route_type"],
        logger,
    )
    _save_csv(
        od,
        "trips.txt",
        scraper.trips.values(),
        ["route_id", "service_id", "trip_id", "trip_headsign", "shape_id"],
        logger,
    )
    _save_csv(
        od,
        "stop_times.txt",
        scraper.stop_times,
        ["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"],
        logger,
    )
    _save_csv(
        od,
        "calendar.txt",
        scraper.calendar.values(),
        [
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
        logger,
    )
    all_shapes = []
    for shape_points in scraper.shapes.values():
        all_shapes.extend(shape_points)
    _save_csv(
        od,
        "shapes.txt",
        all_shapes,
        ["shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence"],
        logger,
    )
    _save_csv(
        od,
        "fare_attributes.txt",
        scraper.fare_attributes.values(),
        ["fare_id", "price", "currency_type", "payment_method", "transfers", "transfer_duration"],
        logger,
    )
    _save_csv(
        od,
        "fare_rules.txt",
        scraper.fare_rules,
        ["fare_id", "route_id", "origin_id", "destination_id", "contains_id"],
        logger,
    )
    _save_csv(
        od,
        "frequencies.txt",
        scraper.frequencies,
        ["trip_id", "start_time", "end_time", "headway_secs", "exact_times"],
        logger,
    )

    if scraper.stops:
        tp = write_transfers_file(od, stops=list(scraper.stops.values()))
        logger.info("transfers.txt saved (%s)", tp.name)

    logger.info("GTFS files written")
