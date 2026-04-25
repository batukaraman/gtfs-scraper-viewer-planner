"""Write GTFS data to files."""

from __future__ import annotations

import csv
import logging
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from ..config import CityConfig
from ..sources.base import GTFSData
from .transfers import build_transfers

logger = logging.getLogger(__name__)


FARE_LICENSE = (
    "TR: Ücretler yalnızca tahmin niteliğindedir; resmi tarife değildir. "
    "EN: Fares are estimates only; not official tariffs."
)


class GTFSWriter:
    """Write GTFS data to CSV files and create ZIP archive."""

    GTFS_FIELDS = {
        "agency.txt": [
            "agency_id", "agency_name", "agency_url", "agency_timezone",
            "agency_phone", "agency_lang",
        ],
        "stops.txt": [
            "stop_id", "stop_code", "stop_name", "stop_desc", "stop_lat",
            "stop_lon", "zone_id", "stop_url", "location_type",
            "parent_station", "stop_timezone", "wheelchair_boarding", "platform_code",
        ],
        "routes.txt": [
            "route_id", "agency_id", "route_short_name", "route_long_name",
            "route_desc", "route_type", "route_url", "route_color",
            "route_text_color", "route_sort_order",
        ],
        "trips.txt": [
            "route_id", "service_id", "trip_id", "trip_headsign",
            "trip_short_name", "direction_id", "block_id", "shape_id",
            "wheelchair_accessible", "bikes_allowed",
        ],
        "stop_times.txt": [
            "trip_id", "arrival_time", "departure_time", "stop_id",
            "stop_sequence", "stop_headsign", "pickup_type", "drop_off_type",
            "shape_dist_traveled", "timepoint",
        ],
        "calendar.txt": [
            "service_id", "monday", "tuesday", "wednesday", "thursday",
            "friday", "saturday", "sunday", "start_date", "end_date",
        ],
        "calendar_dates.txt": [
            "service_id", "date", "exception_type",
        ],
        "shapes.txt": [
            "shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence",
        ],
        "fare_attributes.txt": [
            "fare_id", "price", "currency_type", "payment_method",
            "transfers", "transfer_duration",
        ],
        "fare_rules.txt": [
            "fare_id", "route_id", "origin_id", "destination_id", "contains_id",
        ],
        "frequencies.txt": [
            "trip_id", "start_time", "end_time", "headway_secs", "exact_times",
        ],
        "transfers.txt": [
            "from_stop_id", "to_stop_id", "transfer_type", "min_transfer_time",
        ],
        "feed_info.txt": [
            "feed_publisher_name", "feed_publisher_url", "feed_lang",
            "feed_start_date", "feed_end_date", "feed_version",
            "feed_contact_email", "feed_contact_url", "feed_id", "feed_license",
        ],
    }

    def __init__(self, city: CityConfig, output_dir: Path):
        self.city = city
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def write(self, data: GTFSData) -> Path:
        """Write all GTFS files and return path to output directory."""
        logger.info("Writing GTFS files to %s...", self.output_dir)

        self._write_csv("agency.txt", data.agencies.values())
        self._write_csv("stops.txt", data.stops.values())
        self._write_csv("routes.txt", data.routes.values())
        self._write_csv("trips.txt", data.trips.values())
        self._write_csv("stop_times.txt", data.stop_times)
        self._write_csv("calendar.txt", data.calendar.values())

        if data.calendar_dates:
            self._write_csv("calendar_dates.txt", data.calendar_dates)

        all_shapes = []
        for shape_points in data.shapes.values():
            all_shapes.extend(shape_points)
        if all_shapes:
            self._write_csv("shapes.txt", all_shapes)

        if data.fare_attributes:
            self._write_csv("fare_attributes.txt", data.fare_attributes.values())

        if data.fare_rules:
            self._write_csv("fare_rules.txt", data.fare_rules)

        if data.frequencies:
            self._write_csv("frequencies.txt", data.frequencies)

        if data.stops:
            transfers = build_transfers(list(data.stops.values()))
            if transfers:
                self._write_csv("transfers.txt", transfers)

        self._write_feed_info(data)

        logger.info("GTFS files written successfully")
        return self.output_dir

    def _write_csv(self, filename: str, rows: Iterable[Any]) -> None:
        """Write rows to CSV file."""
        rows_list = list(rows)
        if not rows_list:
            return

        filepath = self.output_dir / filename
        fieldnames = self.GTFS_FIELDS.get(filename, list(rows_list[0].keys()))

        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows_list)

        logger.debug("%s written (%d rows)", filename, len(rows_list))

    def _write_feed_info(self, data: GTFSData) -> None:
        """Write feed_info.txt with metadata."""
        start_date, end_date = self._calendar_window(data.calendar)
        build_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")

        feed_info = [{
            "feed_publisher_name": f"GTFS Pipeline - {self.city.name}",
            "feed_publisher_url": "https://example.com/gtfs-pipeline",
            "feed_lang": self.city.language,
            "feed_start_date": start_date,
            "feed_end_date": end_date,
            "feed_version": f"{self.city.id}|{start_date}_{end_date}|{build_time}",
            "feed_contact_email": "",
            "feed_contact_url": "",
            "feed_id": f"gtfs-{self.city.id}",
            "feed_license": FARE_LICENSE,
        }]

        self._write_csv("feed_info.txt", feed_info)

    def _calendar_window(self, calendar: dict) -> tuple[str, str]:
        """Get min start_date and max end_date from calendar."""
        default_start = datetime.now().strftime("%Y%m%d")
        default_end = (datetime.now().replace(year=datetime.now().year + 1)).strftime("%Y%m%d")

        if not calendar:
            return default_start, default_end

        starts = []
        ends = []
        for row in calendar.values():
            s = row.get("start_date", "")
            e = row.get("end_date", "")
            if s and len(str(s)) == 8:
                starts.append(str(s))
            if e and len(str(e)) == 8:
                ends.append(str(e))

        if not starts or not ends:
            return default_start, default_end

        return min(starts), max(ends)

    def create_zip(self, zip_path: Path | None = None) -> Path:
        """Create ZIP archive of GTFS files."""
        if zip_path is None:
            zip_path = self.output_dir.parent / f"{self.city.id}_gtfs.zip"

        txt_files = list(self.output_dir.glob("*.txt"))
        if not txt_files:
            raise ValueError(f"No GTFS files found in {self.output_dir}")

        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for txt_file in txt_files:
                zf.write(txt_file, txt_file.name)

        logger.info("Created GTFS ZIP: %s (%d files)", zip_path, len(txt_files))
        return zip_path
