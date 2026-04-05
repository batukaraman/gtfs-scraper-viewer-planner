"""Write in-memory GTFS tables to CSV files and optionally to PostgreSQL."""

from __future__ import annotations

import csv
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List

from .gtfs_integrity import ensure_agencies_cover_routes, ensure_stops_cover_stop_times
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


def _calendar_service_window(calendar: Dict) -> tuple[str, str]:
    """Min start_date and max end_date from calendar rows (YYYYMMDD)."""
    if not calendar:
        return "20251019", "20261019"
    starts = []
    ends = []
    for row in calendar.values():
        s = row.get("start_date", "")
        e = row.get("end_date", "")
        if s and len(s) == 8 and s.isdigit():
            starts.append(s)
        if e and len(e) == 8 and e.isdigit():
            ends.append(e)
    if not starts or not ends:
        return "20251019", "20261019"
    return min(starts), max(ends)


def _build_feed_version(feed_id: str, data_start: str, data_end: str, build_utc: datetime) -> str:
    """Stable, auditable version: data window + immutable UTC build timestamp."""
    stamp = build_utc.astimezone(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")
    return f"{feed_id}|data_{data_start}_{data_end}|build_{stamp}"


# Shown in feed_info.feed_license; consumers must not treat fare tables as official tariffs.
_FARE_AND_DATA_LICENSE = (
    "TR: Ücretler yalnızca tr.easyway.info üzerinden görünen tahmini liste "
    "fiyatlarıdır; resmi İETT/İBB/İstanbulkart tarifesi değildir. Aktarma "
    "indirimleri, abonelikler ve özel tarifeler modellenmemiştir. Faturalama "
    "veya hukuki ücret tespiti için kullanılamaz. "
    "EN: Fares are approximate list prices from tr.easyway.info, not official "
    "İETT/İBB/İstanbulkart products. Transfer discounts, subscriptions, and "
    "special tariffs are not modeled. Do not use for billing or legal fare "
    "determination."
)


def _save_feed_info(out_dir: Path, logger: logging.Logger, scraper: Any) -> None:
    """Create feed_info.txt with metadata tied to calendar window and build time."""
    data_start, data_end = _calendar_service_window(scraper.calendar)
    build_utc = getattr(scraper, "feed_build_utc", None) or datetime.now(timezone.utc)
    feed_id = "easyway-istanbul-gtfs"
    feed_version = _build_feed_version(feed_id, data_start, data_end, build_utc)

    feed_info = [{
        "feed_publisher_name": "EasyWay Istanbul GTFS",
        "feed_publisher_url": "https://tr.easyway.info",
        "feed_lang": "tr",
        "feed_start_date": data_start,
        "feed_end_date": data_end,
        "feed_version": feed_version,
        "feed_contact_email": "",
        "feed_contact_url": "",
        "feed_id": feed_id,
        "feed_license": _FARE_AND_DATA_LICENSE,
    }]

    _save_csv(
        out_dir,
        "feed_info.txt",
        feed_info,
        [
            "feed_publisher_name",
            "feed_publisher_url",
            "feed_lang",
            "feed_start_date",
            "feed_end_date",
            "feed_version",
            "feed_contact_email",
            "feed_contact_url",
            "feed_id",
            "feed_license",
        ],
        logger,
    )


def _save_calendar_dates(out_dir: Path, calendar: Dict, logger: logging.Logger) -> None:
    """Create calendar_dates.txt with Turkish national holidays."""
    # Turkish national holidays for 2025-2026
    turkish_holidays = [
        ("20250101", "Yılbaşı"),
        ("20250423", "Ulusal Egemenlik ve Çocuk Bayramı"),
        ("20250501", "Emek ve Dayanışma Günü"),
        ("20250519", "Atatürk'ü Anma, Gençlik ve Spor Bayramı"),
        ("20250331", "Ramazan Bayramı 1. Gün"),
        ("20250401", "Ramazan Bayramı 2. Gün"),
        ("20250402", "Ramazan Bayramı 3. Gün"),
        ("20250607", "Kurban Bayramı 1. Gün"),
        ("20250608", "Kurban Bayramı 2. Gün"),
        ("20250609", "Kurban Bayramı 3. Gün"),
        ("20250610", "Kurban Bayramı 4. Gün"),
        ("20250730", "Demokrasi ve Millî Birlik Günü"),
        ("20250830", "Zafer Bayramı"),
        ("20251029", "Cumhuriyet Bayramı"),
        
        ("20260101", "Yılbaşı"),
        ("20260320", "Ramazan Bayramı 1. Gün"),
        ("20260321", "Ramazan Bayramı 2. Gün"),
        ("20260322", "Ramazan Bayramı 3. Gün"),
        ("20260423", "Ulusal Egemenlik ve Çocuk Bayramı"),
        ("20260501", "Emek ve Dayanışma Günü"),
        ("20260519", "Atatürk'ü Anma, Gençlik ve Spor Bayramı"),
        ("20260527", "Kurban Bayramı 1. Gün"),
        ("20260528", "Kurban Bayramı 2. Gün"),
        ("20260529", "Kurban Bayramı 3. Gün"),
        ("20260530", "Kurban Bayramı 4. Gün"),
        ("20260730", "Demokrasi ve Millî Birlik Günü"),
        ("20260830", "Zafer Bayramı"),
        ("20261029", "Cumhuriyet Bayramı"),
    ]
    
    # Get all service IDs
    service_ids = list(calendar.keys())
    
    # Create exception entries for each holiday and service
    calendar_dates = []
    for service_id in service_ids:
        for date, description in turkish_holidays:
            calendar_dates.append({
                'service_id': service_id,
                'date': date,
                'exception_type': '2'  # 2 = service removed
            })
    
    _save_csv(
        out_dir,
        "calendar_dates.txt",
        calendar_dates,
        ['service_id', 'date', 'exception_type'],
        logger
    )


def save_all_files(scraper: Any) -> None:
    """Persist ``GTFSScraper`` state to ``scraper.output_dir``."""
    logger = scraper.logger
    od = scraper.output_dir
    logger.info("Writing GTFS files...")

    ensure_agencies_cover_routes(scraper.agencies, scraper.routes)
    ensure_stops_cover_stop_times(scraper.stops, scraper.stop_times)

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
        [
            "stop_id", "stop_code", "stop_name", "stop_desc", "stop_lat", 
            "stop_lon", "zone_id", "stop_url", "location_type", 
            "parent_station", "stop_timezone", "wheelchair_boarding", 
            "platform_code"
        ],
        logger,
    )
    _save_csv(
        od,
        "routes.txt",
        scraper.routes.values(),
        [
            "route_id", "agency_id", "route_short_name", "route_long_name", 
            "route_desc", "route_type", "route_url", "route_color", 
            "route_text_color", "route_sort_order"
        ],
        logger,
    )
    _save_csv(
        od,
        "trips.txt",
        scraper.trips.values(),
        [
            "route_id", "service_id", "trip_id", "trip_headsign", 
            "trip_short_name", "direction_id", "block_id", "shape_id",
            "wheelchair_accessible", "bikes_allowed"
        ],
        logger,
    )
    _save_csv(
        od,
        "stop_times.txt",
        scraper.stop_times,
        [
            "trip_id", "arrival_time", "departure_time", "stop_id", 
            "stop_sequence", "stop_headsign", "pickup_type", "drop_off_type",
            "shape_dist_traveled", "timepoint"
        ],
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
    
    # Create feed_info.txt
    _save_feed_info(od, logger, scraper)
    
    # Create calendar_dates.txt with Turkish holidays
    if scraper.calendar:
        _save_calendar_dates(od, scraper.calendar, logger)

    logger.info("GTFS files written")
    
    # Optionally write to PostgreSQL database
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        logger.info("Database URL found, loading data into PostgreSQL...")
        try:
            _write_to_database(od, database_url, logger)
        except Exception as e:
            logger.warning(f"Failed to write to database: {e}")
            logger.warning("CSV files are still available")


def _write_to_database(gtfs_dir: Path, database_url: str, logger: logging.Logger) -> None:
    """Write GTFS CSV files to PostgreSQL database."""
    try:
        from database import GTFSLoader
    except ImportError:
        logger.warning("Database module not available. Install with: pip install -e '.[db]'")
        return
    
    try:
        loader = GTFSLoader(gtfs_dir, database_url)
        results = loader.load_all()
        
        success_count = sum(1 for v in results.values() if v)
        total_count = len(results)
        
        if success_count == total_count:
            logger.info(f"✓ All GTFS data loaded into PostgreSQL ({success_count}/{total_count} files)")
        else:
            logger.warning(f"⚠ Partial database load ({success_count}/{total_count} files)")
            
    except Exception as e:
        logger.error(f"Database write failed: {e}")
        raise
