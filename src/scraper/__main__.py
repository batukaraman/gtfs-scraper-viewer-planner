"""CLI entry point for the GTFS scraper pipeline."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from .config import load_config
from .pipeline import Pipeline


def setup_logging(verbose: bool = False, log_file: Path | None = None) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    format_str = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"

    handlers = [logging.StreamHandler()]
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))

    logging.basicConfig(level=level, format=format_str, handlers=handlers)


def cmd_list(args: argparse.Namespace) -> int:
    """List configured cities."""
    pipeline = Pipeline(config_path=args.config)
    cities = pipeline.list_cities()

    print(f"\nConfigured cities ({len(cities)}):\n")
    print(f"{'ID':<15} {'Name':<20} {'Country':<10} {'Sources':<15} {'OTP Port':<10}")
    print("-" * 70)
    for city in cities:
        sources = ", ".join(city["sources"])
        print(f"{city['id']:<15} {city['name']:<20} {city['country']:<10} {sources:<15} {city['otp_port']:<10}")

    return 0


def cmd_scrape(args: argparse.Namespace) -> int:
    """Scrape GTFS data."""
    pipeline = Pipeline(config_path=args.config, base_dir=args.base_dir)

    if args.all:
        results = pipeline.process_all(
            skip_osm=args.skip_osm,
            skip_gtfs=False,
            force=args.force,
        )
        failed = [r for r in results if "error" in r]
        if failed:
            for f in failed:
                print(f"Failed: {f['city']} - {f['error']}")
            return 1
    elif args.city:
        pipeline.process_city(
            args.city,
            skip_osm=args.skip_osm,
            skip_gtfs=False,
            force=args.force,
        )
    else:
        print("Error: Specify --city or --all")
        return 1

    return 0


def cmd_build(args: argparse.Namespace) -> int:
    """Build OTP graph."""
    pipeline = Pipeline(config_path=args.config, base_dir=args.base_dir)

    if args.all:
        success = True
        for city in pipeline.config.all_cities():
            try:
                if not pipeline.build_otp_graph(city.id, memory=args.memory):
                    success = False
            except Exception as e:
                print(f"Failed to build {city.id}: {e}")
                success = False
        return 0 if success else 1
    elif args.city:
        success = pipeline.build_otp_graph(args.city, memory=args.memory)
        return 0 if success else 1
    else:
        print("Error: Specify --city or --all")
        return 1


def cmd_full(args: argparse.Namespace) -> int:
    """Run full pipeline: scrape + build."""
    pipeline = Pipeline(config_path=args.config, base_dir=args.base_dir)

    cities = pipeline.config.all_cities() if args.all else [pipeline.config.get_city(args.city)]
    cities = [c for c in cities if c is not None]

    if not cities:
        print("Error: No cities to process")
        return 1

    for city in cities:
        try:
            print(f"\n{'='*60}")
            print(f"Processing: {city.name}")
            print(f"{'='*60}")

            pipeline.process_city(
                city.id,
                skip_osm=args.skip_osm,
                force=args.force,
            )

            if not args.skip_build:
                pipeline.build_otp_graph(city.id, memory=args.memory)

        except Exception as e:
            print(f"Failed: {city.id} - {e}")
            if not args.all:
                return 1

    return 0


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        prog="gtfs-scraper",
        description="GTFS Pipeline - Multi-city transit data collection and OTP integration",
    )
    parser.add_argument(
        "-c", "--config",
        type=Path,
        help="Path to cities.yaml config file",
    )
    parser.add_argument(
        "-d", "--base-dir",
        type=Path,
        default=Path.cwd(),
        help="Base directory for data/otp folders",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        help="Write logs to file",
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    list_parser = subparsers.add_parser("list", help="List configured cities")

    scrape_parser = subparsers.add_parser("scrape", help="Scrape GTFS data")
    scrape_parser.add_argument("--city", help="City ID to process")
    scrape_parser.add_argument("--all", action="store_true", help="Process all cities")
    scrape_parser.add_argument("--skip-osm", action="store_true", help="Skip OSM download/extract")
    scrape_parser.add_argument("--force", action="store_true", help="Overwrite existing files")

    build_parser = subparsers.add_parser("build", help="Build OTP graph")
    build_parser.add_argument("--city", help="City ID to build")
    build_parser.add_argument("--all", action="store_true", help="Build all cities")
    build_parser.add_argument("--memory", help="JVM heap size (e.g., 8g)")

    full_parser = subparsers.add_parser("full", help="Run full pipeline (scrape + build)")
    full_parser.add_argument("--city", help="City ID to process")
    full_parser.add_argument("--all", action="store_true", help="Process all cities")
    full_parser.add_argument("--skip-osm", action="store_true", help="Skip OSM download/extract")
    full_parser.add_argument("--skip-build", action="store_true", help="Skip OTP graph build")
    full_parser.add_argument("--memory", help="JVM heap size (e.g., 8g)")
    full_parser.add_argument("--force", action="store_true", help="Overwrite existing files")

    args = parser.parse_args()

    setup_logging(verbose=args.verbose, log_file=args.log_file)

    if args.command is None:
        parser.print_help()
        return 0

    commands = {
        "list": cmd_list,
        "scrape": cmd_scrape,
        "build": cmd_build,
        "full": cmd_full,
    }

    return commands[args.command](args)


if __name__ == "__main__":
    sys.exit(main())
