"""Run: ``python -m scraper`` (default: Istanbul → ``gtfs/``)."""

from __future__ import annotations

from .core import GTFSScraper


def main() -> None:
    cities = ["istanbul"]
    GTFSScraper(cities, output_dir="gtfs", logs_dir="logs").run()


if __name__ == "__main__":
    main()
