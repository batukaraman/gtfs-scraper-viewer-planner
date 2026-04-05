#!/usr/bin/env python3
"""Backward-compatible entry: full GTFS integrity fix (not only missing stops).

Run from repo root:
    python scripts/ensure_gtfs_stops_for_stop_times.py
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from scraper.gtfs_integrity import fix_gtfs_directory  # noqa: E402


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    log = logging.getLogger("gtfs_fix")
    fix_gtfs_directory(ROOT / "gtfs", log)


if __name__ == "__main__":
    main()
