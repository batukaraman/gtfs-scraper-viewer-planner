#!/usr/bin/env python3
"""Validate and repair GTFS CSVs for OpenTripPlanner (referential integrity).

Run from repo root:
    python scripts/validate_and_fix_gtfs.py

Typical runtime: ~1–3 minutes for Istanbul-sized stop_times.txt (streaming).

Then zip for OTP:
    .\\scripts\\zip_gtfs_for_otp.ps1
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
    log = logging.getLogger("validate_gtfs")
    gtfs = ROOT / "gtfs"
    if not gtfs.is_dir():
        log.error("Klasor yok: %s", gtfs)
        sys.exit(1)
    report = fix_gtfs_directory(gtfs, log)
    print("\n--- ozet ---")
    for k, v in report.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
