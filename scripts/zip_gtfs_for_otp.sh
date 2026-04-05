#!/usr/bin/env bash
# Zip repo gtfs/*.txt -> otp/data/gtfs.zip (flat GTFS root). Run from repo root.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
mkdir -p "$ROOT/otp/data"
(cd "$ROOT/gtfs" && zip -q -r "$ROOT/otp/data/gtfs.zip" . -i "*.txt")
echo "Written: $ROOT/otp/data/gtfs.zip"
