#!/usr/bin/env bash
# Extract Istanbul metro bbox from turkey-latest.osm.pbf using Docker (osmium).
# Run from repo root: ./scripts/extract_istanbul_osm_for_otp.sh
# Optional: ARCHIVE_TURKEY=1 to rename turkey pbf to .archived (OTP loads all .pbf in folder)

set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA="$ROOT/otp/data"
INPUT="${INPUT:-turkey-latest.osm.pbf}"
OUTPUT="${OUTPUT:-istanbul.osm.pbf}"
IMAGE="${DOCKER_IMAGE:-debian:bookworm-slim}"
# min_lon,min_lat,max_lon,max_lat
BBOX="${ISTANBUL_OSM_BBOX:-27.90,40.72,30.12,41.58}"

if [[ ! -f "$DATA/$INPUT" ]]; then
  echo "Missing: $DATA/$INPUT — download Geofabrik turkey-latest.osm.pbf into otp/data or use download_turkey script" >&2
  exit 1
fi

mkdir -p "$DATA"
docker pull "$IMAGE"
echo "Extracting $INPUT -> $OUTPUT (bbox $BBOX) via $IMAGE (first run installs osmium-tool via apt, ~1–2 min)"
docker run --rm -v "$DATA:/data" "$IMAGE" bash -c \
  "apt-get update -qq && apt-get install -y -qq osmium-tool && osmium extract -b \"$BBOX\" --strategy complete_ways --overwrite \"/data/$INPUT\" -o \"/data/$OUTPUT\""

if [[ ! -f "$DATA/$OUTPUT" ]]; then
  echo "Output missing: $DATA/$OUTPUT" >&2
  exit 1
fi

ls -lh "$DATA/$OUTPUT"

if [[ "${ARCHIVE_TURKEY:-0}" == "1" ]] && [[ -f "$DATA/$INPUT" ]]; then
  mv -f "$DATA/$INPUT" "$DATA/${INPUT}.archived"
  echo "Archived: $DATA/${INPUT}.archived"
else
  echo "Note: If both $INPUT and $OUTPUT exist, OTP may load BOTH. Set ARCHIVE_TURKEY=1 to archive turkey." >&2
fi
