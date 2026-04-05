#!/usr/bin/env bash
# Build OTP graph into otp/data (run from repo root).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
mkdir -p "$ROOT/otp/data"
docker run --rm -e JAVA_TOOL_OPTIONS=-Xmx8g \
  -v "$ROOT/otp/data:/var/opentripplanner" \
  opentripplanner/opentripplanner:2.5.0 \
  --build --save
echo "Done. Start: docker compose --profile otp up -d otp"
