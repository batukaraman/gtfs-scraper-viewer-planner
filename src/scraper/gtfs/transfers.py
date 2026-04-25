"""Build walking transfers between nearby stops."""

from __future__ import annotations

import math
from collections import defaultdict
from typing import Any

EARTH_RADIUS_M = 6_371_000.0
M_PER_DEG_LAT = 111_000.0


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate great-circle distance in meters."""
    rlat1, rlon1, rlat2, rlon2 = map(math.radians, (lat1, lon1, lat2, lon2))
    dlat, dlon = rlat2 - rlat1, rlon2 - rlon1
    a = math.sin(dlat / 2) ** 2 + math.cos(rlat1) * math.cos(rlat2) * math.sin(dlon / 2) ** 2
    return 2 * EARTH_RADIUS_M * math.asin(min(1.0, math.sqrt(a)))


def build_transfers(
    stops: list[dict[str, Any]],
    max_distance_m: float = 400.0,
    walk_speed_mps: float = 1.3,
    min_walk_seconds: int = 60,
) -> list[dict[str, Any]]:
    """Build walking transfer edges for stop pairs within max_distance_m.
    
    Args:
        stops: List of stop dictionaries with stop_id, stop_lat, stop_lon
        max_distance_m: Maximum walking distance in meters
        walk_speed_mps: Walking speed in meters per second
        min_walk_seconds: Minimum transfer time in seconds
    
    Returns:
        List of transfer dictionaries for transfers.txt
    """
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()

    valid: list[tuple[str, float, float]] = []
    for s in stops:
        sid = str(s.get("stop_id", ""))
        if not sid:
            continue
        try:
            lat = float(s["stop_lat"])
            lon = float(s["stop_lon"])
        except (KeyError, TypeError, ValueError):
            continue
        if math.isnan(lat) or math.isnan(lon):
            continue

        loc = s.get("location_type", 0)
        try:
            loc = int(loc) if loc not in ("", None) and not (isinstance(loc, float) and math.isnan(loc)) else 0
        except (TypeError, ValueError):
            loc = 0

        if loc == 1:
            continue

        valid.append((sid, lat, lon))

    if len(valid) < 2:
        return rows

    cell_deg = max(max_distance_m / M_PER_DEG_LAT, 1e-5)

    def cell_key(lat: float, lon: float) -> tuple[int, int]:
        return (int(lat / cell_deg), int(lon / cell_deg))

    buckets: defaultdict[tuple[int, int], list[int]] = defaultdict(list)
    for idx, (_, lat, lon) in enumerate(valid):
        buckets[cell_key(lat, lon)].append(idx)

    for i, (from_id, lat1, lon1) in enumerate(valid):
        ci, cj = cell_key(lat1, lon1)
        for di in (-1, 0, 1):
            for dj in (-1, 0, 1):
                for j in buckets.get((ci + di, cj + dj), []):
                    if i == j:
                        continue
                    to_id, lat2, lon2 = valid[j]
                    key = (from_id, to_id)
                    if key in seen:
                        continue
                    seen.add(key)

                    dist_m = _haversine_m(lat1, lon1, lat2, lon2)
                    if dist_m > max_distance_m + 1e-6:
                        continue

                    walk_sec = int(math.ceil(dist_m / walk_speed_mps))
                    mtt = max(min_walk_seconds, walk_sec)

                    rows.append({
                        "from_stop_id": from_id,
                        "to_stop_id": to_id,
                        "transfer_type": 2,
                        "min_transfer_time": mtt,
                    })

    return rows
