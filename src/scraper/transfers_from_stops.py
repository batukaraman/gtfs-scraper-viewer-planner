"""
Build GTFS transfers.txt from stop coordinates (walking transfers).

Used by ``scraper.export.save_all_files``. The trip planner only reads ``transfers.txt``;
it does not import this module.
"""

from __future__ import annotations

import csv
import math
from collections import defaultdict
from pathlib import Path
from typing import Any, DefaultDict, List, Mapping, Optional, Sequence, Set, Tuple, Union

EARTH_RADIUS_M = 6_371_000.0
M_PER_DEG_LAT = 111_000.0
TRANSFER_FIELDNAMES = ["from_stop_id", "to_stop_id", "transfer_type", "min_transfer_time"]


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in meters."""
    rlat1, rlon1, rlat2, rlon2 = map(math.radians, (lat1, lon1, lat2, lon2))
    dlat, dlon = rlat2 - rlat1, rlon2 - rlon1
    a = math.sin(dlat / 2) ** 2 + math.cos(rlat1) * math.cos(rlat2) * math.sin(dlon / 2) ** 2
    return 2 * EARTH_RADIUS_M * math.asin(min(1.0, math.sqrt(a)))


def build_transfer_rows(
    stops: Sequence[Mapping[str, Any]],
    max_distance_m: float = 400.0,
    walk_speed_mps: float = 1.3,
    min_walk_seconds: int = 60,
) -> List[dict]:
    """
    Directed walking transfer edges for stop pairs within max_distance_m.

    transfer_type 2 = minimum transfer time (GTFS). min_transfer_time in seconds.
    """
    rows: List[dict] = []
    seen: Set[Tuple[str, str]] = set()

    valid: List[Tuple[str, float, float]] = []
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
            loc = int(loc) if loc != "" and not (isinstance(loc, float) and math.isnan(loc)) else 0
        except (TypeError, ValueError):
            loc = 0
        # Skip station parent entries (no precise coordinate for routing); use child stops
        if loc == 1:
            continue
        valid.append((sid, lat, lon))

    if len(valid) < 2:
        return rows

    # Spatial hash grid (stdlib only).
    cell_deg = max(max_distance_m / M_PER_DEG_LAT, 1e-5)

    def cell_key(lat: float, lon: float) -> Tuple[int, int]:
        return (int(lat / cell_deg), int(lon / cell_deg))

    buckets: DefaultDict[Tuple[int, int], List[int]] = defaultdict(list)
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
                    rows.append(
                        {
                            "from_stop_id": from_id,
                            "to_stop_id": to_id,
                            "transfer_type": 2,
                            "min_transfer_time": mtt,
                        }
                    )

    return rows


def write_transfers_file(
    gtfs_dir: Union[str, Path],
    stops: Optional[Sequence[Mapping[str, Any]]] = None,
    *,
    max_distance_m: float = 400.0,
    walk_speed_mps: float = 1.3,
    stops_txt_path: Optional[Union[str, Path]] = None,
) -> Path:
    """
    Write {gtfs_dir}/transfers.txt.

    Provide either `stops` (in-memory records) or rely on stops_txt_path / gtfs_dir/stops.txt.
    """
    gtfs_dir = Path(gtfs_dir)
    gtfs_dir.mkdir(parents=True, exist_ok=True)
    out_path = gtfs_dir / "transfers.txt"

    if stops is None:
        src = Path(stops_txt_path) if stops_txt_path else gtfs_dir / "stops.txt"
        if not src.exists():
            raise FileNotFoundError(f"stops.txt not found: {src}")
        with open(src, newline="", encoding="utf-8") as f:
            stops = list(csv.DictReader(f))

    rows = build_transfer_rows(
        stops, max_distance_m=max_distance_m, walk_speed_mps=walk_speed_mps
    )

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=TRANSFER_FIELDNAMES, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)

    return out_path
