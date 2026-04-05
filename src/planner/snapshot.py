"""Optional on-disk cache for :class:`RaptorContext` (gzip + pickle)."""

from __future__ import annotations

import gzip
import hashlib
import os
import pickle
import datetime as dt
from pathlib import Path
from typing import Any, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from planner.preprocess import RaptorContext

_SNAPSHOT_VERSION = 2


def raptor_cache_enabled() -> bool:
    return os.environ.get("GTFS_RAPTOR_CACHE", "").strip().lower() in ("1", "true", "yes", "on")


def raptor_cache_path(cache_key: str) -> Path:
    base = Path(os.environ.get("GTFS_RAPTOR_CACHE_DIR", ".cache/gtfs_raptor"))
    base.mkdir(parents=True, exist_ok=True)
    h = hashlib.sha256(cache_key.encode("utf-8")).hexdigest()[:32]
    return base / f"{h}.pkl.gz"


def try_load_raptor_snapshot(
    path: Path,
    repo: Any,
    on_date: dt.date,
) -> Optional["RaptorContext"]:
    if not raptor_cache_enabled() or not path.is_file():
        return None
    try:
        with gzip.open(path, "rb") as f:
            payload = pickle.load(f)
    except Exception:
        return None
    meta = payload.get("meta") or {}
    if int(meta.get("v", 0)) != _SNAPSHOT_VERSION:
        return None
    if meta.get("date") != on_date.isoformat():
        return None
    try:
        if int(meta.get("trips_rows", -1)) != len(repo.trips):
            return None
        if int(meta.get("stop_times_rows", -1)) != len(repo.stop_times):
            return None
    except (TypeError, ValueError):
        return None
    ctx = payload.get("ctx")
    return ctx if ctx is not None else None


def save_raptor_snapshot(path: Path, ctx: "RaptorContext", repo: Any, on_date: dt.date) -> None:
    if not raptor_cache_enabled():
        return
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "meta": {
                "v": _SNAPSHOT_VERSION,
                "date": on_date.isoformat(),
                "trips_rows": len(repo.trips),
                "stop_times_rows": len(repo.stop_times),
            },
            "ctx": ctx,
        }
        with gzip.open(path, "wb", compresslevel=3) as f:
            pickle.dump(payload, f, protocol=pickle.HIGHEST_PROTOCOL)
    except Exception:
        pass
