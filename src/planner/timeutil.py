"""GTFS HH:MM:SS to seconds (service day; hours may exceed 24)."""

from __future__ import annotations

import numpy as np
import pandas as pd


def gtfs_time_to_seconds(t: str) -> int:
    parts = str(t).strip().split(":")
    h = int(parts[0])
    m = int(parts[1]) if len(parts) > 1 else 0
    s = int(parts[2]) if len(parts) > 2 else 0
    return h * 3600 + m * 60 + s


def seconds_to_gtfs_time(sec: int) -> str:
    sec = max(0, sec)
    h = sec // 3600
    m = (sec % 3600) // 60
    s = sec % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def gtfs_series_to_seconds(ser: pd.Series) -> np.ndarray:
    """Vectorized HH:MM:SS → seconds (handles >24h hours). For large stop_times columns."""
    s = ser.astype(str).str.strip()
    exp = s.str.split(":", expand=True)
    n = len(ser)
    h = pd.to_numeric(exp[0], errors="coerce").fillna(0).astype(np.int64).to_numpy(dtype=np.int64)
    if exp.shape[1] > 1:
        m = pd.to_numeric(exp[1], errors="coerce").fillna(0).astype(np.int64).to_numpy(dtype=np.int64)
    else:
        m = np.zeros(n, dtype=np.int64)
    if exp.shape[1] > 2:
        sec = pd.to_numeric(exp[2], errors="coerce").fillna(0).astype(np.int64).to_numpy(dtype=np.int64)
    else:
        sec = np.zeros(n, dtype=np.int64)
    return h * 3600 + m * 60 + sec
