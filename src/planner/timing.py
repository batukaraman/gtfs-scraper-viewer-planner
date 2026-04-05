"""Optional phase timing for the Streamlit planner (stdout, no extra deps)."""

from __future__ import annotations

import os
import time
from contextlib import contextmanager
from typing import Iterator


def timing_enabled() -> bool:
    v = os.environ.get("GTFS_PLANNER_TIMING", "").strip().lower()
    return v in ("1", "true", "yes", "on")


def log_phase(label: str, elapsed_ms: float) -> None:
    if timing_enabled():
        print(f"[planner.timing] {label}: {elapsed_ms:.2f} ms")


@contextmanager
def timed_phase(label: str) -> Iterator[None]:
    t0 = time.perf_counter()
    try:
        yield
    finally:
        log_phase(label, (time.perf_counter() - t0) * 1000)
