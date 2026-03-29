"""Map a calendar date to active GTFS service_id values."""

from __future__ import annotations

import datetime as dt
from typing import Set

import pandas as pd


def _parse_yyyymmdd(s: str) -> dt.date:
    s = str(s).strip()
    return dt.datetime.strptime(s, "%Y%m%d").date()


def service_ids_for_date(calendar: pd.DataFrame, on_date: dt.date) -> Set[str]:
    """
    Return service_id values valid on `on_date` per calendar.txt rules.

    Expects columns: service_id, monday..sunday, start_date, end_date (GTFS ints or str YYYYMMDD).
    """
    if calendar.empty:
        return set()

    weekday = on_date.weekday()  # Monday=0
    day_cols = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
    col = day_cols[weekday]

    out: Set[str] = set()
    for _, row in calendar.iterrows():
        try:
            if int(row[col]) != 1:
                continue
        except (TypeError, ValueError, KeyError):
            continue
        start = row["start_date"]
        end = row["end_date"]
        if isinstance(start, (int, float)):
            start = f"{int(start):08d}"
        if isinstance(end, (int, float)):
            end = f"{int(end):08d}"
        try:
            sd = _parse_yyyymmdd(str(start))
            ed = _parse_yyyymmdd(str(end))
        except ValueError:
            continue
        if sd <= on_date <= ed:
            out.add(str(row["service_id"]))
    return out
