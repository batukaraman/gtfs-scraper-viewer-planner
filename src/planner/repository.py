"""GTFS data access — CSV today, database-friendly interface later."""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Protocol, runtime_checkable

import pandas as pd

from planner.calendar_filter import service_ids_for_date


class MissingTransfersError(FileNotFoundError):
    """Raised when {gtfs_dir}/transfers.txt is absent."""


@runtime_checkable
class TransitDataSource(Protocol):
    """Contract for GTFS-backed routing data (swap with SQL later)."""

    @property
    def gtfs_dir(self) -> Path: ...

    def load(self) -> None: ...

    def service_ids_on(self, on_date: dt.date) -> set[str]: ...

    @property
    def stops(self) -> pd.DataFrame: ...

    @property
    def routes(self) -> pd.DataFrame: ...

    @property
    def trips(self) -> pd.DataFrame: ...

    @property
    def stop_times(self) -> pd.DataFrame: ...

    @property
    def transfers(self) -> pd.DataFrame: ...

    @property
    def calendar(self) -> pd.DataFrame: ...

    @property
    def frequencies(self) -> pd.DataFrame: ...

    @property
    def shapes(self) -> pd.DataFrame: ...


class CsvGtfsRepository:
    """Load standard GTFS tables from a directory."""

    def __init__(self, gtfs_dir: str | Path):
        self._dir = Path(gtfs_dir)
        self._loaded = False
        self._agency = pd.DataFrame()
        self._stops = pd.DataFrame()
        self._routes = pd.DataFrame()
        self._trips = pd.DataFrame()
        self._stop_times = pd.DataFrame()
        self._transfers = pd.DataFrame()
        self._calendar = pd.DataFrame()
        self._frequencies = pd.DataFrame()
        self._shapes = pd.DataFrame()

    @property
    def gtfs_dir(self) -> Path:
        return self._dir

    def load(self) -> None:
        if self._loaded:
            return
        d = self._dir
        if not d.is_dir():
            raise FileNotFoundError(f"GTFS directory not found: {d}")

        transfers_path = d / "transfers.txt"
        if not transfers_path.is_file():
            raise MissingTransfersError(
                f"transfers.txt missing in {d}. Run the scraper so it saves the feed, or from "
                f"Python: from scraper.transfers_from_stops import write_transfers_file; "
                f"write_transfers_file({d!r})"
            )

        self._agency = pd.read_csv(d / "agency.txt")
        self._stops = pd.read_csv(d / "stops.txt")
        self._routes = pd.read_csv(d / "routes.txt")
        self._trips = pd.read_csv(d / "trips.txt")
        self._stop_times = pd.read_csv(d / "stop_times.txt")
        self._transfers = pd.read_csv(transfers_path)
        self._calendar = pd.read_csv(d / "calendar.txt")
        freq = d / "frequencies.txt"
        self._frequencies = pd.read_csv(freq) if freq.exists() else pd.DataFrame()
        self._shapes = pd.read_csv(d / "shapes.txt")
        self._loaded = True

    def service_ids_on(self, on_date: dt.date) -> set[str]:
        if not self._loaded:
            self.load()
        return service_ids_for_date(self._calendar, on_date)

    @property
    def stops(self) -> pd.DataFrame:
        return self._stops

    @property
    def routes(self) -> pd.DataFrame:
        return self._routes

    @property
    def trips(self) -> pd.DataFrame:
        return self._trips

    @property
    def stop_times(self) -> pd.DataFrame:
        return self._stop_times

    @property
    def transfers(self) -> pd.DataFrame:
        return self._transfers

    @property
    def calendar(self) -> pd.DataFrame:
        return self._calendar

    @property
    def frequencies(self) -> pd.DataFrame:
        return self._frequencies

    @property
    def shapes(self) -> pd.DataFrame:
        return self._shapes

    @property
    def agency(self) -> pd.DataFrame:
        return self._agency
