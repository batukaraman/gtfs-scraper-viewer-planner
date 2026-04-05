"""GTFS data backend: PostgreSQL if ``DATABASE_URL`` is set, otherwise CSV under ``gtfs_dir``.

The two modes are mutually exclusive — never mix DB trip data with on-disk shapes, etc.
"""

from __future__ import annotations

import hashlib
import os

from dotenv import load_dotenv

load_dotenv()


def database_url() -> str | None:
    """Return trimmed ``DATABASE_URL`` or ``None`` if unset / empty."""
    u = (os.getenv("DATABASE_URL") or "").strip()
    return u if u else None


def use_database() -> bool:
    return database_url() is not None


def otp_base_url() -> str | None:
    """Base URL for OpenTripPlanner (e.g. ``http://localhost:8080``). No trailing slash."""
    u = (os.getenv("OTP_BASE_URL") or "").strip().rstrip("/")
    return u if u else None


def otp_planner_available() -> bool:
    return otp_base_url() is not None


def database_url_fingerprint() -> str:
    """Short stable hash for Streamlit cache keys (does not log the secret)."""
    url = database_url()
    if not url:
        return ""
    return hashlib.sha256(url.encode("utf-8")).hexdigest()[:24]
