"""Minimal OpenTripPlanner 2.x GraphQL client (plan query + helpers)."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import requests

# OTP 2.x default router GraphQL endpoint
DEFAULT_GRAPHQL_PATH = "/otp/routers/default/index/graphql"

_PLAN_QUERY = """
query PlanTrip(
  $fromLat: Float!
  $fromLon: Float!
  $toLat: Float!
  $toLon: Float!
  $date: String!
  $time: String!
  $numItineraries: Int!
) {
  plan(
    from: { lat: $fromLat, lon: $fromLon }
    to: { lat: $toLat, lon: $toLon }
    date: $date
    time: $time
    transportModes: [{ mode: TRANSIT }, { mode: WALK }]
    numItineraries: $numItineraries
  ) {
    itineraries {
      duration
      walkTime
      startTime
      endTime
      legs {
        mode
        startTime
        endTime
        distance
        from { name lat lon }
        to { name lat lon }
        route { shortName longName }
        trip { gtfsId }
        legGeometry { points }
      }
    }
  }
}
"""


def graphql_url(base_url: str) -> str:
    return f"{base_url.rstrip('/')}{DEFAULT_GRAPHQL_PATH}"


def decode_polyline(encoded: str) -> List[Tuple[float, float]]:
    """Decode Google-encoded polyline to (lat, lon) list (1e-5 deg steps)."""
    if not encoded or not isinstance(encoded, str):
        return []
    coordinates: List[Tuple[float, float]] = []
    index = 0
    lat = 0
    lng = 0
    length = len(encoded)
    while index < length:
        result = 0
        shift = 0
        while index < length:
            b = ord(encoded[index]) - 63
            index += 1
            result |= (b & 0x1F) << shift
            shift += 5
            if b < 0x20:
                break
        dlat = ~(result >> 1) if (result & 1) else (result >> 1)
        lat += dlat

        result = 0
        shift = 0
        while index < length:
            b = ord(encoded[index]) - 63
            index += 1
            result |= (b & 0x1F) << shift
            shift += 5
            if b < 0x20:
                break
        dlng = ~(result >> 1) if (result & 1) else (result >> 1)
        lng += dlng
        coordinates.append((lat * 1e-5, lng * 1e-5))
    return coordinates


def _ms_to_hhmmss(ms: Optional[int]) -> str:
    if ms is None:
        return "?"
    sec = int(ms // 1000) % 86400
    h, r = divmod(sec, 3600)
    m, s = divmod(r, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


@dataclass
class OtpLegView:
    mode: str
    start_label: str
    end_label: str
    start_ms: Optional[int]
    end_ms: Optional[int]
    route_name: str
    line: List[Tuple[float, float]] = field(default_factory=list)


@dataclass
class OtpItineraryView:
    duration_sec: int
    walk_time_sec: int
    start_ms: Optional[int]
    end_ms: Optional[int]
    legs: List[OtpLegView]


def _parse_itineraries(data: Optional[Dict[str, Any]]) -> List[OtpItineraryView]:
    if not data:
        return []
    plan = data.get("plan")
    if not plan:
        return []
    raw = plan.get("itineraries") or []
    out: List[OtpItineraryView] = []
    for it in raw:
        legs_out: List[OtpLegView] = []
        for leg in it.get("legs") or []:
            fr = leg.get("from") or {}
            to = leg.get("to") or {}
            geom = leg.get("legGeometry") or {}
            pts = geom.get("points")
            line = decode_polyline(pts) if isinstance(pts, str) else []
            if not line:
                try:
                    la0, lo0 = float(fr.get("lat")), float(fr.get("lon"))
                    la1, lo1 = float(to.get("lat")), float(to.get("lon"))
                    line = [(la0, lo0), (la1, lo1)]
                except (TypeError, ValueError):
                    line = []
            route = leg.get("route") or {}
            rn = str(route.get("shortName") or route.get("longName") or "").strip()
            legs_out.append(
                OtpLegView(
                    mode=str(leg.get("mode") or "?").upper(),
                    start_label=str(fr.get("name") or "—"),
                    end_label=str(to.get("name") or "—"),
                    start_ms=leg.get("startTime"),
                    end_ms=leg.get("endTime"),
                    route_name=rn,
                    line=line,
                )
            )
        out.append(
            OtpItineraryView(
                duration_sec=int(it.get("duration") or 0),
                walk_time_sec=int(it.get("walkTime") or 0),
                start_ms=it.get("startTime"),
                end_ms=it.get("endTime"),
                legs=legs_out,
            )
        )
    return out


def fetch_plan(
    base_url: str,
    *,
    from_lat: float,
    from_lon: float,
    to_lat: float,
    to_lon: float,
    depart: dt.datetime,
    num_itineraries: int = 5,
    timeout_sec: float = 90.0,
) -> Tuple[Optional[Dict[str, Any]], List[OtpItineraryView], Optional[str]]:
    """
    Call OTP GraphQL ``plan``.

    Returns (raw_data, parsed_views, error_message).
    """
    url = graphql_url(base_url)
    variables = {
        "fromLat": float(from_lat),
        "fromLon": float(from_lon),
        "toLat": float(to_lat),
        "toLon": float(to_lon),
        "date": depart.strftime("%Y-%m-%d"),
        "time": depart.strftime("%H:%M"),
        "numItineraries": max(1, min(int(num_itineraries), 10)),
    }
    try:
        r = requests.post(
            url,
            json={"query": _PLAN_QUERY, "variables": variables},
            timeout=timeout_sec,
            headers={"Content-Type": "application/json", "OTPTimeout": "120000"},
        )
        r.raise_for_status()
        body = r.json()
    except requests.RequestException as e:
        return None, [], str(e)
    if not isinstance(body, dict):
        return None, [], "Invalid JSON response"
    errs = body.get("errors")
    if errs:
        msg = "; ".join(str(e.get("message", e)) for e in errs if isinstance(e, dict))
        return body.get("data"), _parse_itineraries(body.get("data")), msg or "GraphQL errors"
    data = body.get("data")
    return data, _parse_itineraries(data), None


def format_itinerary_summary(iv: OtpItineraryView, index: int) -> str:
    dur_m = max(0, iv.duration_sec) // 60
    walk_m = max(0, iv.walk_time_sec) // 60
    return (
        f"Seçenek {index + 1}: ~{dur_m} dk yolculuk · ~{walk_m} dk yürüyüş · "
        f"{_ms_to_hhmmss(iv.start_ms)} → {_ms_to_hhmmss(iv.end_ms)}"
    )
