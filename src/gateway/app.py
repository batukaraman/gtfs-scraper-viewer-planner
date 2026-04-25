"""FastAPI Transit Gateway Application."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Any

import aiohttp
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from .config import GatewayConfig
from .container_manager import ContainerManager

logger = logging.getLogger(__name__)


class Location(BaseModel):
    """Geographic location."""
    
    lat: float = Field(..., ge=-90, le=90, description="Latitude")
    lon: float = Field(..., ge=-180, le=180, description="Longitude")


class PlanRequest(BaseModel):
    """Trip planning request."""
    
    origin: Location = Field(..., alias="from", description="Origin location")
    destination: Location = Field(..., alias="to", description="Destination location")
    date: str | None = Field(None, description="Date (YYYY-MM-DD)")
    time: str | None = Field(None, description="Time (HH:MM)")
    arrive_by: bool = Field(False, description="If true, time is arrival time")
    mode: str = Field("TRANSIT,WALK", description="Transport modes")
    num_itineraries: int = Field(5, ge=1, le=10, description="Number of itineraries")
    
    class Config:
        populate_by_name = True


class PlanResponse(BaseModel):
    """Trip planning response."""
    
    city: str
    request_time_ms: int
    data: dict[str, Any]


class StatusResponse(BaseModel):
    """Gateway status response."""
    
    status: str
    cities: dict[str, dict]
    timestamp: str


config: GatewayConfig | None = None
container_manager: ContainerManager | None = None
plan_cache: dict[str, tuple[float, dict[str, Any]]] = {}


def get_config_path() -> Path:
    """Get config path from environment or default."""
    import os
    
    config_path = os.environ.get("GATEWAY_CONFIG")
    if config_path:
        return Path(config_path)
    return Path(__file__).parent.parent.parent / "config" / "cities.yaml"


def get_data_dir() -> Path:
    """Get data directory from environment or default."""
    import os
    
    data_dir = os.environ.get("GATEWAY_DATA_DIR")
    if data_dir:
        return Path(data_dir)
    return Path(__file__).parent.parent.parent / "data"


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    global config, container_manager
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    )
    
    config_path = get_config_path()
    data_dir = get_data_dir()
    
    logger.info("Loading config from %s", config_path)
    logger.info("Data directory: %s", data_dir)
    
    config = GatewayConfig.load(config_path, data_dir)
    container_manager = ContainerManager(config)
    
    await container_manager.start()
    
    logger.info("Gateway started with %d cities", len(config.cities))
    
    yield
    
    await container_manager.stop()
    logger.info("Gateway stopped")


app = FastAPI(
    title="Transit API Gateway",
    description="Unified routing API with on-demand OTP management",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "ok"}


@app.get("/status", response_model=StatusResponse)
async def get_status():
    """Get gateway and container status."""
    return StatusResponse(
        status="ok",
        cities=container_manager.get_status(),
        timestamp=datetime.utcnow().isoformat(),
    )


@app.get("/cities")
async def list_cities():
    """List available cities."""
    return {
        city_id: {
            "name": city.name,
            "tier": city.tier,
            "bbox": {
                "min_lon": city.bbox[0],
                "min_lat": city.bbox[1],
                "max_lon": city.bbox[2],
                "max_lat": city.bbox[3],
            },
            "has_graph": (city.graph_path / "graph.obj").exists(),
        }
        for city_id, city in config.cities.items()
    }


@app.post("/api/plan", response_model=PlanResponse)
async def plan_trip(
    request: PlanRequest,
    city: str | None = Query(None, description="City ID (auto-detected if not provided)"),
    prefer_fresh: bool = Query(False, description="Bypass cache and force fresh OTP query"),
):
    """Plan a transit trip.
    
    The city is automatically detected from coordinates, or can be specified explicitly.
    OTP containers are started on-demand and stopped after idle timeout.
    """
    import time
    
    start_time = time.time()
    
    if city:
        city_config = config.get_city(city)
        if not city_config:
            raise HTTPException(status_code=404, detail=f"Unknown city: {city}")
    else:
        city_config = config.find_city_by_coordinates(
            request.origin.lat, request.origin.lon
        )
        if not city_config:
            city_config = config.find_city_by_coordinates(
                request.destination.lat, request.destination.lon
            )
        
        if not city_config:
            raise HTTPException(
                status_code=400,
                detail="Could not determine city from coordinates. "
                       "Please specify city parameter or ensure coordinates are within a configured city's bbox.",
            )
    
    cache_key = _build_cache_key(city_config.id, request)
    cached = _cache_get(cache_key)
    if cached and not prefer_fresh:
        request_time_ms = int((time.time() - start_time) * 1000)
        return PlanResponse(
            city=city_config.id,
            request_time_ms=request_time_ms,
            data=cached,
        )

    success, message, port = await container_manager.ensure_running(city_config.id)
    
    if not success:
        raise HTTPException(status_code=503, detail=message)
    
    otp_response = await _forward_to_otp(port, request)
    _cache_put(cache_key, otp_response)
    
    request_time_ms = int((time.time() - start_time) * 1000)
    
    return PlanResponse(
        city=city_config.id,
        request_time_ms=request_time_ms,
        data=otp_response,
    )


@app.post("/cities/{city_id}/warmup")
async def warmup_city(city_id: str):
    """Warm up city OTP container in advance."""
    city = config.get_city(city_id)
    if city is None:
        raise HTTPException(status_code=404, detail=f"Unknown city: {city_id}")

    success, message, port = await container_manager.ensure_running(city_id)
    if not success:
        raise HTTPException(status_code=503, detail=message)

    return {
        "status": "ready",
        "city": city_id,
        "port": port,
        "tier": city.tier,
        "message": message,
    }


async def _forward_to_otp(port: int, request: PlanRequest) -> dict:
    """Forward request to OTP and return response."""
    
    date = request.date or datetime.now().strftime("%Y-%m-%d")
    time_str = request.time or datetime.now().strftime("%H:%M")
    
    graphql_query = """
    query plan(
        $fromLat: Float!, $fromLon: Float!,
        $toLat: Float!, $toLon: Float!,
        $date: String!, $time: String!,
        $arriveBy: Boolean!, $numItineraries: Int!,
        $modes: [TransportMode!]
    ) {
        plan(
            from: {lat: $fromLat, lon: $fromLon}
            to: {lat: $toLat, lon: $toLon}
            date: $date
            time: $time
            arriveBy: $arriveBy
            numItineraries: $numItineraries
            transportModes: $modes
        ) {
            itineraries {
                startTime
                endTime
                duration
                walkTime
                waitingTime
                walkDistance
                legs {
                    mode
                    startTime
                    endTime
                    duration
                    distance
                    from {
                        name
                        lat
                        lon
                        departureTime
                    }
                    to {
                        name
                        lat
                        lon
                        arrivalTime
                    }
                    route {
                        shortName
                        longName
                        color
                    }
                    legGeometry {
                        points
                    }
                }
            }
        }
    }
    """
    
    modes = []
    for mode in request.mode.split(","):
        mode = mode.strip().upper()
        if mode == "TRANSIT":
            modes.extend([
                {"mode": "BUS"},
                {"mode": "RAIL"},
                {"mode": "SUBWAY"},
                {"mode": "TRAM"},
                {"mode": "FERRY"},
            ])
        elif mode == "WALK":
            modes.append({"mode": "WALK"})
        elif mode == "BICYCLE":
            modes.append({"mode": "BICYCLE"})
        else:
            modes.append({"mode": mode})
    
    variables = {
        "fromLat": request.origin.lat,
        "fromLon": request.origin.lon,
        "toLat": request.destination.lat,
        "toLon": request.destination.lon,
        "date": date,
        "time": time_str,
        "arriveBy": request.arrive_by,
        "numItineraries": request.num_itineraries,
        "modes": modes,
    }
    
    url = f"http://localhost:{port}/otp/routers/default/index/graphql"
    
    async with aiohttp.ClientSession() as session:
        async with session.post(
            url,
            json={"query": graphql_query, "variables": variables},
            timeout=30,
        ) as resp:
            if resp.status != 200:
                text = await resp.text()
                raise HTTPException(
                    status_code=502,
                    detail=f"OTP returned error: {text}",
                )
            return await resp.json()


def main():
    """Run the gateway server."""
    import uvicorn
    import os
    
    host = os.environ.get("GATEWAY_HOST", "0.0.0.0")
    port = int(os.environ.get("GATEWAY_PORT", "8000"))
    
    uvicorn.run(app, host=host, port=port)


def _build_cache_key(city_id: str, request: PlanRequest) -> str:
    """Build deterministic cache key with rounded coordinates."""
    date = request.date or datetime.now().strftime("%Y-%m-%d")
    time_str = request.time or datetime.now().strftime("%H:%M")
    return (
        f"{city_id}|"
        f"{request.origin.lat:.5f},{request.origin.lon:.5f}|"
        f"{request.destination.lat:.5f},{request.destination.lon:.5f}|"
        f"{date}|{time_str}|{request.arrive_by}|{request.mode}|{request.num_itineraries}"
    )


def _cache_get(key: str) -> dict[str, Any] | None:
    item = plan_cache.get(key)
    if item is None:
        return None
    expires_at, payload = item
    if expires_at <= datetime.now().timestamp():
        plan_cache.pop(key, None)
        return None
    return payload


def _cache_put(key: str, payload: dict[str, Any]) -> None:
    ttl = config.plan_cache_ttl_seconds
    expires_at = datetime.now().timestamp() + max(ttl, 1)
    plan_cache[key] = (expires_at, payload)


if __name__ == "__main__":
    main()
