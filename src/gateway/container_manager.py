"""On-demand Docker container management for OTP instances."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Callable

import docker
from docker.errors import NotFound, APIError

from .config import CityOTP, GatewayConfig

logger = logging.getLogger(__name__)


@dataclass
class ContainerState:
    """State of an OTP container."""
    
    city_id: str
    container_id: str | None = None
    status: str = "stopped"  # stopped, starting, running, stopping
    last_used: float = 0
    port: int = 0


class ContainerManager:
    """Manage on-demand OTP Docker containers."""
    
    def __init__(self, config: GatewayConfig):
        self.config = config
        self.client = docker.from_env()
        self.states: dict[str, ContainerState] = {}
        self.locks: dict[str, asyncio.Lock] = {}
        self._cleanup_task: asyncio.Task | None = None
        
        for city_id in config.cities:
            self.states[city_id] = ContainerState(city_id=city_id)
            self.locks[city_id] = asyncio.Lock()
    
    async def start(self) -> None:
        """Start the container manager background tasks."""
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        logger.info("Container manager started")
    
    async def stop(self) -> None:
        """Stop all managed containers."""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        
        for city_id in self.states:
            await self._stop_container(city_id)
        
        logger.info("Container manager stopped")
    
    async def ensure_running(self, city_id: str) -> tuple[bool, str, int]:
        """Ensure OTP container for city is running.
        
        Returns:
            Tuple of (success, message, port)
        """
        city = self.config.get_city(city_id)
        if not city:
            return False, f"Unknown city: {city_id}", 0
        
        if not city.graph_path.exists() or not (city.graph_path / "graph.obj").exists():
            return False, f"Graph not found for {city_id}. Run: gtfs-scraper build --city {city_id}", 0
        
        async with self.locks[city_id]:
            state = self.states[city_id]
            
            if state.status == "running":
                state.last_used = time.time()
                return True, "Already running", state.port
            
            if state.status == "starting":
                pass
            else:
                state.status = "starting"
                success = await self._start_container(city)
                
                if not success:
                    state.status = "stopped"
                    return False, f"Failed to start container for {city_id}", 0
            
            healthy = await self._wait_for_healthy(city)
            
            if healthy:
                state.status = "running"
                state.port = city.port
                state.last_used = time.time()
                return True, "Container started", state.port
            else:
                await self._stop_container(city_id)
                state.status = "stopped"
                return False, f"Container failed health check for {city_id}", 0
    
    async def _start_container(self, city: CityOTP) -> bool:
        """Start a Docker container for the city."""
        container_name = f"otp_{city.id}"
        
        try:
            existing = self.client.containers.get(container_name)
            if existing.status == "running":
                logger.info("Container %s already running", container_name)
                self.states[city.id].container_id = existing.id
                return True
            else:
                logger.info("Removing stopped container %s", container_name)
                existing.remove(force=True)
        except NotFound:
            pass
        
        logger.info("Starting container %s (port %d, memory %s)", container_name, city.port, city.memory)
        
        try:
            container = self.client.containers.run(
                self.config.otp_image,
                command=["--load", "--serve"],
                name=container_name,
                detach=True,
                remove=True,
                ports={"8080/tcp": city.port},
                volumes={
                    str(city.graph_path.resolve()): {
                        "bind": "/var/opentripplanner",
                        "mode": "ro",
                    }
                },
                environment={
                    "JAVA_TOOL_OPTIONS": f"-Xmx{city.memory}",
                },
            )
            
            self.states[city.id].container_id = container.id
            logger.info("Container %s started (id: %s)", container_name, container.short_id)
            return True
            
        except APIError as e:
            logger.error("Failed to start container %s: %s", container_name, e)
            return False
    
    async def _stop_container(self, city_id: str) -> None:
        """Stop container for a city."""
        state = self.states[city_id]
        container_name = f"otp_{city_id}"
        
        if state.status == "stopped":
            return
        
        state.status = "stopping"
        
        try:
            container = self.client.containers.get(container_name)
            logger.info("Stopping container %s", container_name)
            container.stop(timeout=10)
        except NotFound:
            pass
        except APIError as e:
            logger.error("Error stopping container %s: %s", container_name, e)
        
        state.status = "stopped"
        state.container_id = None
        state.port = 0
    
    async def _wait_for_healthy(self, city: CityOTP) -> bool:
        """Wait for OTP to become healthy."""
        import aiohttp
        
        url = f"http://localhost:{city.port}/otp/routers/default"
        start_time = time.time()
        
        logger.info("Waiting for %s to become healthy...", city.id)
        
        while time.time() - start_time < self.config.container_startup_timeout:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, timeout=5) as resp:
                        if resp.status == 200:
                            logger.info("%s is healthy", city.id)
                            return True
            except Exception:
                pass
            
            await asyncio.sleep(self.config.health_check_interval)
        
        logger.error("%s failed to become healthy within %ds", city.id, self.config.container_startup_timeout)
        return False
    
    async def _cleanup_loop(self) -> None:
        """Background task to stop idle containers."""
        while True:
            try:
                await asyncio.sleep(30)
                
                now = time.time()
                for city_id, state in self.states.items():
                    if state.status == "running":
                        idle_time = now - state.last_used
                        if idle_time > self.config.container_idle_timeout:
                            logger.info(
                                "Container %s idle for %ds, stopping",
                                city_id,
                                int(idle_time),
                            )
                            async with self.locks[city_id]:
                                await self._stop_container(city_id)
                                
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("Cleanup loop error: %s", e)
    
    def get_status(self) -> dict[str, dict]:
        """Get status of all containers."""
        return {
            city_id: {
                "status": state.status,
                "port": state.port,
                "last_used": state.last_used,
                "idle_seconds": int(time.time() - state.last_used) if state.last_used else 0,
            }
            for city_id, state in self.states.items()
        }
