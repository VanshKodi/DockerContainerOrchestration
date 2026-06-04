"""Registry: queries the backend and builds a host_port -> mapping index.

The backend stores port_mappings as listening_port (inside container) +
host_port (on host). For routing we need the inverse: given the host_port a
request arrived on, find which container owns it and how to reach it.

Each mapping carries BOTH container identifiers because the backend's two
APIs key on different things:
  - db_id               -> UUID, used for /crud/containers/{id}
  - docker_container_id -> Docker ID, used for /docker/containers/{id}/start|stop
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass

import httpx

import config


@dataclass(frozen=True)
class Mapping:
    host_port: int
    listening_port: int
    db_id: str
    docker_container_id: str
    container_name: str
    auto_sleep: int
    sleep_interval_seconds: int
    status: str


class Registry:
    def __init__(self) -> None:
        self._cache: dict[int, Mapping] = {}
        self._cache_expiry: float = 0.0
        self._lock = asyncio.Lock()

    async def _fetch_all(self) -> dict[int, Mapping]:
        """Fetch all containers + their ports and build the host_port index."""
        mappings: dict[int, Mapping] = {}
        async with httpx.AsyncClient(timeout=config.BACKEND_TIMEOUT) as client:
            resp = await client.get(
                f"{config.BACKEND_URL}crud/containers", headers=config.HEADERS
            )
            resp.raise_for_status()
            containers = resp.json()

            async def load_ports(container: dict) -> list[Mapping]:
                db_id = container.get("id")
                ports_resp = await client.get(
                    f"{config.BACKEND_URL}crud/containers/{db_id}/ports",
                    headers=config.HEADERS,
                )
                ports_resp.raise_for_status()
                result: list[Mapping] = []
                for p in ports_resp.json():
                    result.append(
                        Mapping(
                            host_port=p["host_port"],
                            listening_port=p["listening_port"],
                            db_id=db_id,
                            docker_container_id=container.get("container_id"),
                            container_name=container.get("container_name"),
                            auto_sleep=container.get("auto_sleep", 0),
                            sleep_interval_seconds=container.get(
                                "sleep_interval_seconds", 300
                            ),
                            status=container.get("status", "stopped"),
                        )
                    )
                return result

            all_ports = await asyncio.gather(
                *(load_ports(c) for c in containers)
            )

        seen_listening: dict[int, str] = {}
        for port_list in all_ports:
            for m in port_list:
                if m.host_port in mappings:
                    print(
                        f"[registry] WARNING: duplicate host_port {m.host_port} "
                        f"({m.container_name}); keeping first."
                    )
                    continue
                if m.listening_port in seen_listening:
                    print(
                        f"[registry] WARNING: listening_port {m.listening_port} "
                        f"used by both '{seen_listening[m.listening_port]}' and "
                        f"'{m.container_name}'; localhost upstream will collide."
                    )
                seen_listening[m.listening_port] = m.container_name
                mappings[m.host_port] = m
        return mappings

    async def _refresh(self) -> None:
        async with self._lock:
            if time.monotonic() < self._cache_expiry:
                return  # another coroutine refreshed while we waited
            self._cache = await self._fetch_all()
            self._cache_expiry = time.monotonic() + config.CACHE_TTL

    async def get_all_mappings(self, *, force: bool = False) -> dict[int, Mapping]:
        """Return the full host_port -> Mapping index (cached)."""
        if force or time.monotonic() >= self._cache_expiry:
            await self._refresh()
        return dict(self._cache)

    async def get_mapping(self, host_port: int) -> Mapping | None:
        """Return the mapping for a single host_port, refreshing on miss."""
        if time.monotonic() >= self._cache_expiry or host_port not in self._cache:
            await self._refresh()
        return self._cache.get(host_port)

    def invalidate(self) -> None:
        """Force the next lookup to re-fetch from the backend."""
        self._cache_expiry = 0.0


# Module-level singleton shared by all per-port apps.
registry = Registry()
