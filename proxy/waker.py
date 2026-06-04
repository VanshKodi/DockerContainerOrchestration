"""Waker: wake-on-demand container logic.

Wakes containers through the backend's /docker endpoints (the backend owns
Docker access and DB status). A per-container asyncio.Lock collapses bursts
of concurrent requests into a single wake call; waiters proceed once the
container passes a TCP readiness probe.
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime

import httpx

import config
from registry import Mapping, registry


class WakeError(Exception):
    """Raised when a container could not be woken or never became ready."""


_locks: dict[str, asyncio.Lock] = {}
_locks_guard = asyncio.Lock()


async def _lock_for(db_id: str) -> asyncio.Lock:
    async with _locks_guard:
        lock = _locks.get(db_id)
        if lock is None:
            lock = asyncio.Lock()
            _locks[db_id] = lock
        return lock


async def _probe(listening_port: int) -> bool:
    """One TCP connect attempt against the upstream port."""
    try:
        fut = asyncio.open_connection(config.UPSTREAM_HOST, listening_port)
        reader, writer = await asyncio.wait_for(
            fut, timeout=config.PROBE_CONNECT_TIMEOUT
        )
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
        return True
    except (OSError, asyncio.TimeoutError):
        return False


async def _wait_ready(listening_port: int) -> None:
    deadline = time.monotonic() + config.PROBE_TIMEOUT
    delay = config.PROBE_BACKOFF_START
    while True:
        if await _probe(listening_port):
            return
        if time.monotonic() >= deadline:
            raise WakeError(
                f"upstream localhost:{listening_port} not ready within "
                f"{config.PROBE_TIMEOUT}s"
            )
        await asyncio.sleep(delay)
        delay = min(delay * 2, config.PROBE_BACKOFF_MAX)


async def _start_container(docker_container_id: str) -> None:
    async with httpx.AsyncClient(timeout=config.BACKEND_TIMEOUT) as client:
        resp = await client.post(
            f"{config.BACKEND_URL}docker/containers/{docker_container_id}/start",
            headers=config.HEADERS,
        )
        if resp.status_code >= 400:
            raise WakeError(
                f"backend failed to start {docker_container_id}: "
                f"{resp.status_code} {resp.text}"
            )


async def touch_last_accessed(db_id: str) -> None:
    """Update last_accessed_at on the backend. Fire-and-forget friendly."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        async with httpx.AsyncClient(timeout=config.BACKEND_TIMEOUT) as client:
            await client.put(
                f"{config.BACKEND_URL}crud/containers/{db_id}",
                headers=config.HEADERS,
                json={"last_accessed_at": now},
            )
    except httpx.HTTPError as e:
        print(f"[waker] failed to update last_accessed_at for {db_id}: {e}")


async def ensure_awake(mapping: Mapping) -> None:
    """Guarantee the container behind `mapping` is running and reachable."""
    # Fast path: already running -> just confirm it's reachable.
    if mapping.status == "running" and await _probe(mapping.listening_port):
        return

    lock = await _lock_for(mapping.db_id)
    async with lock:
        # Re-check under lock; another request may have woken it already.
        current = await registry.get_mapping(mapping.host_port)
        status = current.status if current else mapping.status

        if status != "running":
            print(f"[waker] waking {mapping.container_name} ({status})")
            await _start_container(mapping.docker_container_id)
            await touch_last_accessed(mapping.db_id)
            registry.invalidate()

        await _wait_ready(mapping.listening_port)
        print(f"[waker] {mapping.container_name} ready on {mapping.listening_port}")
