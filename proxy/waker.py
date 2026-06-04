"""Waker: wake-on-demand container logic.

Wakes containers through the backend's /docker endpoints (the backend owns
Docker access and DB status). A per-container asyncio.Lock collapses bursts
of concurrent requests into a single wake call; waiters proceed once the
container passes a TCP readiness probe.

A shared httpx.AsyncClient is used for all backend calls. Access timestamp
updates are accumulated in memory and batch-flushed to the backend every
ACCESS_FLUSH_INTERVAL seconds to avoid per-request HTTP churn.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from datetime import datetime

import httpx

import config
from registry import Mapping, registry


class WakeError(Exception):
    """Raised when a container could not be woken or never became ready."""


# ── Shared client ──────────────────────────────────────────────────────

_client: httpx.AsyncClient | None = None


def get_client() -> httpx.AsyncClient:
    global _client
    if _client is None:
        _client = httpx.AsyncClient(timeout=config.BACKEND_TIMEOUT)
    return _client


async def close_client() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None


# ── Lock helpers ───────────────────────────────────────────────────────

_locks: dict[str, asyncio.Lock] = {}
_locks_guard = asyncio.Lock()


async def _lock_for(db_id: str) -> asyncio.Lock:
    async with _locks_guard:
        lock = _locks.get(db_id)
        if lock is None:
            lock = asyncio.Lock()
            _locks[db_id] = lock
        return lock


# ── Readiness probe ────────────────────────────────────────────────────

async def _probe(listening_port: int) -> bool:
    """One TCP connect attempt against the upstream port."""
    try:
        fut = asyncio.open_connection(config.UPSTREAM_HOST, listening_port)
        reader, writer = await asyncio.wait_for(fut, timeout=1.0)
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


# ── Docker actions via backend ─────────────────────────────────────────

async def _start_container(docker_container_id: str) -> None:
    client = get_client()
    resp = await client.post(
        f"{config.BACKEND_URL}docker/containers/{docker_container_id}/start",
        headers=config.HEADERS,
    )
    if resp.status_code >= 400:
        raise WakeError(
            f"backend failed to start {docker_container_id}: "
            f"{resp.status_code} {resp.text}"
        )


# ── Batched access tracker ─────────────────────────────────────────────

_touch_batch: dict[str, str] = {}
_touch_lock = asyncio.Lock()
_touch_task: asyncio.Task | None = None


async def touch_last_accessed(db_id: str) -> None:
    """Accumulate a touch event; flush to backend happens in the background."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    async with _touch_lock:
        _touch_batch[db_id] = now


async def _flush_loop() -> None:
    while True:
        await asyncio.sleep(config.ACCESS_FLUSH_INTERVAL)
        async with _touch_lock:
            batch = _touch_batch.copy()
            _touch_batch.clear()
        if not batch:
            continue
        client = get_client()
        for db_id, ts in batch.items():
            try:
                await client.put(
                    f"{config.BACKEND_URL}crud/containers/{db_id}",
                    headers=config.HEADERS,
                    json={"last_accessed_at": ts},
                )
            except httpx.HTTPError:
                pass


def start_tracker() -> None:
    global _touch_task
    if _touch_task is not None:
        return
    _touch_task = asyncio.create_task(_flush_loop())


async def stop_tracker() -> None:
    global _touch_task
    if _touch_task is None:
        return
    _touch_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await _touch_task
    _touch_task = None
    # Flush remaining on shutdown.
    async with _touch_lock:
        remaining = _touch_batch.copy()
        _touch_batch.clear()
    if remaining:
        client = get_client()
        for db_id, ts in remaining.items():
            try:
                await client.put(
                    f"{config.BACKEND_URL}crud/containers/{db_id}",
                    headers=config.HEADERS,
                    json={"last_accessed_at": ts},
                )
            except httpx.HTTPError:
                pass


# ── Public API ─────────────────────────────────────────────────────────

async def ensure_awake(mapping: Mapping) -> None:
    """Guarantee the container behind `mapping` is running and reachable."""
    # Fast path: already running — no probe, no lock, just go.
    if mapping.status == "running":
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
