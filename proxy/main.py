"""Entry point: orchestrates one uvicorn server per host_port.

Startup:
  - fetch all mappings, start a server task per host_port
Background:
  - server poller: reconcile running servers with backend mappings
  - reaper: stop idle auto_sleep containers

Each uvicorn.Server has signal handlers disabled (multiple servers would
otherwise fight over them); shutdown is driven from the top level.
"""

from __future__ import annotations

import asyncio
import contextlib
import signal
from datetime import datetime

import httpx
import uvicorn

import config
from registry import registry
from router import make_app


class ServerHandle:
    def __init__(self, host_port: int) -> None:
        self.host_port = host_port
        cfg = uvicorn.Config(
            app=make_app(host_port),
            host="0.0.0.0",
            port=host_port,
            log_level="info",
            ws="websockets",
        )
        self.server = uvicorn.Server(cfg)
        # Disable per-server signal handlers; main() owns shutdown.
        self.server.install_signal_handlers = lambda: None  # type: ignore[assignment]
        self.task: asyncio.Task | None = None

    def start(self) -> None:
        self.task = asyncio.create_task(self.server.serve())

    async def stop(self) -> None:
        self.server.should_exit = True
        if self.task is not None:
            with contextlib.suppress(asyncio.CancelledError):
                await self.task


servers: dict[int, ServerHandle] = {}


async def reconcile_servers() -> None:
    """Start servers for new host_ports, stop ones that disappeared."""
    mappings = await registry.get_all_mappings(force=True)
    wanted = set(mappings.keys())
    running = set(servers.keys())

    for host_port in wanted - running:
        handle = ServerHandle(host_port)
        servers[host_port] = handle
        handle.start()
        print(f"[main] started proxy on :{host_port} "
              f"-> {mappings[host_port].container_name}")

    for host_port in running - wanted:
        handle = servers.pop(host_port)
        await handle.stop()
        print(f"[main] stopped proxy on :{host_port}")


async def server_poller(stop: asyncio.Event) -> None:
    while not stop.is_set():
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=config.SERVER_POLL_INTERVAL)
        if stop.is_set():
            break
        try:
            await reconcile_servers()
        except httpx.HTTPError as e:
            print(f"[main] server poll failed: {e}")


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


async def reaper(stop: asyncio.Event) -> None:
    while not stop.is_set():
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=config.REAPER_INTERVAL)
        if stop.is_set():
            break
        try:
            await _reap_once()
        except httpx.HTTPError as e:
            print(f"[reaper] failed: {e}")


async def _reap_once() -> None:
    now = datetime.now()
    async with httpx.AsyncClient(timeout=config.BACKEND_TIMEOUT) as client:
        resp = await client.get(
            f"{config.BACKEND_URL}crud/containers", headers=config.HEADERS
        )
        resp.raise_for_status()
        for c in resp.json():
            if not c.get("auto_sleep"):
                continue
            if c.get("status") != "running":
                continue
            last = _parse_dt(c.get("last_accessed_at"))
            if last is None:
                continue
            idle = (now - last).total_seconds()
            if idle <= c.get("sleep_interval_seconds", 300):
                continue

            docker_id = c.get("container_id")
            db_id = c.get("id")
            name = c.get("container_name")
            print(f"[reaper] sleeping {name} (idle {idle:.0f}s)")
            try:
                stop_resp = await client.post(
                    f"{config.BACKEND_URL}docker/containers/{docker_id}/stop",
                    headers=config.HEADERS,
                )
                stop_resp.raise_for_status()
                # backend writes 'stopped'; override to 'sleeping' per schema.
                await client.put(
                    f"{config.BACKEND_URL}crud/containers/{db_id}",
                    headers=config.HEADERS,
                    json={"status": "sleeping"},
                )
                registry.invalidate()
            except httpx.HTTPError as e:
                print(f"[reaper] failed to sleep {name}: {e}")


async def _wait_for_backend() -> None:
    """Retry reconcile_servers until the backend is reachable."""
    delay = 1.0
    while True:
        try:
            await reconcile_servers()
            return
        except httpx.HTTPError as e:
            print(f"[main] backend not ready ({e}); retrying in {delay:.0f}s")
            await asyncio.sleep(delay)
            delay = min(delay * 2, 16.0)


async def run() -> None:
    # Wait for the backend to be reachable, then do the initial spin-up.
    await _wait_for_backend()

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _request_stop() -> None:
        print("[main] shutdown requested")
        stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, _request_stop)

    poller = asyncio.create_task(server_poller(stop))
    reaper_task = asyncio.create_task(reaper(stop))

    await stop.wait()

    poller.cancel()
    reaper_task.cancel()
    for task in (poller, reaper_task):
        with contextlib.suppress(asyncio.CancelledError):
            await task

    await asyncio.gather(*(h.stop() for h in list(servers.values())))
    print("[main] all servers stopped")


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass
    except Exception:
        import traceback
        traceback.print_exc()
        input("\nPress Enter to exit...")


if __name__ == "__main__":
    main()
