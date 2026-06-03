import asyncio
import logging
from datetime import datetime, timezone

import httpx

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("proxy")

BACKEND_URL = "http://localhost:20000"
TOKEN = "changeme"
UPDATE_INTERVAL = 60
POLL_INTERVAL = 0.5
POLL_TIMEOUT = 30

containers: dict[str, dict] = {}
port_by_host: dict[int, dict] = {}
listeners: dict[int, asyncio.Server] = {}
starting: set[str] = set()

_client: httpx.AsyncClient | None = None


def client() -> httpx.AsyncClient:
    global _client
    if _client is None:
        _client = httpx.AsyncClient(
            base_url=BACKEND_URL,
            headers={"Authorization": f"Bearer {TOKEN}"},
        )
    return _client


# ── Helpers ──────────────────────────────────────────────────────────

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_iso(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, TypeError):
        return None


# ── Backend HTTP helpers ─────────────────────────────────────────────

async def api_get(path: str) -> list | dict:
    r = await client().get(path)
    r.raise_for_status()
    return r.json()


async def api_post(path: str) -> dict:
    r = await client().post(path)
    r.raise_for_status()
    return r.json()


async def api_put(path: str, body: dict) -> dict:
    r = await client().put(path, json=body)
    r.raise_for_status()
    return r.json()


async def fetch_containers() -> list[dict]:
    return await api_get("/crud/containers")


async def fetch_ports(container_id: str) -> list[dict]:
    return await api_get(f"/crud/containers/{container_id}/ports")


# ── Wake-up ──────────────────────────────────────────────────────────

async def wake_container(docker_id: str, listening_port: int) -> bool:
    log.info("Waking container %s", docker_id)
    try:
        await api_post(f"/docker/containers/{docker_id}/start")
    except httpx.HTTPError as e:
        log.warning("Failed to start container %s: %s", docker_id, e)

    deadline = asyncio.get_event_loop().time() + POLL_TIMEOUT
    attempt = 0
    while asyncio.get_event_loop().time() < deadline:
        try:
            r, w = await asyncio.wait_for(
                asyncio.open_connection("127.0.0.1", listening_port),
                timeout=POLL_INTERVAL,
            )
            w.close()
            await w.wait_closed()
            log.info(
                "Port %d ready for %s after ~%.1fs",
                listening_port, docker_id, attempt * POLL_INTERVAL,
            )
            return True
        except (OSError, asyncio.TimeoutError):
            attempt += 1
            await asyncio.sleep(POLL_INTERVAL)

    log.error("Timed out waiting for container %s port %d", docker_id, listening_port)
    return False


async def wake_and_cleanup(container_id: str, docker_id: str, listening_port: int) -> None:
    ok = await wake_container(docker_id, listening_port)
    starting.discard(container_id)


# ── TCP proxy ────────────────────────────────────────────────────────

async def proxy_loop(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        while True:
            data = await reader.read(65536)
            if not data:
                break
            writer.write(data)
            await writer.drain()
    except (ConnectionResetError, BrokenPipeError, OSError):
        pass
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


async def handle_connection(
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
    host_port: int,
) -> None:
    addr = client_writer.get_extra_info("peername")
    entry = port_by_host.get(host_port)
    if entry is None:
        log.warning("No port mapping for host_port %d (from %s)", host_port, addr)
        client_writer.close()
        return

    c = entry["container"]
    mapping = entry["mapping"]
    container_id = c["id"]
    docker_id = c["container_id"]
    listening_port = mapping["listening_port"]
    auto_sleep = c.get("auto_sleep", 0)

    log.info("Connection from %s to host_port %d (container %s)", addr, host_port, docker_id)

    # ── auto_sleep == 0: proxy directly, no start/wait ─────────────
    if auto_sleep == 0:
        try:
            up_r, up_w = await asyncio.open_connection("127.0.0.1", listening_port)
        except OSError as e:
            log.error("Cannot connect to upstream port %d for %s: %s", listening_port, docker_id, e)
            client_writer.close()
            return

        await api_put(f"/crud/containers/{container_id}", {"last_accessed_at": now_iso()})
        t1 = asyncio.create_task(proxy_loop(client_reader, up_w))
        t2 = asyncio.create_task(proxy_loop(up_r, client_writer))
        done, _ = await asyncio.wait({t1, t2}, return_when=asyncio.FIRST_COMPLETED)
        for t in done:
            try:
                t.result()
            except Exception:
                pass
        for t in {t1, t2} - done:
            t.cancel()
        await api_put(f"/crud/containers/{container_id}", {"last_accessed_at": now_iso()})
        return

    # ── Wake up sleeping container ──────────────────────────────────
    await api_put(f"/crud/containers/{container_id}", {"last_accessed_at": now_iso()})

    if container_id not in starting:
        starting.add(container_id)
        asyncio.create_task(wake_and_cleanup(container_id, docker_id, listening_port))
    else:
        log.info("Container %s already being started, waiting...", docker_id)

    if not await poll_port(listening_port):
        log.error("Port %d not ready for %s, closing connection", listening_port, docker_id)
        client_writer.close()
        return

    try:
        up_r, up_w = await asyncio.open_connection("127.0.0.1", listening_port)
    except OSError as e:
        log.error("Cannot connect to upstream after wake for %s: %s", docker_id, e)
        client_writer.close()
        return

    t1 = asyncio.create_task(proxy_loop(client_reader, up_w))
    t2 = asyncio.create_task(proxy_loop(up_r, client_writer))
    done, _ = await asyncio.wait({t1, t2}, return_when=asyncio.FIRST_COMPLETED)
    for t in done:
        try:
            t.result()
        except Exception:
            pass
    for t in {t1, t2} - done:
        t.cancel()

    await api_put(f"/crud/containers/{container_id}", {"last_accessed_at": now_iso()})
    log.info("Connection closed for %s (host_port %d)", docker_id, host_port)


async def poll_port(port: int) -> bool:
    deadline = asyncio.get_event_loop().time() + POLL_TIMEOUT
    while asyncio.get_event_loop().time() < deadline:
        try:
            r, w = await asyncio.wait_for(
                asyncio.open_connection("127.0.0.1", port),
                timeout=POLL_INTERVAL,
            )
            w.close()
            await w.wait_closed()
            return True
        except (OSError, asyncio.TimeoutError):
            await asyncio.sleep(POLL_INTERVAL)
    return False


# ── Dynamic listener management ─────────────────────────────────────

async def start_listener(host_port: int) -> None:
    if host_port in listeners:
        return
    try:
        server = await asyncio.start_server(
            lambda r, w: handle_connection(r, w, host_port),
            host="0.0.0.0",
            port=host_port,
        )
        listeners[host_port] = server
        log.info("Started TCP listener on port %d", host_port)
    except OSError as e:
        log.error("Cannot listen on port %d: %s", host_port, e)


async def stop_listener(host_port: int) -> None:
    server = listeners.pop(host_port, None)
    if server is not None:
        server.close()
        await server.wait_closed()
        log.info("Stopped TCP listener on port %d", host_port)


def rebuild_port_index() -> None:
    port_by_host.clear()
    for c in containers.values():
        for m in c.get("_ports", []):
            port_by_host[m["host_port"]] = {"container": c, "mapping": m}


# ── Startup ──────────────────────────────────────────────────────────

async def startup() -> None:
    log.info("Fetching containers...")
    raw = await fetch_containers()
    containers.clear()
    for c in raw:
        c["_ports"] = await fetch_ports(c["id"])
        containers[c["id"]] = c
    rebuild_port_index()
    for hp in list(port_by_host.keys()):
        await start_listener(hp)
    log.info("Startup complete: %d containers, %d listeners", len(containers), len(listeners))


# ── Update ───────────────────────────────────────────────────────────

async def update() -> None:
    log.info("Running update cycle...")
    try:
        raw = await fetch_containers()
    except httpx.HTTPError as e:
        log.warning("Update: failed to fetch containers: %s", e)
        return

    containers.clear()
    for c in raw:
        try:
            c["_ports"] = await fetch_ports(c["id"])
        except httpx.HTTPError as e:
            log.warning("Update: failed to fetch ports for %s: %s", c["id"], e)
            c["_ports"] = []
        containers[c["id"]] = c

    new_host_ports = set()
    for c in containers.values():
        for m in c.get("_ports", []):
            new_host_ports.add(m["host_port"])
    old_host_ports = set(listeners.keys())

    rebuild_port_index()

    for hp in new_host_ports - old_host_ports:
        await start_listener(hp)
    for hp in old_host_ports - new_host_ports:
        await stop_listener(hp)

    now = datetime.now(timezone.utc)
    for c in containers.values():
        if c.get("auto_sleep", 0) != 1 or c.get("status") != "running":
            continue
        last_dt = parse_iso(c.get("last_accessed_at"))
        if last_dt is None:
            continue
        interval = c.get("sleep_interval_seconds", 300)
        if (now - last_dt).total_seconds() > interval:
            did = c["container_id"]
            log.info("Auto-stopping %s (idle > %ds)", did, interval)
            try:
                await api_post(f"/docker/containers/{did}/stop")
            except httpx.HTTPError as e:
                log.warning("Failed to auto-stop %s: %s", did, e)

    log.info("Update complete: %d containers, %d listeners", len(containers), len(listeners))


# ── Main ─────────────────────────────────────────────────────────────

async def main() -> None:
    await startup()
    while True:
        await asyncio.sleep(UPDATE_INTERVAL)
        await update()


if __name__ == "__main__":
    asyncio.run(main())
