"""Router: builds a Starlette app for a single host_port.

Each app binds one host_port and forwards traffic to the owning container at
http://127.0.0.1:{listening_port}. HTTP is forwarded with httpx (streamed
both ways); WebSockets bridge a server-side Starlette socket to an upstream
client socket from the `websockets` library.
"""

from __future__ import annotations

import asyncio
import contextlib

import httpx
import websockets
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import PlainTextResponse, StreamingResponse
from starlette.routing import Route, WebSocketRoute
from starlette.websockets import WebSocket, WebSocketDisconnect
from websockets.exceptions import ConnectionClosed

import config
from registry import Mapping, registry
from waker import WakeError, ensure_awake, touch_last_accessed


def _filter_request_headers(request: Request) -> list[tuple[bytes, bytes]]:
    headers: list[tuple[bytes, bytes]] = []
    for name, value in request.headers.raw:
        if name.decode("latin-1").lower() in config.HOP_BY_HOP_HEADERS:
            continue
        headers.append((name, value))
    return headers


def _filter_response_headers(resp: httpx.Response) -> list[tuple[bytes, bytes]]:
    headers: list[tuple[bytes, bytes]] = []
    for name, value in resp.headers.raw:
        if name.decode("latin-1").lower() in config.HOP_BY_HOP_HEADERS:
            continue
        headers.append((name, value))
    return headers


def make_app(host_port: int) -> Starlette:
    # One shared client per app; reuses connection pool to the upstream.
    client = httpx.AsyncClient(timeout=None)

    async def _resolve(host_port: int) -> Mapping:
        mapping = await registry.get_mapping(host_port)
        if mapping is None:
            raise LookupError(f"no container mapped to host_port {host_port}")
        await ensure_awake(mapping)
        return mapping

    async def http_handler(request: Request) -> StreamingResponse:
        try:
            mapping = await _resolve(host_port)
        except LookupError as e:
            return PlainTextResponse(str(e), status_code=502)
        except WakeError as e:
            return PlainTextResponse(f"upstream not ready: {e}", status_code=504)

        path = request.url.path
        query = request.url.query
        upstream_url = httpx.URL(
            f"http://{config.UPSTREAM_HOST}:{mapping.listening_port}{path}"
        )
        if query:
            upstream_url = upstream_url.copy_with(query=query.encode("ascii"))

        headers = _filter_request_headers(request)
        client_host = request.client.host if request.client else ""
        headers.append((b"x-forwarded-for", client_host.encode("latin-1")))
        headers.append(
            (b"x-forwarded-host", request.headers.get("host", "").encode("latin-1"))
        )
        headers.append((b"x-forwarded-proto", request.url.scheme.encode("latin-1")))

        # Buffer body so we can retry on connection error.
        try:
            body = await request.body()
        except Exception:
            body = b""

        async def _forward(content: bytes = body) -> httpx.Response | None:
            try:
                req = client.build_request(
                    method=request.method,
                    url=upstream_url,
                    headers=headers,
                    content=content,
                )
                return await client.send(req, stream=True)
            except httpx.HTTPError:
                return None

        upstream_resp = await _forward()
        if upstream_resp is None:
            # Connection failed — container may have crashed or was
            # manually stopped. Refresh registry and attempt recovery.
            registry.invalidate()
            retry_mapping = await registry.get_mapping(host_port)
            if retry_mapping:
                try:
                    await ensure_awake(retry_mapping, force=True)
                    upstream_resp = await _forward()
                except WakeError:
                    pass
                if upstream_resp is not None:
                    await touch_last_accessed(retry_mapping.db_id)
                    return StreamingResponse(
                        upstream_resp.aiter_raw(),
                        status_code=upstream_resp.status_code,
                        headers=dict(
                            (k.decode("latin-1"), v.decode("latin-1"))
                            for k, v in _filter_response_headers(upstream_resp)
                        ),
                    )
            return PlainTextResponse("upstream error: connection refused", status_code=502)

        # Bump access timestamp (in-memory, batch-flushed to backend).
        await touch_last_accessed(mapping.db_id)

        return StreamingResponse(
            upstream_resp.aiter_raw(),
            status_code=upstream_resp.status_code,
            headers=dict(
                (k.decode("latin-1"), v.decode("latin-1"))
                for k, v in _filter_response_headers(upstream_resp)
            ),
        )

    async def ws_handler(ws: WebSocket) -> None:
        try:
            mapping = await _resolve(host_port)
        except (LookupError, WakeError) as e:
            await ws.close(code=1011, reason=str(e)[:120])
            return

        path = ws.url.path
        query = ws.url.query
        upstream_uri = f"ws://{config.UPSTREAM_HOST}:{mapping.listening_port}{path}"
        if query:
            upstream_uri += f"?{query}"

        subprotocols = ws.scope.get("subprotocols") or None

        _ws_upstream = None
        try:
            _ws_upstream = await websockets.connect(
                upstream_uri,
                subprotocols=subprotocols,
                open_timeout=config.PROBE_TIMEOUT,
                max_size=None,
            )
        except Exception as e:
            # Connection failed — container may have crashed. Recovery
            # attempt similar to HTTP handler.
            registry.invalidate()
            retry_mapping = await registry.get_mapping(host_port)
            if retry_mapping:
                try:
                    await ensure_awake(retry_mapping, force=True)
                    _ws_upstream = await websockets.connect(
                        upstream_uri,
                        subprotocols=subprotocols,
                        open_timeout=config.PROBE_TIMEOUT,
                        max_size=None,
                    )
                except Exception:
                    pass
            if _ws_upstream is None:
                await ws.close(code=1011, reason=f"upstream ws failed: {e}"[:120])
                return
            mapping = retry_mapping

        upstream = _ws_upstream

        await ws.accept(
            subprotocol=upstream.subprotocol if upstream.subprotocol else None
        )

        async def client_to_upstream() -> None:
            try:
                while True:
                    msg = await ws.receive()
                    if msg["type"] == "websocket.disconnect":
                        break
                    if (text := msg.get("text")) is not None:
                        await upstream.send(text)
                    elif (data := msg.get("bytes")) is not None:
                        await upstream.send(data)
            except (WebSocketDisconnect, ConnectionClosed):
                pass

        async def upstream_to_client() -> None:
            try:
                async for message in upstream:
                    if isinstance(message, str):
                        await ws.send_text(message)
                    else:
                        await ws.send_bytes(message)
            except (ConnectionClosed, WebSocketDisconnect):
                pass

        c2u = asyncio.create_task(client_to_upstream())
        u2c = asyncio.create_task(upstream_to_client())
        done, pending = await asyncio.wait(
            {c2u, u2c}, return_when=asyncio.FIRST_COMPLETED
        )
        for task in pending:
            task.cancel()
        await upstream.close()
        try:
            await ws.close()
        except RuntimeError:
            pass
        await touch_last_accessed(mapping.db_id)

    @contextlib.asynccontextmanager
    async def lifespan(_app: Starlette):
        yield
        await client.aclose()

    app = Starlette(
        routes=[
            WebSocketRoute("/{path:path}", ws_handler),
            Route(
                "/{path:path}",
                http_handler,
                methods=[
                    "GET", "POST", "PUT", "PATCH", "DELETE",
                    "HEAD", "OPTIONS", "TRACE",
                ],
            ),
        ],
        lifespan=lifespan,
    )
    return app
