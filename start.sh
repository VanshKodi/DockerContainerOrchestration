#!/bin/bash
set -e

# ── Configuration ────────────────────────────────────────────────────
BACKEND_PORT=${BACKEND_PORT:-20000}
FRONTEND_PORT=${FRONTEND_PORT:-20001}
PROXY_PORTS=${PROXY_PORTS:-""}          # optional space-separated list for EXPOSE docs

# ── Helpers ──────────────────────────────────────────────────────────
cleanup() {
    echo ""
    echo "[start.sh] Shutting down services…"
    kill $BACKEND_PID $FRONTEND_PID $PROXY_PID 2>/dev/null || true
    wait $BACKEND_PID $FRONTEND_PID $PROXY_PID 2>/dev/null || true
    echo "[start.sh] All services stopped."
    exit 0
}
trap cleanup SIGINT SIGTERM

# ── Start Backend (:20000) ───────────────────────────────────────────
echo "[start.sh] Starting backend on 0.0.0.0:${BACKEND_PORT}…"
cd /app/backend
uv run uvicorn main:app --host 0.0.0.0 --port "$BACKEND_PORT" --log-level info &
BACKEND_PID=$!

# Wait for backend to accept connections
for i in $(seq 1 15); do
    if uv run python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:${BACKEND_PORT}/')" 2>/dev/null; then
        echo "[start.sh] Backend ready."
        break
    fi
    if [ "$i" -eq 15 ]; then
        echo "[start.sh] WARNING: Backend did not respond within 15s — continuing anyway."
    fi
    sleep 1
done

# ── Serve Frontend (:20001) ──────────────────────────────────────────
echo "[start.sh] Serving frontend on 0.0.0.0:${FRONTEND_PORT}…"
cd /app/frontend
python3 -m http.server "$FRONTEND_PORT" --bind 0.0.0.0 --directory /app/frontend/dist &
FRONTEND_PID=$!

# ── Start Proxy (dynamic ports) ─────────────────────────────────────
echo "[start.sh] Starting proxy…"
cd /app/proxy
UPSTREAM_HOST="${UPSTREAM_HOST:-127.0.0.1}" uv run python main.py &
PROXY_PID=$!

echo ""
echo "════════════════════════════════════════════════════════════"
echo "  Backend  →  http://127.0.0.1:${BACKEND_PORT}"
echo "  Frontend →  http://127.0.0.1:${FRONTEND_PORT}"
echo "  Proxy    →  (binds port_mappings from the database)"
echo "════════════════════════════════════════════════════════════"
echo ""
echo "Services running. Send SIGTERM/SIGINT to stop."
echo ""

wait
