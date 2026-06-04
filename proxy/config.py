"""Static configuration for the proxy.

Values are hardcoded to match the existing backend/proxy setup.
"""

# Backend API base URL (note trailing slash).
BACKEND_URL = "http://127.0.0.1:20000/"

# Shared internal auth token (same as backend/utils.py AUTH_TOKEN).
AUTH_TOKEN = "changeme"

# Default headers for authenticated backend calls.
HEADERS = {"Authorization": f"Bearer {AUTH_TOKEN}"}

# Host where containers publish their listening_port.
UPSTREAM_HOST = "127.0.0.1"

# Registry cache time-to-live, in seconds. Avoids hammering the backend
# under burst traffic while keeping data reasonably fresh.
CACHE_TTL = 5.0

# Readiness probe (TCP connect to localhost:listening_port).
PROBE_TIMEOUT = 30.0        # max total seconds to wait for a container
PROBE_BACKOFF_START = 0.25  # initial retry delay
PROBE_BACKOFF_MAX = 2.0     # cap on retry delay
PROBE_CONNECT_TIMEOUT = 1.0  # per-attempt TCP connect timeout

# Backend HTTP call timeout (seconds).
BACKEND_TIMEOUT = 10.0

# How often the server poller reconciles running servers with backend
# mappings (start new host_ports, stop removed ones).
SERVER_POLL_INTERVAL = 60.0

# How often the reaper checks for idle auto_sleep containers.
REAPER_INTERVAL = 60.0

# Hop-by-hop headers that must not be forwarded (RFC 7230 6.1).
HOP_BY_HOP_HEADERS = frozenset({
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
})
