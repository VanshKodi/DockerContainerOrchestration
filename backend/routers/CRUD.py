import sqlite3
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, FastAPI

crud_router = APIRouter()

DB_DIR = Path(__file__).resolve().parent.parent.parent / ".userdata"
DB_PATH = DB_DIR / "app.db"


def get_conn(db_path: Path = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def init_db(db_path: Path = DB_PATH) -> None:
    db_path.unlink(missing_ok=True)
    with get_conn(db_path) as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS containers (
                id                     TEXT PRIMARY KEY,
                container_id           TEXT NOT NULL UNIQUE,
                container_name         TEXT NOT NULL,
                description            TEXT,
                visibility             TEXT NOT NULL DEFAULT 'private'
                                           CHECK(visibility IN ('public','private','internal')),
                auto_sleep             INTEGER NOT NULL DEFAULT 0,
                sleep_interval_seconds INTEGER NOT NULL DEFAULT 300,
                status                 TEXT NOT NULL DEFAULT 'running'
                                           CHECK(status IN ('running','stopped','sleeping')),
                last_accessed_at       TEXT
            );

            CREATE TABLE IF NOT EXISTS port_mappings (
                id             TEXT PRIMARY KEY,
                container_id   TEXT NOT NULL
                                   REFERENCES containers(id)
                                   ON DELETE CASCADE,
                listening_port INTEGER NOT NULL,
                host_port      INTEGER NOT NULL UNIQUE,
                label          TEXT
            );
        """)
    print("[init_db] Database ready.")


def seed_db(db_path: Path = DB_PATH) -> None:
    with get_conn(db_path) as conn:
        if conn.execute("SELECT COUNT(*) FROM containers").fetchone()[0] > 0:
            print("[seed_db] Seed data already present — skipping.")
            return

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        containers = [
            {"id": str(uuid.uuid4()), "container_id": "a1b2c3d4e5f6", "container_name": "nginx-frontend", "description": "Public-facing Nginx serving the React SPA", "visibility": "public", "auto_sleep": 0, "sleep_interval_seconds": 300, "status": "running", "last_accessed_at": now},
            {"id": str(uuid.uuid4()), "container_id": "b2c3d4e5f6a1", "container_name": "api-server", "description": "FastAPI backend — REST + metrics endpoint", "visibility": "internal", "auto_sleep": 1, "sleep_interval_seconds": 600, "status": "running", "last_accessed_at": now},
            {"id": str(uuid.uuid4()), "container_id": "c3d4e5f6a1b2", "container_name": "postgres-db", "description": "PostgreSQL 16 — primary database", "visibility": "private", "auto_sleep": 0, "sleep_interval_seconds": 0, "status": "running", "last_accessed_at": now},
            {"id": str(uuid.uuid4()), "container_id": "d4e5f6a1b2c3", "container_name": "redis-cache", "description": "Redis for session storage and job queues", "visibility": "private", "auto_sleep": 1, "sleep_interval_seconds": 1800, "status": "sleeping", "last_accessed_at": now},
        ]

        conn.executemany("""INSERT INTO containers (id, container_id, container_name, description, visibility, auto_sleep, sleep_interval_seconds, status, last_accessed_at) VALUES (:id, :container_id, :container_name, :description, :visibility, :auto_sleep, :sleep_interval_seconds, :status, :last_accessed_at)""", containers)

        def cid(name):
            return next(c["id"] for c in containers if c["container_name"] == name)

        port_mappings = [
            {"id": str(uuid.uuid4()), "container_id": cid("nginx-frontend"), "listening_port": 80, "host_port": 8080, "label": "web"},
            {"id": str(uuid.uuid4()), "container_id": cid("api-server"), "listening_port": 8000, "host_port": 8000, "label": "api"},
            {"id": str(uuid.uuid4()), "container_id": cid("api-server"), "listening_port": 9090, "host_port": 9090, "label": "metrics"},
            {"id": str(uuid.uuid4()), "container_id": cid("postgres-db"), "listening_port": 5432, "host_port": 5432, "label": "postgres"},
            {"id": str(uuid.uuid4()), "container_id": cid("redis-cache"), "listening_port": 6379, "host_port": 6379, "label": "redis"},
            {"id": str(uuid.uuid4()), "container_id": cid("redis-cache"), "listening_port": 26379, "host_port": 26379, "label": "sentinel"},
        ]

        conn.executemany("""INSERT INTO port_mappings (id, container_id, listening_port, host_port, label) VALUES (:id, :container_id, :listening_port, :host_port, :label)""", port_mappings)

    print(f"[seed_db] Inserted {len(containers)} containers and {len(port_mappings)} port mappings.")


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Starting up...")
    DB_DIR.mkdir(parents=True, exist_ok=True)
    init_db()
    seed_db()
    yield


@crud_router.get("/")
async def root():
    return {"message": "crud"}