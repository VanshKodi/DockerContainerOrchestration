import sqlite3
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, Depends, FastAPI, HTTPException
from pydantic import BaseModel
from utils import verify_token

crud_router = APIRouter(dependencies=[Depends(verify_token)])

DB_DIR = Path(__file__).resolve().parent.parent.parent / ".userdata"
DB_PATH = DB_DIR / "app.db"


def get_conn(db_path: Path = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def init_db(db_path: Path = DB_PATH) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
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
                last_accessed_at       TEXT,
                domain                 TEXT
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
        try:
            conn.execute("ALTER TABLE containers ADD COLUMN domain TEXT")
        except sqlite3.OperationalError:
            pass
    print("[init_db] Database ready.")


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Starting up...")
    DB_DIR.mkdir(parents=True, exist_ok=True)
    init_db()
    yield


# ── Models ──────────────────────────────────────────────────────────

class ContainerCreate(BaseModel):
    container_id: str
    container_name: str
    description: str | None = None
    visibility: str = "private"
    auto_sleep: int = 0
    sleep_interval_seconds: int = 300
    status: str = "running"
    domain: str | None = None

class ContainerUpdate(BaseModel):
    container_name: str | None = None
    description: str | None = None
    visibility: str | None = None
    auto_sleep: int | None = None
    sleep_interval_seconds: int | None = None
    status: str | None = None
    last_accessed_at: str | None = None
    domain: str | None = None

class PortMappingCreate(BaseModel):
    listening_port: int
    host_port: int
    label: str | None = None

class PortMappingUpdate(BaseModel):
    listening_port: int | None = None
    host_port: int | None = None
    label: str | None = None


# ── Helpers ─────────────────────────────────────────────────────────

def row_to_dict(row: sqlite3.Row | None) -> dict | None:
    if row is None:
        return None
    return dict(row)


# ── Container CRUD ──────────────────────────────────────────────────

@crud_router.get("/containers")
async def list_containers():
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM containers ORDER BY container_name").fetchall()
    return [row_to_dict(r) for r in rows]


@crud_router.get("/containers/{container_id}")
async def get_container(container_id: str):
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM containers WHERE id = ?", (container_id,)).fetchone()
    if row is None:
        raise HTTPException(404, "Container not found")
    return row_to_dict(row)


@crud_router.post("/containers", status_code=201)
async def create_container(body: ContainerCreate):
    cid = str(uuid.uuid4())
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with get_conn() as conn:
        conn.execute("""INSERT INTO containers (id, container_id, container_name, description, visibility, auto_sleep, sleep_interval_seconds, status, last_accessed_at, domain) VALUES (?,?,?,?,?,?,?,?,?,?)""",
                     (cid, body.container_id, body.container_name, body.description, body.visibility, body.auto_sleep, body.sleep_interval_seconds, body.status, now, body.domain))
    return {"id": cid}


@crud_router.put("/containers/{container_id}")
async def update_container(container_id: str, body: ContainerUpdate):
    with get_conn() as conn:
        existing = conn.execute("SELECT * FROM containers WHERE id = ?", (container_id,)).fetchone()
        if existing is None:
            raise HTTPException(404, "Container not found")
        updates = {k: v for k, v in body.model_dump(exclude_unset=True).items() if v is not None}
        if not updates:
            return row_to_dict(existing)
        updates["id"] = container_id
        sets = ", ".join(f"{k} = :{k}" for k in updates)
        conn.execute(f"UPDATE containers SET {sets} WHERE id = :id", updates)
        row = conn.execute("SELECT * FROM containers WHERE id = ?", (container_id,)).fetchone()
    return row_to_dict(row)


@crud_router.delete("/containers/{container_id}", status_code=204)
async def delete_container(container_id: str):
    with get_conn() as conn:
        conn.execute("DELETE FROM containers WHERE id = ?", (container_id,))
    return None


# ── Port Mapping CRUD ──────────────────────────────────────────────

@crud_router.get("/containers/{container_id}/ports")
async def list_ports(container_id: str):
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM port_mappings WHERE container_id = ? ORDER BY host_port", (container_id,)).fetchall()
    return [row_to_dict(r) for r in rows]


@crud_router.get("/ports/{port_id}")
async def get_port(port_id: str):
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM port_mappings WHERE id = ?", (port_id,)).fetchone()
    if row is None:
        raise HTTPException(404, "Port mapping not found")
    return row_to_dict(row)


@crud_router.post("/containers/{container_id}/ports", status_code=201)
async def create_port(container_id: str, body: PortMappingCreate):
    pid = str(uuid.uuid4())
    with get_conn() as conn:
        container = conn.execute("SELECT id FROM containers WHERE id = ?", (container_id,)).fetchone()
        if container is None:
            raise HTTPException(404, "Container not found")
        conn.execute("""INSERT INTO port_mappings (id, container_id, listening_port, host_port, label) VALUES (?,?,?,?,?)""",
                     (pid, container_id, body.listening_port, body.host_port, body.label))
    return {"id": pid}


@crud_router.put("/ports/{port_id}")
async def update_port(port_id: str, body: PortMappingUpdate):
    with get_conn() as conn:
        existing = conn.execute("SELECT * FROM port_mappings WHERE id = ?", (port_id,)).fetchone()
        if existing is None:
            raise HTTPException(404, "Port mapping not found")
        updates = {k: v for k, v in body.model_dump(exclude_unset=True).items() if v is not None}
        if not updates:
            return row_to_dict(existing)
        updates["id"] = port_id
        sets = ", ".join(f"{k} = :{k}" for k in updates)
        conn.execute(f"UPDATE port_mappings SET {sets} WHERE id = :id", updates)
        row = conn.execute("SELECT * FROM port_mappings WHERE id = ?", (port_id,)).fetchone()
    return row_to_dict(row)


@crud_router.delete("/ports/{port_id}", status_code=204)
async def delete_port(port_id: str):
    with get_conn() as conn:
        conn.execute("DELETE FROM port_mappings WHERE id = ?", (port_id,))
    return None