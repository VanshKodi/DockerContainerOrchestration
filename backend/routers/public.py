from fastapi import APIRouter
from routers.CRUD import get_conn, row_to_dict

public_router = APIRouter()


@public_router.get("/containers")
async def list_public_containers():
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM containers WHERE visibility = 'public' ORDER BY container_name"
        ).fetchall()
    return [row_to_dict(r) for r in rows]
