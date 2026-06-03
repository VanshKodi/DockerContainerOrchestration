from fastapi import APIRouter, Depends, HTTPException
from utils import verify_token
from routers.CRUD import get_conn

docker_router = APIRouter(dependencies=[Depends(verify_token)])

DB_STATUS = {"start": "running", "stop": "stopped"}


def _update_status(container_id: str, action: str):
    with get_conn() as conn:
        row = conn.execute(
            "SELECT id FROM containers WHERE container_id = ?", (container_id,)
        ).fetchone()
        if row is None:
            raise HTTPException(404, f"Container {container_id} not found in database")
        conn.execute(
            "UPDATE containers SET status = ? WHERE id = ?",
            (DB_STATUS[action], row["id"]),
        )


@docker_router.post("/containers/{container_id}/start")
async def start_container(container_id: str):
    import docker

    client = docker.from_env()
    try:
        container = client.containers.get(container_id)
        container.start()
    except docker.errors.NotFound:
        raise HTTPException(404, f"Docker container {container_id} not found")
    except docker.errors.APIError as e:
        raise HTTPException(400, str(e))
    _update_status(container_id, "start")
    return {"message": f"Container {container_id} started"}


@docker_router.post("/containers/{container_id}/stop")
async def stop_container(container_id: str):
    import docker

    client = docker.from_env()
    try:
        container = client.containers.get(container_id)
        container.stop()
    except docker.errors.NotFound:
        raise HTTPException(404, f"Docker container {container_id} not found")
    except docker.errors.APIError as e:
        raise HTTPException(400, str(e))
    _update_status(container_id, "stop")
    return {"message": f"Container {container_id} stopped"}
