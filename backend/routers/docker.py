from fastapi import APIRouter, Depends, HTTPException
from utils import verify_token

docker_router = APIRouter(dependencies=[Depends(verify_token)])


@docker_router.post("/containers/{container_id}/start")
async def start_container(container_id: str):
    import docker
    client = docker.from_env()
    try:
        container = client.containers.get(container_id)
        container.start()
        return {"message": f"Container {container_id} started"}
    except docker.errors.NotFound:
        raise HTTPException(404, f"Container {container_id} not found")
    except docker.errors.APIError as e:
        raise HTTPException(400, str(e))


@docker_router.post("/containers/{container_id}/stop")
async def stop_container(container_id: str):
    import docker
    client = docker.from_env()
    try:
        container = client.containers.get(container_id)
        container.stop()
        return {"message": f"Container {container_id} stopped"}
    except docker.errors.NotFound:
        raise HTTPException(404, f"Container {container_id} not found")
    except docker.errors.APIError as e:
        raise HTTPException(400, str(e))
