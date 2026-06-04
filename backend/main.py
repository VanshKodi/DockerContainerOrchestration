from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers import CRUD, docker, public

# uv run uvicorn main:app --reload --port 20000

app = FastAPI(lifespan=CRUD.lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {"message": "Hello World"}

app.include_router(public.public_router, prefix="/public", tags=["Public"])
app.include_router(CRUD.crud_router, prefix="/crud", tags=["CRUD"])
app.include_router(docker.docker_router, prefix="/docker", tags=["Docker"])