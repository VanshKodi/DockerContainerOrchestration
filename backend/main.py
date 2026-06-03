from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers import CRUD
from utils import verify_token

# uv run uvicorn main:app --reload --port 20000

app = FastAPI(lifespan=CRUD.lifespan, dependencies=[Depends(verify_token)])

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {"message": "to be or not to be"}

app.include_router(CRUD.crud_router, prefix="/crud", tags=["CRUD"])