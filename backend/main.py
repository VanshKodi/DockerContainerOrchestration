from fastapi import FastAPI
from routers import CRUD

# uv run uvicorn main:app --reload --port 20000

app = FastAPI(lifespan=CRUD.lifespan)

@app.get("/")
async def root():
    return {"message": "to be or not to be"}

app.include_router(CRUD.crud_router, prefix="/crud", tags=["CRUD"])