from clickhouse_con.save_data import SaveData
from clickhouse_con.filedata import FileData
from contextlib import asynccontextmanager

from fastapi import FastAPI
import uvicorn

from api.endpoints.post_data import data_router


@asynccontextmanager    
async def lifespan(app: FastAPI):
    data = await FileData().get_data_from_dir()
    await SaveData(data).create_and_save()
    yield

app = FastAPI(lifespan=lifespan)
app.include_router(data_router)



#app.mount("/static", StaticFiles(directory="static"), name="static")

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)#127.0.0.1

