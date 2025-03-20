from clickhouse_con.save_data import SaveData
from clickhouse_con.filedata import FileData

from fastapi import FastAPI
import uvicorn

from api.endpoints.post_data import data_router

app = FastAPI()
app.include_router(data_router)

#app.mount("/static", StaticFiles(directory="static"), name="static")

def main():
    SaveData(FileData().get_data_from_dir()).create_and_save()


if __name__ == "__main__":
    main()
    uvicorn.run(app, host="127.0.0.1", port=8000)#127.0.0.1

