from fastapi import APIRouter, HTTPException

from api.models.data_model import post_more_data

from clickhouse_con.ch_classes import DataChange
from clickhouse_con.save_data import SaveData

data_router = APIRouter()

@data_router.post("/api/v1/post_data/")
async def post_data(enter_data: post_more_data):
    try:
        change_data = DataChange(enter_data.data).get_change_data()
        SaveData({enter_data.table_name.strftime("%Y-%m-%d %H").replace("-","_").replace(" ","__").replace(":", "_"): 
                  change_data}).create_and_save()

        return {"success": "data has been posted"}

    except Exception as e:
            raise HTTPException(status_code=401, detail=f"Invalid data {e}")
    
