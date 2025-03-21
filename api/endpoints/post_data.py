from fastapi import APIRouter, HTTPException

from api.models.data_model import post_more_data

from api.get_from_api import get_api_data

data_router = APIRouter()

@data_router.post("/api/v1/post_data/")
async def post_data(enter_data: post_more_data):
    try:
        await get_api_data(enter_data)
        return {"success": "data has been posted"}

    except Exception as e:
            raise HTTPException(status_code=401, detail=f"Invalid data {e}")
    

