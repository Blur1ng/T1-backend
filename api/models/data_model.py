from pydantic import BaseModel
from datetime import datetime

class post_more_data(BaseModel):
    data: list
    table_name: str | None = datetime.now()


