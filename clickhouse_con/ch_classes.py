import asyncio
from dateutil import parser

import logging
logger = logging.getLogger(__name__)

class DataChange:
    def __init__(self, data: list[dict]):
        self.data = data
    
    async def get_change_data(self) -> list[dict]:
        change_data = []
        for old_dict in self.data:
            true_types = await self._get_true_types_from_dict(old_dict)
            change_data.append(true_types)
        return change_data

    @staticmethod
    async def _get_true_types_from_dict(data: dict) -> dict:
        update_dict = {}

        data_id = int(data.get("id"))
        data.pop("id")
        update_dict["id"] = data_id

        for column_name in data:
            if "time" in column_name:
                time_ = data.get(column_name)
                try:
                    data_time = parser.parse(time_) if time_ else None
                except Exception as e:
                    data_time = None
                    print("Can not parse", e)
                update_dict[column_name] = data_time
            
            elif "is_" in column_name:
                bool_value = data.get(column_name)

                if isinstance(bool_value, str):
                    bool_value = bool_value.lower() not in ["false", "0", "no", "n"]
                else:
                    bool_value = bool(bool_value)

                update_dict[column_name] = bool_value
            else:
                update_dict[column_name] = data[column_name]
        return update_dict
    






