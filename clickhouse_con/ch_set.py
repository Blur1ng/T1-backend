import asyncio
from clickhouse_connect.driver.httpclient import HttpClient
from datetime import datetime, date


class ClickHouseTable:
    def __init__(self, table_name: str, cursor: HttpClient):
        self.cursor = cursor
        self.table_name = table_name

    async def create_table(self, parameters: dict, order_by: str):
        update_parameters = [f"`{k}` {await self._py_type_to_ch_type(v)}" for k, v in parameters.items()]
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS `{self.table_name}` ({', '.join(update_parameters)}) 
            ENGINE = MergeTree()
            ORDER BY (`{order_by}`)   
        """
        await self.cursor.command(create_table_sql)

    async def insert_data(self, data_list: list[dict]):
        columns = list(data_list[0].keys())
        
        all_row_values = []
        for dict_data in data_list:
            row_values = []
            for col in columns:
                v = dict_data.get(col)
                if isinstance(v, bool):
                    v = 1 if v else 0
                row_values.append(v)
            all_row_values.append(row_values)

        await self.cursor.insert(self.table_name, all_row_values, columns)
    
    @staticmethod
    async def _py_type_to_ch_type(value):
        type_mapping = {bool: "UInt8", str: "String", int: "Int64", 
                        float: "Float64", datetime: "DateTime", date: "Date", 
                        None: "Nullable(String)"}
        
        type_ = type_mapping[type(value)]
        return type_ if type_ else "String"


