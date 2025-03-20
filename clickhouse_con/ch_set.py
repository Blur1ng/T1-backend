from clickhouse_connect.driver.httpclient import HttpClient
from datetime import datetime, date


class ClickHouseTable:
    def __init__(self, table_name: str, cursor: HttpClient):
        self.cursor = cursor
        self.table_name = table_name

    def create_table(self, parameters: dict):
        update_parameters = []

        for k, v in parameters.items():
            update_parameters.append(f"`{k}` {self._py_type_to_ch_type(v)}")

        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS `{self.table_name}` ({', '.join(update_parameters)}) 
            ENGINE = MergeTree()
            ORDER BY (`id`)   
        """
        self.cursor.command(create_table_sql)

    def insert_data(self, data_list: list[dict]):
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

        self.cursor.insert(self.table_name, all_row_values, columns)
    
    @staticmethod
    def _py_type_to_ch_type(value):
        if isinstance(value, bool):
            column_type = "UInt8"
        elif isinstance(value, int):
            if value >= 0:
                column_type = "UInt64"
            else:
                column_type = "Int64"
        elif isinstance(value, float):
            column_type = "Float64"
        elif isinstance(value, datetime):
            column_type = "DateTime"
        elif isinstance(value, date):
            column_type = "Date"
        elif value is None:
            column_type = "Nullable(String)"        
        else:
            column_type = "String"
        return column_type


