from clickhouse_con.ch_con import ClickHouse_con
from clickhouse_con.ch_set import ClickHouseTable
from clickhouse_con.settings import ORDER_BY

class SaveData:
    def __init__(self, all_data: dict[str, list[dict]]):
        self.all_data = all_data

    def create_and_save(self):
        ch_con = ClickHouse_con()
        ch_con.connect()
        cursor = ch_con.cursor
        try:
            for file_name, columns in self.all_data.items():
                table_name_ = file_name
                ch = ClickHouseTable(table_name_, cursor)
                ch.create_table(columns[0], ORDER_BY)
                ch.insert_data(columns)
        finally:
            cursor.close()
