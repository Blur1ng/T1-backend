from clickhouse_con.ch_con import ClickHouse_con
from clickhouse_con.ch_set import ClickHouseTable
from clickhouse_con.settings import ORDER_BY

import logging
logger = logging.getLogger(__name__)


class SaveData:
    def __init__(self, all_data: dict[str, list[dict]]):
        self.all_data = all_data

    async def create_and_save(self):
        ch_con = ClickHouse_con()
        await ch_con.connect()
        cursor = ch_con.cursor
        try:
            for table_name_, columns in self.all_data.items():
                ch = ClickHouseTable(table_name_, cursor)
                await ch.create_table(columns[0], ORDER_BY)
                await ch.insert_data(columns)
        finally:
            await cursor.close()
