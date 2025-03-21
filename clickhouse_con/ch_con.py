import clickhouse_connect
from clickhouse_con.settings import ch_settings

import logging
logger = logging.getLogger(__name__)


class ClickHouse_con:
    def __init__(self):
        self.user = ch_settings.user
        self.password = ch_settings.password
        self.host = ch_settings.host
        self.port = ch_settings.port
        self.db = ch_settings.db

        self.cursor = None
    
    async def connect(self):
        client = await clickhouse_connect.create_async_client(
            host=self.host, 
            port=self.port, 
            username=self.user, 
            password=self.password,
            database=self.db,
        )
        self.cursor = client

    def close(self):
        self.cursor.close()

    def get_cursor(self):
        return self.cursor