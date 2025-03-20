import os
import clickhouse_connect
from dotenv import load_dotenv

load_dotenv()


class ClickHouse_con:
    def __init__(self):
        self.user=os.getenv("user")
        self.password=os.getenv("password")
        self.host=os.getenv("host")
        self.port=os.getenv("port")
        self.db=os.getenv("db")

        self.cursor = None
    
    def connect(self):
        client = clickhouse_connect.get_client(
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