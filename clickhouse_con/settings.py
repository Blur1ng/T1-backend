from pydantic_settings import BaseSettings
from pydantic import Field
from dotenv import load_dotenv


load_dotenv()

class CH_Settings(BaseSettings):
    user:     str = Field("user",     env="user")
    password: str = Field("password", env="password")
    host:     str = Field("host",     env="host")
    port:     str = Field("port",     env="port")
    db:       str = Field("db",       env="db")
    MAINDIR:  str = Field("MAINDIR",  env="MAINDIR")

class KAFKA_Settings(BaseSettings):
    bootstrap_servers: str = Field("localhost:9092", env="KAFKA_BOOTSTRAP_SERVERS")
    topic:             str = Field("test_topic",     env="KAFKA_TOPIC")
    group_id:          str = Field("test_group",     env="KAFKA_GROUP_ID")

ch_settings = CH_Settings()
kafka_settings = KAFKA_Settings()

ORDER_BY="id"
