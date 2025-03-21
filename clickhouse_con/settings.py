from pydantic_settings import BaseSettings
from pydantic import Field

from dotenv import load_dotenv

load_dotenv()
class CH_Settings(BaseSettings):
    user:           str = Field("user", env="user")
    password:       str = Field("password", env="password")
    host:           str = Field("host", env="host")
    port:           str = Field("port", env="port")
    db:             str = Field("db", env="db")
    MAINDIR:        str = Field("server", env="server")

ch_settings = CH_Settings()

ORDER_BY = "id"
