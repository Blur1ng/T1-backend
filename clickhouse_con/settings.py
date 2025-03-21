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
    MAINDIR:        str = Field("MAINDIR", env="MAINDIR")

#class KAFKA_Settings(BaseSettings):
#    USERNAME:       str =  Field("USERNAME", env="USERNAME"),
#    PASSWORD:       str =  Field("PASSWORD", env="PASSWORD"),
#    FROM:           str =  Field("FROM", env="FROM"),
#    PORT:           str =  Field("PORT", env="PORT"),
#    SERVER:         str =  Field("SERVER", env="SERVER"),

ch_settings = CH_Settings()
#kafka_settings = KAFKA_Settings()

ORDER_BY = "id"
