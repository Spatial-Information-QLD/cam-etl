from typing import Any

from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="cam_etl_db__", extra="allow")

    host: str = "localhost"
    port: int = 5432
    name: str = "lalfdb"
    user: str = "postgres"
    password: str = "postgres"


class ETLSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="cam_etl__", extra="allow")

    batch_size: int = 10_000

    db: DatabaseSettings | None = None

    def model_post_init(self, __context: Any) -> None:
        self.db = DatabaseSettings()


class Settings(BaseSettings):
    etl: ETLSettings | None = None

    def model_post_init(self, __context: Any) -> None:
        self.etl = ETLSettings()


settings = Settings()
