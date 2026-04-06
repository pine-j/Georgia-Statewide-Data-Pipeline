from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = Field(default="Georgia Statewide Webapp")
    app_env: str = Field(default="local")
    api_prefix: str = Field(default="/api")
    data_mode: str = Field(default="staged")
    database_url: str = Field(
        default="postgresql+psycopg://postgres:postgres@localhost:5432/georgia_webapp"
    )
    cors_origins: str = Field(
        default="http://localhost:5173,http://127.0.0.1:5173"
    )
    default_state: str = Field(default="ga")
    map_provider: str = Field(default="maplibre")
    staged_chunk_size: int = Field(default=10000)

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    @property
    def cors_origin_list(self) -> list[str]:
        return [origin.strip() for origin in self.cors_origins.split(",") if origin.strip()]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
