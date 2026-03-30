from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field


class Settings(BaseSettings):

    REDIS_HOST: str = Field(env="REDIS_HOST", default="0.0.0.0")
    REDIS_PORT: int = Field(env="REDIS_PORT", default=6379)

    LOG_LEVEL: str = Field(env="LOG_LEVEL", default="warning")

    RED_SCALE: float = Field(env="RED_SCALE", default=0.990)
    BLUE_SCALE: float = Field(env="BLUE_SCALE", default=1.010)

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_ignore_empty=True
    )

settings = Settings()

