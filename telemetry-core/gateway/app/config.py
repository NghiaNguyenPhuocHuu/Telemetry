import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    PROJECT_NAME: str = "TelemetryCore Ingestion Gateway"
    
    # Redis Configuration
    REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", 6379))
    REDIS_STREAM_KEY: str = "telemetry:stream"
    
    # Max items in Redis Stream before trimming (prevents memory overflow)
    REDIS_MAX_STREAM_LEN: int = 100_000

    # Postgres / TimescaleDB settings (optional DSN can override)
    POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT: int = int(os.getenv("POSTGRES_PORT", 5432))
    POSTGRES_DB: str = os.getenv("POSTGRES_DB", "telemetry")
    POSTGRES_USER: str = os.getenv("POSTGRES_USER", "postgres")
    POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "")
    POSTGRES_DSN: str = os.getenv("POSTGRES_DSN", "")

    class Config:
        env_file = ".env"

settings = Settings()