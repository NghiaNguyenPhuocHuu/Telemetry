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

    class Config:
        env_file = ".env"

settings = Settings()