from fastapi import FastAPI, Request, HTTPException
from contextlib import asynccontextmanager
import logging

from app.config import settings
from app.services.redis_client import RedisClient

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    redis_client = RedisClient(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        stream_key=settings.REDIS_STREAM_KEY,
        maxlen=settings.REDIS_MAX_STREAM_LEN,
    )
    try:
        await redis_client.connect()
    except Exception:
        logger.exception("Failed to connect to Redis at startup")
    app.state.redis = redis_client
    try:
        yield
    finally:
        try:
            await redis_client.close()
        except Exception:
            logger.exception("Error closing Redis client")


app = FastAPI(lifespan=lifespan, title=settings.PROJECT_NAME)


@app.post("/api/v1/telemetry")
async def ingest(request: Request):
    body = await request.body()
    if not body:
        raise HTTPException(status_code=400, detail="Empty request body")

    parsed = False
    # Attempt to parse Protobuf if available
    try:
        from app.pb import telemetry_pb2  # type: ignore

        # Common message names: MetricReading, Telemetry
        for name in ("MetricReading", "Telemetry"):
            Message = getattr(telemetry_pb2, name, None)
            if Message is not None:
                msg = Message()
                msg.ParseFromString(body)
                parsed = True
                break
    except Exception:
        logger.exception("Protobuf parse failed, storing raw bytes")

    # Push to Redis stream
    try:
        await app.state.redis.add_to_stream(body)
    except Exception:
        logger.exception("Failed to push telemetry to Redis stream")
        raise HTTPException(status_code=500, detail="Failed to store telemetry")

    return {"status": "ok", "parsed": parsed}
