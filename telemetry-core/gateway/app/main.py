from fastapi import FastAPI, Request, HTTPException
from contextlib import asynccontextmanager
import logging
import asyncio

from app.config import settings
from app.services.redis_client import RedisClient
from app.consumer import StreamConsumer

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Setup Redis client
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
    
    # Setup and start consumer as background task
    consumer = StreamConsumer(redis_client=redis_client)
    consumer_task = asyncio.create_task(consumer.start())
    app.state.consumer = consumer
    app.state.consumer_task = consumer_task
    logger.info("Consumer background task started")
    
    try:
        yield
    finally:
        # Shutdown consumer
        try:
            await consumer.stop()
            # Wait for task to finish (with timeout to avoid hanging)
            await asyncio.wait_for(consumer_task, timeout=5.0)
        except asyncio.TimeoutError:
            logger.warning("Consumer task did not complete within timeout, cancelling")
            consumer_task.cancel()
        except Exception:
            logger.exception("Error stopping consumer")
        
        # Shutdown Redis
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
