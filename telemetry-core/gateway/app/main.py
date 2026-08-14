from fastapi import FastAPI, Request, HTTPException, status
from contextlib import asynccontextmanager
import logging
import asyncio

from app.config import settings
from app.services.redis_client import RedisClient
from app.consumer import StreamConsumer

# 1. Global Import for high-throughput performance
try:
    from app.pb.telemetry_pb2 import MetricReading
except ImportError:
    MetricReading = None
    logging.warning("Protobuf definitions not found. Run protoc to generate them.")

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
        # Shutdown consumer gracefully
        try:
            await consumer.stop()
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

# 2. Use 202 Accepted for async queues
@app.post("/api/v1/telemetry", status_code=status.HTTP_202_ACCEPTED)
async def ingest(request: Request):
    body = await request.body()
    if not body:
        raise HTTPException(status_code=400, detail="Empty request body")

    device_id = "unknown"
    parsed = False
    
    # 3. Optimized parsing without dynamic getattr loops
    if MetricReading is not None:
        try:
            msg = MetricReading()
            msg.ParseFromString(body)
            device_id = msg.device_id or "unknown"
            parsed = True
        except Exception:
            logger.exception("Protobuf parse failed, storing raw bytes")

    # Push to Redis stream (passing device_id as a separate field if your client supports it)
    try:
        # Note: adjust `add_to_stream` signature in your RedisClient if needed to accept device_id
        await app.state.redis.add_to_stream(body) 
    except Exception:
        logger.exception("Failed to push telemetry to Redis stream")
        raise HTTPException(status_code=500, detail="Failed to store telemetry")

    return {"status": "accepted", "parsed": parsed, "device_id": device_id}