import asyncio
import logging
from typing import Optional

from app.config import settings
from app.services.redis_client import RedisClient
from app.services.postgres_client import PostgresClient

logger = logging.getLogger(__name__)


class StreamConsumer:
    """Redis Stream consumer that reads entries, decodes Protobuf, and persists to Postgres."""

    def __init__(
        self,
        redis_client: Optional[RedisClient] = None,
        postgres_client: Optional[PostgresClient] = None,
    ):
        self.redis = redis_client or RedisClient(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            stream_key=settings.REDIS_STREAM_KEY,
            maxlen=settings.REDIS_MAX_STREAM_LEN,
        )
        self.postgres = postgres_client or PostgresClient(
            dsn=settings.POSTGRES_DSN,
            host=settings.POSTGRES_HOST,
            port=settings.POSTGRES_PORT,
            database=settings.POSTGRES_DB,
            user=settings.POSTGRES_USER,
            password=settings.POSTGRES_PASSWORD,
        )
        self._running = False

    async def start(self):
        await self.redis.connect()
        await self.postgres.connect()
        self._running = True
        logger.info("Consumer started, polling stream %s", self.redis.stream_key)
        
        # Track last seen ID per stream to support multi-stream reads
        last_ids = {self.redis.stream_key: "$"}
        
        while self._running:
            try:
                # Use a small block to avoid busy looping
                result = await self.redis._client.xread(last_ids, block=1000, count=10)
                if result:
                    # result is list of (stream, [(id, {field: value}), ...])
                    for stream, entries in result:
                        for entry_id, mapping in entries:
                            data = mapping.get(b"data") or mapping.get("data")
                            if data:
                                await self._process_telemetry(data, entry_id)
                            # Update last_id for this specific stream
                            last_ids[stream] = entry_id
            except Exception:
                logger.exception("Error while reading from stream")
                await asyncio.sleep(1)

    async def _process_telemetry(self, data: bytes, entry_id) -> None:
        """Decode Protobuf and persist to database."""
        try:
            from app.pb import telemetry_pb2  # type: ignore

            # Try MetricReading first (primary message type)
            msg = telemetry_pb2.MetricReading()
            msg.ParseFromString(data)
            
            if not msg.device_id:
                logger.warning("Skipping entry %s: no device_id in parsed message", entry_id)
                return

            # Extract acceleration and gyro vectors
            accel = {
                "x": msg.acceleration.x,
                "y": msg.acceleration.y,
                "z": msg.acceleration.z,
            } if msg.acceleration else {}
            
            gyro = {
                "x": msg.gyro.x,
                "y": msg.gyro.y,
                "z": msg.gyro.z,
            } if msg.gyro else {}

            # Insert into database
            reading_id = await self.postgres.insert_telemetry(
                device_id=msg.device_id,
                timestamp=msg.timestamp,
                temperature=msg.temperature if msg.temperature else None,
                voltage=msg.voltage if msg.voltage else None,
                acceleration=accel,
                gyro=gyro,
                metadata=dict(msg.metadata) if msg.metadata else {},
            )
            logger.info(
                "Persisted reading id=%s device=%s timestamp=%s",
                reading_id,
                msg.device_id,
                msg.timestamp,
            )
        except Exception:
            logger.exception("Failed to process telemetry from entry %s", entry_id)

    async def stop(self):
        self._running = False
        try:
            await self.postgres.close()
        except Exception:
            logger.exception("Error closing Postgres client")
        try:
            await self.redis.close()
        except Exception:
            logger.exception("Error closing Redis client")


__all__ = ["StreamConsumer"]
