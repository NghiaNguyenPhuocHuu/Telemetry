import asyncio
import logging
from typing import Optional

from app.config import settings
from app.services.redis_client import RedisClient

logger = logging.getLogger(__name__)


class StreamConsumer:
    """Simple Redis Stream consumer that reads entries and writes to Postgres.

    For now this consumer will only read messages and log them. Later we'll
    add DB persistence and protobuf decoding.
    """

    def __init__(self, redis_client: Optional[RedisClient] = None):
        self.redis = redis_client or RedisClient(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            stream_key=settings.REDIS_STREAM_KEY,
            maxlen=settings.REDIS_MAX_STREAM_LEN,
        )
        self._running = False

    async def start(self):
        await self.redis.connect()
        self._running = True
        logger.info("Consumer started, polling stream %s", self.redis.stream_key)
        # Basic polling loop (XREAD) - placeholder implementation
        last_id = "$"
        while self._running:
            try:
                # Use a small block to avoid busy looping
                result = await self.redis._client.xread({self.redis.stream_key: last_id}, block=1000, count=10)
                if result:
                    # result is list of (stream, [(id, {field: value}), ...])
                    for stream, entries in result:
                        for entry_id, mapping in entries:
                            data = mapping.get(b"data") or mapping.get("data")
                            logger.info("Consumed %s %s bytes", entry_id, len(data) if data else 0)
                            last_id = entry_id
            except Exception:
                logger.exception("Error while reading from stream")
                await asyncio.sleep(1)

    async def stop(self):
        self._running = False
        try:
            await self.redis.close()
        except Exception:
            logger.exception("Error closing consumer redis client")


__all__ = ["StreamConsumer"]
