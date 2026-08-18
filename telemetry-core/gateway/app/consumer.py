import asyncio
import logging
from typing import Optional, Dict, Any

from app.config import settings
from app.services.redis_client import RedisClient
from app.services.postgres_client import PostgresClient
from app.services.websocket_manager import manager 

#1: Gloabl Import to prevent cac[hing bottlenecks
try:
    from app.pb import telemetry_pb2
except ImportError:
    telemetry_pb2 = None
    logging.warning("Protobuf definitions not found. Run protoc to generate them.")

logger = logging.getLogger(__name__)

class StreamConsumer:
    """Redis Stream consumer using Consumer Groups for zero-data-loss and batch processing."""

    def __init__(
        self,
        redis_client: Optional[RedisClient] = None,
        postgres_client: Optional[PostgresClient] = None,
        group_name: str = "telemetry_consumers",
        consumer_name: str = "worker_1",
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
        self.group_name = group_name
        self.consumer_name = consumer_name
        self._running = False

    async def _ensure_consumer_group(self):
        """Creates the Redis consumer group if it doesn't already exist."""
        try:
            # '0' ensures the group processes all data from the beginning if it's new.
            # mkstream=True automatically creates the stream key if empty.
            await self.redis._client.xgroup_create(
                name=self.redis.stream_key, 
                groupname=self.group_name, 
                id="0", 
                mkstream=True
            )
            logger.info(f"Consumer group '{self.group_name}' created.")
        except Exception as e:
            if "BUSYGROUP" in str(e):
                logger.debug(f"Consumer group '{self.group_name}' already exists.")
            else:
                logger.error(f"Error creating consumer group: {e}")
                raise e

    async def start(self):
        await self.redis.connect()
        await self.postgres.connect()
        
        await self._ensure_consumer_group()
        
        self._running = True
        logger.info("Consumer '%s' started, polling stream %s", self.consumer_name, self.redis.stream_key)
        
        while self._running:
            try:
                # 2. Use XREADGROUP to permanently track processed messages
                # ">" means "fetch messages never delivered to this group before"
                result = await self.redis._client.xreadgroup(
                    groupname=self.group_name,
                    consumername=self.consumer_name,
                    streams={self.redis.stream_key: ">"},
                    count=1000,
                    block=1000
                )
                
                if result:
                    batch_records = []
                    entry_ids_to_ack = []
                    
                    for entries in result:
                        for entry_id, mapping in entries:
                            raw_bytes = mapping.get(b"data") or mapping.get(b"payload") or mapping.get("data")
                            
                            if raw_bytes:
                                parsed_record = self._parse_to_dict(raw_bytes, entry_id)
                                if parsed_record:
                                    batch_records.append(parsed_record)
                                    
                            entry_ids_to_ack.append(entry_id)

                    # 3. Execute a single bulk database insert
                    if batch_records:
                        await self.postgres.bulk_insert_telemetry(batch_records)
                        logger.info("Bulk inserted %d records to Postgres.", len(batch_records))
                        
                        for record in batch_records:
                            # Broadcast to WebSocket clients
                            await manager.broadcast(record)

                    # 4. Acknowledge messages so Redis can free memory (removes them from PEL)
                    if entry_ids_to_ack:
                        await self.redis._client.xack(
                            self.redis.stream_key, 
                            self.group_name, 
                            *entry_ids_to_ack
                        )

            except Exception:
                logger.exception("Error while reading from stream")
                await asyncio.sleep(1)

    def _parse_to_dict(self, data: bytes, entry_id) -> Optional[Dict[str, Any]]:
        """Decode Protobuf entirely in memory and return a mapped dictionary."""
        if not telemetry_pb2:
            return None
            
        try:
            msg = telemetry_pb2.MetricReading()
            msg.ParseFromString(data)
            
            if not msg.device_id:
                logger.warning("Skipping entry %s: no device_id in parsed message", entry_id)
                return None

            # Nested messages in proto3 can be checked with HasField
            accel = {
                "x": msg.acceleration.x,
                "y": msg.acceleration.y,
                "z": msg.acceleration.z,
            } if msg.HasField('acceleration') else {}
            
            gyro = {
                "x": msg.gyro.x,
                "y": msg.gyro.y,
                "z": msg.gyro.z,
            } if msg.HasField('gyro') else {}

            # 5. Fixed the float boolean logic to prevent 0.0 metrics from being dropped
            return {
                "device_id": msg.device_id,
                "timestamp": msg.timestamp,
                "temperature": msg.temperature, 
                "voltage": msg.voltage,         
                "acceleration": accel,
                "gyro": gyro,
                "metadata": dict(msg.metadata) if msg.metadata else {},
            }
        except Exception:
            logger.exception("Failed to parse telemetry from entry %s", entry_id)
            return None

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