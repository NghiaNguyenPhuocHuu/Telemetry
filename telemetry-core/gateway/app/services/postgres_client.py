import logging
import json
import asyncpg
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


class PostgresClient:
    """AsyncPG client for TimescaleDB/Postgres operations.
    
    Handles connection pooling and high-performance bulk/single CRUD operations for telemetry data.
    """

    def __init__(
        self,
        dsn: str = "",
        host: str = "localhost",
        port: int = 5432,
        database: str = "telemetry",
        user: str = "postgres",
        password: str = "",
    ):
        if dsn:
            self.dsn = dsn
        else:
            password_part = f":{password}" if password else ""
            self.dsn = f"postgresql://{user}{password_part}@{host}:{port}/{database}"
        
        self._pool: Optional[asyncpg.Pool] = None

    async def connect(self) -> None:
        """Create a connection pool."""
        try:
            self._pool = await asyncpg.create_pool(self.dsn, min_size=5, max_size=20)
            logger.info("Connected to Postgres pool")
        except Exception as e:
            logger.exception("Failed to create Postgres connection pool: %s", e)
            raise

    async def close(self) -> None:
        """Close the connection pool."""
        if self._pool:
            await self._pool.close()
            logger.info("Closed Postgres connection pool")

    async def insert_telemetry(
        self,
        device_id: str,
        timestamp: int,
        temperature: Optional[float] = None,
        voltage: Optional[float] = None,
        acceleration: Optional[Dict[str, float]] = None,
        gyro: Optional[Dict[str, float]] = None,
        metadata: Optional[Dict[str, str]] = None,
    ) -> int:
        """Insert a single telemetry reading into the database."""
        if not self._pool:
            raise RuntimeError("Database pool not initialized")

        accel = acceleration or {}
        gyro_data = gyro or {}
        meta = metadata or {}

        query = """
        INSERT INTO telemetry_readings (
            device_id, timestamp, temperature, voltage,
            acceleration_x, acceleration_y, acceleration_z,
            gyro_x, gyro_y, gyro_z, metadata
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        RETURNING id;
        """

        try:
            async with self._pool.acquire() as conn:
                result = await conn.fetchval(
                    query,
                    device_id,
                    timestamp,
                    temperature,
                    voltage,
                    accel.get("x"),
                    accel.get("y"),
                    accel.get("z"),
                    gyro_data.get("x"),
                    gyro_data.get("y"),
                    gyro_data.get("z"),
                    json.dumps(meta),
                )
                return result
        except Exception as e:
            logger.exception("Failed to insert telemetry: %s", e)
            raise

    async def bulk_insert_telemetry(self, records: list[Dict[str, Any]]) -> None:
        """High-performance bulk insertion for consumer batches using executemany."""
        if not self._pool:
            raise RuntimeError("Database pool not initialized")

        query = """
        INSERT INTO telemetry_readings (
            device_id, timestamp, temperature, voltage,
            acceleration_x, acceleration_y, acceleration_z,
            gyro_x, gyro_y, gyro_z, metadata
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        """
        
        # Format the batch list into tuples matching the query parameters
        data = [
            (
                r["device_id"], 
                r["timestamp"], 
                r["temperature"], 
                r["voltage"],
                r["acceleration"].get("x") if r.get("acceleration") else None, 
                r["acceleration"].get("y") if r.get("acceleration") else None, 
                r["acceleration"].get("z") if r.get("acceleration") else None,
                r["gyro"].get("x") if r.get("gyro") else None, 
                r["gyro"].get("y") if r.get("gyro") else None, 
                r["gyro"].get("z") if r.get("gyro") else None,
                json.dumps(r.get("metadata", {}))
            )
            for r in records
        ]

        try:
            async with self._pool.acquire() as conn:
                await conn.executemany(query, data)
                logger.debug("Successfully bulk inserted %d records.", len(data))
        except Exception as e:
            logger.exception("Failed to execute bulk insert: %s", e)
            raise

    async def get_latest_readings(
        self, device_id: str, limit: int = 10
    ) -> list[Dict[str, Any]]:
        """Fetch latest telemetry readings for a device."""
        if not self._pool:
            raise RuntimeError("Database pool not initialized")

        query = """
        SELECT * FROM telemetry_readings
        WHERE device_id = $1
        ORDER BY timestamp DESC
        LIMIT $2;
        """

        try:
            async with self._pool.acquire() as conn:
                rows = await conn.fetch(query, device_id, limit)
                return [dict(row) for row in rows]
        except Exception as e:
            logger.exception("Failed to fetch readings: %s", e)
            raise

    async def insert_alert(
        self,
        device_id: str,
        alert_type: str,
        severity: str = "warning",
        message: str = "",
        affected_reading_id: Optional[int] = None,
    ) -> int:
        """Insert an alert into the alerts table."""
        if not self._pool:
            raise RuntimeError("Database pool not initialized")

        query = """
        INSERT INTO alerts (device_id, alert_type, severity, message, affected_reading_id)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING id;
        """

        try:
            async with self._pool.acquire() as conn:
                result = await conn.fetchval(
                    query, device_id, alert_type, severity, message, affected_reading_id
                )
                return result
        except Exception as e:
            logger.exception("Failed to insert alert: %s", e)
            raise


__all__ = ["PostgresClient"]