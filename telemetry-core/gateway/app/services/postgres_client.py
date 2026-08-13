import logging
import asyncpg
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


class PostgresClient:
    """AsyncPG client for TimescaleDB/Postgres operations.
    
    Handles connection pooling and basic CRUD operations for telemetry data.
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
        """Initialize Postgres client.
        
        Args:
            dsn: Full connection string (takes precedence if provided)
            host: Postgres hostname
            port: Postgres port
            database: Database name
            user: Username
            password: Password
        """
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
        """Insert a telemetry reading into the database.
        
        Args:
            device_id: Device identifier
            timestamp: Epoch timestamp in milliseconds
            temperature: Temperature reading
            voltage: Voltage reading
            acceleration: Dict with 'x', 'y', 'z' keys
            gyro: Dict with 'x', 'y', 'z' keys
            metadata: Key-value metadata as dict
            
        Returns:
            The inserted row ID
        """
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
                    meta,
                )
                return result
        except Exception as e:
            logger.exception("Failed to insert telemetry: %s", e)
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
