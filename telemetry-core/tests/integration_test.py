"""
Integration test for TelemetryCore end-to-end flow:
Gateway → Redis → Consumer → PostgreSQL
"""

import asyncio
import time
import logging
import requests
from typing import Optional

import asyncpg

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


class IntegrationTester:
    """End-to-end integration test: send telemetry, verify DB persistence."""

    def __init__(
        self,
        gateway_url: str = "http://localhost:8000",
        postgres_dsn: str = "postgresql://postgres@localhost:5432/telemetry",
    ):
        self.gateway_url = gateway_url
        self.postgres_dsn = postgres_dsn
        self.db_pool: Optional[asyncpg.Pool] = None

    async def setup(self) -> None:
        """Initialize database connection pool."""
        try:
            self.db_pool = await asyncpg.create_pool(self.postgres_dsn, min_size=1, max_size=5)
            logger.info("Connected to PostgreSQL")
        except Exception as e:
            logger.error("Failed to connect to PostgreSQL: %s", e)
            raise

    async def teardown(self) -> None:
        """Close database connections."""
        if self.db_pool:
            await self.db_pool.close()
            logger.info("Closed PostgreSQL pool")

    def send_telemetry(self, device_id: str, temperature: float = 25.0) -> None:
        """Send binary Protobuf telemetry to the gateway.
        
        Args:
            device_id: Device identifier
            temperature: Temperature reading
        """
        try:
            from app.pb import telemetry_pb2
            
            msg = telemetry_pb2.MetricReading(
                device_id=device_id,
                timestamp=int(time.time() * 1000),  # milliseconds
                temperature=temperature,
                voltage=12.0,
            )
            
            response = requests.post(
                f"{self.gateway_url}/api/v1/telemetry",
                data=msg.SerializeToString(),
                timeout=5,
            )
            response.raise_for_status()
            logger.info("Sent telemetry for %s: status=%s", device_id, response.status_code)
        except Exception as e:
            logger.error("Failed to send telemetry: %s", e)
            raise

    async def verify_in_database(self, device_id: str, max_wait_seconds: int = 5) -> bool:
        """Poll database until telemetry appears (or timeout).
        
        Args:
            device_id: Device to search for
            max_wait_seconds: Max time to wait for data to appear
            
        Returns:
            True if data found, False if timeout
        """
        if not self.db_pool:
            logger.error("Database pool not initialized")
            return False

        start_time = time.time()
        poll_interval = 0.5

        while time.time() - start_time < max_wait_seconds:
            try:
                async with self.db_pool.acquire() as conn:
                    row = await conn.fetchrow(
                        "SELECT id, device_id, temperature FROM telemetry_readings WHERE device_id = $1 ORDER BY timestamp DESC LIMIT 1",
                        device_id,
                    )
                    if row:
                        logger.info(
                            "✓ Found in DB: id=%s, device=%s, temperature=%.1f",
                            row["id"],
                            row["device_id"],
                            row["temperature"],
                        )
                        return True
            except Exception as e:
                logger.warning("Database query error (will retry): %s", e)

            await asyncio.sleep(poll_interval)

        logger.error("✗ Telemetry NOT found in database after %ds", max_wait_seconds)
        return False

    async def run_test(self, device_id: str = "test_device_001", temperature: float = 23.5) -> bool:
        """Run full integration test: send → verify.
        
        Args:
            device_id: Test device ID
            temperature: Test temperature value
            
        Returns:
            True if test passed, False if failed
        """
        logger.info("Starting integration test...")
        
        try:
            # 1. Send telemetry
            self.send_telemetry(device_id, temperature)
            logger.info("Telemetry sent, waiting for pipeline...")

            # 2. Poll database
            found = await self.verify_in_database(device_id, max_wait_seconds=5)
            
            if found:
                logger.info("✓ Integration test PASSED")
                return True
            else:
                logger.error("✗ Integration test FAILED: data not found in DB")
                return False
        except Exception as e:
            logger.exception("Integration test FAILED with exception: %s", e)
            return False


async def main():
    """Run integration test."""
    tester = IntegrationTester()
    
    try:
        await tester.setup()
        
        # Test 1: Basic telemetry
        success = await tester.run_test(
            device_id="test_device_001",
            temperature=23.5,
        )
        
        if success:
            # Test 2: Another device
            success2 = await tester.run_test(
                device_id="test_device_002",
                temperature=25.0,
            )
            
            if success2:
                logger.info("✓✓ All tests PASSED!")
            else:
                logger.warning("Second test failed")
        
    finally:
        await tester.teardown()


if __name__ == "__main__":
    asyncio.run(main())
