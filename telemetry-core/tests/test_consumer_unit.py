"""
Unit tests for StreamConsumer with mocked Redis and Postgres.
Tests core logic: Protobuf decoding, data extraction, DB writes.
"""

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import time

# Mock the imports before importing consumer
import sys
from unittest.mock import MagicMock

# Mock protobuf module
sys.modules['app.pb'] = MagicMock()
sys.modules['app.pb.telemetry_pb2'] = MagicMock()


class TestStreamConsumerLogic:
    """Test StreamConsumer with mocked dependencies."""

    @pytest.mark.asyncio
    async def test_consumer_connects(self):
        """Test that consumer connects to Redis and Postgres."""
        # Mock clients
        mock_redis = AsyncMock()
        mock_redis.connect = AsyncMock()
        mock_redis._client = MagicMock()
        mock_redis.stream_key = "telemetry:stream"

        mock_postgres = AsyncMock()
        mock_postgres.connect = AsyncMock()

        # Import after mocks are set up
        from app.consumer import StreamConsumer

        consumer = StreamConsumer(
            redis_client=mock_redis,
            postgres_client=mock_postgres,
        )

        # Start and immediately stop to test connection
        asyncio.create_task(consumer.start())
        await asyncio.sleep(0.1)
        await consumer.stop()

        # Verify connections were attempted
        mock_redis.connect.assert_called_once()
        mock_postgres.connect.assert_called_once()

    @pytest.mark.asyncio
    async def test_consumer_decodes_protobuf(self):
        """Test that consumer correctly decodes Protobuf messages."""
        # Mock the Protobuf message
        mock_message = MagicMock()
        mock_message.device_id = "device_001"
        mock_message.timestamp = 1691234567000
        mock_message.temperature = 23.5
        mock_message.voltage = 12.0
        mock_message.acceleration.x = 0.1
        mock_message.acceleration.y = 0.2
        mock_message.acceleration.z = 0.3
        mock_message.gyro.x = 1.0
        mock_message.gyro.y = 2.0
        mock_message.gyro.z = 3.0
        mock_message.metadata = {"location": "warehouse"}

        mock_reading_class = MagicMock()
        mock_reading_class.return_value = mock_message

        mock_redis = AsyncMock()
        mock_redis.connect = AsyncMock()
        mock_redis._client = MagicMock()
        mock_redis.stream_key = "telemetry:stream"
        mock_redis._client.xread = AsyncMock(return_value=None)  # Empty read
        mock_redis.close = AsyncMock()

        mock_postgres = AsyncMock()
        mock_postgres.connect = AsyncMock()
        mock_postgres.insert_telemetry = AsyncMock(return_value=42)  # Return ID
        mock_postgres.close = AsyncMock()

        from app.consumer import StreamConsumer

        consumer = StreamConsumer(
            redis_client=mock_redis,
            postgres_client=mock_postgres,
        )

        # Simulate Protobuf parsing
        msg_bytes = b"mock_protobuf_data"
        with patch("app.consumer.telemetry_pb2.MetricReading", mock_reading_class):
            await consumer._process_telemetry(msg_bytes, "1234-0")

        # Verify DB insert was called with correct data
        mock_postgres.insert_telemetry.assert_called_once()
        call_args = mock_postgres.insert_telemetry.call_args

        assert call_args.kwargs["device_id"] == "device_001"
        assert call_args.kwargs["timestamp"] == 1691234567000
        assert call_args.kwargs["temperature"] == 23.5
        assert call_args.kwargs["voltage"] == 12.0
        assert call_args.kwargs["acceleration"]["x"] == 0.1
        assert call_args.kwargs["gyro"]["z"] == 3.0
        assert call_args.kwargs["metadata"]["location"] == "warehouse"

    @pytest.mark.asyncio
    async def test_consumer_skips_empty_device_id(self):
        """Test that consumer skips messages with no device_id."""
        mock_message = MagicMock()
        mock_message.device_id = ""  # Empty device ID

        mock_reading_class = MagicMock(return_value=mock_message)

        mock_postgres = AsyncMock()
        mock_postgres.insert_telemetry = AsyncMock()

        mock_redis = AsyncMock()
        mock_redis.connect = AsyncMock()
        mock_redis._client = MagicMock()
        mock_redis.stream_key = "telemetry:stream"
        mock_redis.close = AsyncMock()

        from app.consumer import StreamConsumer

        consumer = StreamConsumer(
            redis_client=mock_redis,
            postgres_client=mock_postgres,
        )

        with patch("app.consumer.telemetry_pb2.MetricReading", mock_reading_class):
            await consumer._process_telemetry(b"mock_data", "1234-0")

        # Should NOT call insert_telemetry for empty device_id
        mock_postgres.insert_telemetry.assert_not_called()

    @pytest.mark.asyncio
    async def test_consumer_handles_missing_vectors(self):
        """Test that consumer safely handles missing acceleration/gyro vectors."""
        mock_message = MagicMock()
        mock_message.device_id = "device_002"
        mock_message.timestamp = 1691234567000
        mock_message.temperature = 25.0
        mock_message.voltage = 12.0
        mock_message.acceleration = None  # Missing
        mock_message.gyro = None  # Missing
        mock_message.metadata = {}

        mock_reading_class = MagicMock(return_value=mock_message)

        mock_postgres = AsyncMock()
        mock_postgres.insert_telemetry = AsyncMock(return_value=43)

        mock_redis = AsyncMock()
        mock_redis.connect = AsyncMock()
        mock_redis._client = MagicMock()
        mock_redis.stream_key = "telemetry:stream"
        mock_redis.close = AsyncMock()

        from app.consumer import StreamConsumer

        consumer = StreamConsumer(
            redis_client=mock_redis,
            postgres_client=mock_postgres,
        )

        with patch("app.consumer.telemetry_pb2.MetricReading", mock_reading_class):
            await consumer._process_telemetry(b"mock_data", "1234-1")

        # Should still insert with empty acceleration/gyro dicts
        mock_postgres.insert_telemetry.assert_called_once()
        call_args = mock_postgres.insert_telemetry.call_args
        assert call_args.kwargs["acceleration"] == {}
        assert call_args.kwargs["gyro"] == {}

    @pytest.mark.asyncio
    async def test_consumer_graceful_shutdown(self):
        """Test that consumer closes all connections on shutdown."""
        mock_redis = AsyncMock()
        mock_redis.connect = AsyncMock()
        mock_redis._client = MagicMock()
        mock_redis.stream_key = "telemetry:stream"
        mock_redis.close = AsyncMock()

        mock_postgres = AsyncMock()
        mock_postgres.connect = AsyncMock()
        mock_postgres.close = AsyncMock()

        from app.consumer import StreamConsumer

        consumer = StreamConsumer(
            redis_client=mock_redis,
            postgres_client=mock_postgres,
        )

        # Start and stop
        asyncio.create_task(consumer.start())
        await asyncio.sleep(0.05)
        await consumer.stop()

        # Both should be closed
        mock_postgres.close.assert_called_once()
        mock_redis.close.assert_called_once()


class TestPostgresClientLogic:
    """Test PostgresClient CRUD operations."""

    def test_postgres_client_dsn_construction(self):
        """Test that PostgresClient correctly builds connection DSN."""
        from app.services.postgres_client import PostgresClient

        # Test with explicit DSN
        client_dsn = PostgresClient(dsn="postgresql://user:pass@localhost:5432/db")
        assert client_dsn.dsn == "postgresql://user:pass@localhost:5432/db"

        # Test with components
        client_components = PostgresClient(
            host="myhost",
            port=5433,
            database="mydb",
            user="myuser",
            password="mypass",
        )
        expected = "postgresql://myuser:mypass@myhost:5433/mydb"
        assert client_components.dsn == expected

        # Test with no password
        client_no_pass = PostgresClient(
            host="localhost",
            port=5432,
            database="telemetry",
            user="postgres",
            password="",
        )
        expected_no_pass = "postgresql://postgres@localhost:5432/telemetry"
        assert client_no_pass.dsn == expected_no_pass


class TestIntegrationTestLogic:
    """Test integration test helpers."""

    def test_integration_tester_init(self):
        """Test IntegrationTester initialization."""
        from tests.integration_test import IntegrationTester

        tester = IntegrationTester(
            gateway_url="http://localhost:8000",
            postgres_dsn="postgresql://postgres@localhost:5432/telemetry",
        )

        assert tester.gateway_url == "http://localhost:8000"
        assert tester.postgres_dsn == "postgresql://postgres@localhost:5432/telemetry"
        assert tester.db_pool is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
