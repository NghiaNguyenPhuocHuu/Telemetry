import typing
import redis.asyncio as redis


class RedisClient:
    def __init__(self, host: str = "localhost", port: int = 6379, stream_key: str = "telemetry:stream", maxlen: int = 100_000):
        self.host = host
        self.port = port
        self.stream_key = stream_key
        self.maxlen = maxlen
        self._client: typing.Optional[redis.Redis] = None

    async def connect(self) -> None:
        self._client = redis.Redis(host=self.host, port=self.port, decode_responses=False)

    async def add_to_stream(self, data: bytes) -> str:
        if self._client is None:
            await self.connect()
        # xadd expects a mapping of field: value. Use 'data' as the field.
        return await self._client.xadd(self.stream_key, {"data": data}, maxlen=self.maxlen, approximate=False)

    async def close(self) -> None:
        if self._client is not None:
            try:
                await self._client.close()
            finally:
                self._client = None
