import redis.asyncio as redis
from app.config import settings

class RedisCache:
    _client = None

    @classmethod
    def get_client(cls):
        if cls._client is None:
            cls._client = redis.Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, decode_responses=True)
        return cls._client

    @classmethod
    async def get(cls, key: str):
        client = cls.get_client()
        return await client.get(key)

    @classmethod
    async def set(cls, key: str, value: str, ttl: int):
        client = cls.get_client()
        await client.set(key, value, ex=ttl) 