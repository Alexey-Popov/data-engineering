import aiomysql
from app.config import settings

class Database:
    pool = None

    @classmethod
    async def init_pool(cls):
        if cls.pool is None:
            cls.pool = await aiomysql.create_pool(
                host=settings.DB_HOST,
                port=settings.DB_PORT,
                user=settings.DB_USER,
                password=settings.DB_PASSWORD,
                db=settings.DB_NAME,
                autocommit=True,
                minsize=1,
                maxsize=5
            )

    @classmethod
    async def get_conn(cls):
        if cls.pool is None:
            await cls.init_pool()
        return await cls.pool.acquire() 