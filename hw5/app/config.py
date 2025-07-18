from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    DB_HOST: str = "db"
    DB_PORT: int = 3306
    DB_USER: str = "root"
    DB_PASSWORD: str = "example"
    DB_NAME: str = "ad_platform"
    REDIS_HOST: str = "redis"
    REDIS_PORT: int = 6379

    class Config:
        env_file = ".env"

settings = Settings() 