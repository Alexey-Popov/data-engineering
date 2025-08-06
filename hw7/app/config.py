import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # Cassandra settings
    CASSANDRA_HOST: str = "cassandra"
    CASSANDRA_PORT: int = 9042
    CASSANDRA_KEYSPACE: str = "amazon_reviews"
    
    # Redis settings
    REDIS_HOST: str = "redis"
    REDIS_PORT: int = 6379
    REDIS_TTL: int = 300  # 5 minutes
    
    # API settings
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000
    
    class Config:
        env_file = ".env"

settings = Settings() 