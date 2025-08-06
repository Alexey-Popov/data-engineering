from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import redis
import json
import time
from app.config import settings

class CassandraDB:
    def __init__(self):
        self.cluster = None
        self.session = None
        self._connected = False
    
    def connect(self):
        """Connect to Cassandra cluster with retry logic"""
        if self._connected:
            return
            
        max_retries = 10
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                print(f"Attempting to connect to Cassandra (attempt {attempt + 1}/{max_retries})...")
                self.cluster = Cluster([settings.CASSANDRA_HOST], port=settings.CASSANDRA_PORT)
                self.session = self.cluster.connect()
                self.session.set_keyspace(settings.CASSANDRA_KEYSPACE)
                self._connected = True
                print("Connected to Cassandra successfully")
                return
            except Exception as e:
                print(f"Error connecting to Cassandra (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    print(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    print("Failed to connect to Cassandra after all retries")
                    # Don't raise the exception, just log it
                    self._connected = False
    
    def execute(self, query, parameters=None):
        """Execute a CQL query"""
        if not self._connected:
            self.connect()
            
        if not self._connected:
            print("Warning: Cassandra not connected, returning empty result")
            return []
            
        try:
            if parameters:
                return self.session.execute(query, parameters)
            else:
                return self.session.execute(query)
        except Exception as e:
            print(f"Error executing query: {e}")
            return []
    
    def close(self):
        """Close Cassandra connection"""
        if self.session:
            self.session.shutdown()
        if self.cluster:
            self.cluster.shutdown()
        self._connected = False

class RedisCache:
    def __init__(self):
        self.redis_client = None
        self._connected = False
    
    def connect(self):
        """Connect to Redis with retry logic"""
        if self._connected:
            return
            
        max_retries = 10
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                print(f"Attempting to connect to Redis (attempt {attempt + 1}/{max_retries})...")
                self.redis_client = redis.Redis(
                    host=settings.REDIS_HOST,
                    port=settings.REDIS_PORT,
                    decode_responses=True
                )
                # Test connection
                self.redis_client.ping()
                self._connected = True
                print("Connected to Redis successfully")
                return
            except Exception as e:
                print(f"Error connecting to Redis (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    print(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    print("Failed to connect to Redis after all retries")
                    self._connected = False
    
    def get(self, key):
        """Get value from cache"""
        if not self._connected:
            self.connect()
            
        if not self._connected:
            return None
            
        try:
            value = self.redis_client.get(key)
            return json.loads(value) if value else None
        except Exception as e:
            print(f"Error getting from cache: {e}")
            return None
    
    def set(self, key, value, ttl=None):
        """Set value in cache with TTL"""
        if not self._connected:
            self.connect()
            
        if not self._connected:
            return
            
        try:
            if ttl is None:
                ttl = settings.REDIS_TTL
            self.redis_client.setex(key, ttl, json.dumps(value))
        except Exception as e:
            print(f"Error setting cache: {e}")
    
    def delete(self, key):
        """Delete key from cache"""
        if not self._connected:
            self.connect()
            
        if not self._connected:
            return
            
        try:
            self.redis_client.delete(key)
        except Exception as e:
            print(f"Error deleting from cache: {e}")

# Global instances - these will be initialized when first used
cassandra_db = None
redis_cache = None

def get_cassandra_db():
    """Get Cassandra DB instance (lazy initialization)"""
    global cassandra_db
    if cassandra_db is None:
        cassandra_db = CassandraDB()
    return cassandra_db

def get_redis_cache():
    """Get Redis cache instance (lazy initialization)"""
    global redis_cache
    if redis_cache is None:
        redis_cache = RedisCache()
    return redis_cache 