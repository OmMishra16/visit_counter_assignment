import redis
import asyncio
from typing import Dict, List, Optional, Any
from .consistent_hash import ConsistentHash
from .config import settings

class RedisManager:
    def __init__(self):
        """Initialize Redis connection pools and consistent hashing"""
        self.connection_pools: Dict[str, redis.ConnectionPool] = {}
        self.redis_clients: Dict[str, redis.Redis] = {}
        
        # Parse Redis nodes from comma-separated string
        redis_nodes = [node.strip() for node in settings.REDIS_NODES.split(",") if node.strip()]
        self.consistent_hash = ConsistentHash(redis_nodes, settings.VIRTUAL_NODES)
        
        # Initialize connection pools for each Redis node
        for node in redis_nodes:
            self.connection_pools[node] = redis.ConnectionPool.from_url(node)
            self.redis_clients[node] = redis.Redis(connection_pool=self.connection_pools[node])

    async def get_connection(self, key: str) -> redis.Redis:
        """
        Get Redis connection for the given key using consistent hashing
        
        Args:
            key: The key to determine which Redis node to use
            
        Returns:
            Redis client for the appropriate node
        """
        # For Task 2, we'll use the first Redis node
        if self.redis_clients:
            return list(self.redis_clients.values())[0]
        raise Exception("No Redis connections available")

    async def increment(self, key: str, amount: int = 1) -> int:
        """
        Increment a counter in Redis
        
        Args:
            key: The key to increment
            amount: Amount to increment by
            
        Returns:
            New value of the counter
        """
        redis_client = await self.get_connection(key)
        # Redis operations are blocking, so we run them in a thread pool
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None, lambda: redis_client.incrby(key, amount)
        )
        return result

    async def get(self, key: str) -> Optional[int]:
        """
        Get value for a key from Redis
        
        Args:
            key: The key to get
            
        Returns:
            Value of the key or None if not found
        """
        redis_client = await self.get_connection(key)
        # Redis operations are blocking, so we run them in a thread pool
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None, lambda: redis_client.get(key)
        )
        
        if result is None:
            return 0
        
        try:
            return int(result)
        except (ValueError, TypeError):
            return 0
