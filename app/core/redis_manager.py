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
        
        redis_nodes = [node.strip() for node in settings.REDIS_NODES.split(",") if node.strip()]
        self.consistent_hash = ConsistentHash(redis_nodes, settings.VIRTUAL_NODES)
        
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
        node = self.consistent_hash.get_node(key)
        
        if node in self.redis_clients:
            return self.redis_clients[node], node
        
        raise Exception(f"No Redis connection available for node {node}")

    async def increment(self, key: str, amount: int = 1) -> int:
        """
        Increment a counter in Redis
        
        Args:
            key: The key to increment
            amount: Amount to increment by
            
        Returns:
            New value of the counter
        """
        redis_client, _ = await self.get_connection(key)
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None, lambda: redis_client.incrby(key, amount)
        )
        return result

    async def get(self, key: str) -> (Optional[int], str):
        """
        Get value for a key from Redis
        
        Args:
            key: The key to get
            
        Returns:
            Tuple of (value, node) where value is the key's value and node is the Redis node
        """
        redis_client, node = await self.get_connection(key)
        # Redis operations are blocking, so we run them in a thread pool
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None, lambda: redis_client.get(key)
        )
        
        try:
            print(f"Node URL: {node}")
            
            if "redis://" in node:
                host_port = node.replace("redis://", "").split("/")[0]
                port = host_port.split(":")[1]
                served_via = f"redis_{port}"
            else:
                served_via = "redis"
        except (IndexError, ValueError) as e:
            print(f"Error extracting port: {e}, node: {node}")
            served_via = "redis"
        
        if result is None:
            return 0, served_via
        
        try:
            return int(result), served_via
        except (TypeError, ValueError):
            return 0, served_via
