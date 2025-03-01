from typing import Dict, List, Any
import asyncio
import time
from datetime import datetime
from ..core.redis_manager import RedisManager

class VisitCounterService:
    # In-memory storage for visit counters
    _visit_counters = {}
    
    # Application-level cache with TTL
    _cache = {}
    _cache_ttl = 5  # 5 seconds TTL
    
    def __init__(self):
        """Initialize the visit counter service with Redis manager"""
        self.redis_manager = RedisManager()

    async def increment_visit(self, page_id: str) -> None:
        """
        Increment visit count for a page
        
        Args:
            page_id: Unique identifier for the page
        """
        # Initialize counter if it doesn't exist
        if page_id not in self._visit_counters:
            self._visit_counters[page_id] = 0
            
        # Increment the counter
        self._visit_counters[page_id] += 1

    async def get_visit_count(self, page_id: str) -> int:
        """
        Get current visit count for a page
        
        Args:
            page_id: Unique identifier for the page
            
        Returns:
            Current visit count
        """
        # Return 0 if page_id doesn't exist in counters
        return self._visit_counters.get(page_id, 0)
        
    async def increment_visit_redis(self, page_id: str) -> None:
        """
        Increment visit count for a page in Redis
        
        Args:
            page_id: Unique identifier for the page
        """
        key = f"visit_counter:{page_id}"
        await self.redis_manager.increment(key)
        
        # Invalidate cache after write
        self._invalidate_cache(key)
        
    async def get_visit_count_redis(self, page_id: str) -> (int, str):
        """
        Get current visit count for a page from Redis or cache
        
        Args:
            page_id: Unique identifier for the page
            
        Returns:
            Tuple of (count, source) where source is 'in_memory' or 'redis'
        """
        key = f"visit_counter:{page_id}"
        
        # Check if value is in cache and not expired
        if key in self._cache and self._cache[key]['expires_at'] > time.time():
            return self._cache[key]['value'], 'in_memory'
        
        # Cache miss or expired, get from Redis
        count = await self.redis_manager.get(key)
        
        # Update cache with new value and expiration time
        self._update_cache(key, count)
        
        return count, 'redis'
    
    def _update_cache(self, key: str, value: int) -> None:
        """
        Update the cache with a new value and expiration time
        
        Args:
            key: The key to cache
            value: The value to cache
        """
        self._cache[key] = {
            'value': value,
            'expires_at': time.time() + self._cache_ttl
        }
    
    def _invalidate_cache(self, key: str) -> None:
        """
        Invalidate a cached value
        
        Args:
            key: The key to invalidate
        """
        if key in self._cache:
            del self._cache[key]