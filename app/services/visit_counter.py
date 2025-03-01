from typing import Dict, List, Any
import asyncio
import time
from datetime import datetime
from ..core.redis_manager import RedisManager

class VisitCounterService:
    _visit_counters = {}
    
    _cache = {}
    _cache_ttl = 5 
    
    _write_buffer = {}
    _flush_interval = 30  
    _flush_task = None
    _lock = asyncio.Lock()
    
    def __init__(self):
        """Initialize the visit counter service with Redis manager"""
        self.redis_manager = RedisManager()
        
        self._start_flush_task()
    
    def _start_flush_task(self):
        """Start the background task to flush the write buffer periodically"""
        if VisitCounterService._flush_task is None or VisitCounterService._flush_task.done():
            loop = asyncio.get_event_loop()
            VisitCounterService._flush_task = loop.create_task(self._periodic_flush())
    
    async def _periodic_flush(self):
        """Periodically flush the write buffer to Redis"""
        while True:
            try:
                await asyncio.sleep(VisitCounterService._flush_interval)
                await self._flush_buffer_to_redis()
            except asyncio.CancelledError:
             
                await self._flush_buffer_to_redis()
                raise
            except Exception as e:
                print(f"Error in periodic flush: {e}")
              
                await asyncio.sleep(5)  
    
    async def _flush_buffer_to_redis(self):
        """Flush all pending writes in the buffer to Redis"""
        async with VisitCounterService._lock:
            buffer_copy = dict(VisitCounterService._write_buffer)
            
            if not buffer_copy:
                return 
                
            VisitCounterService._write_buffer.clear()
        
    
        for key, count in buffer_copy.items():
            if count > 0:
                try:
                    await self.redis_manager.increment(key, count)
         
                    self._invalidate_cache(key)
                except Exception as e:
                    print(f"Error flushing key {key} to Redis: {e}")
             
                    async with VisitCounterService._lock:
                        if key not in VisitCounterService._write_buffer:
                            VisitCounterService._write_buffer[key] = 0
                        VisitCounterService._write_buffer[key] += count

    async def increment_visit(self, page_id: str) -> None:
        """
        Increment visit count for a page
        
        Args:
            page_id: Unique identifier for the page
        """
   
        if page_id not in self._visit_counters:
            self._visit_counters[page_id] = 0
            

        self._visit_counters[page_id] += 1

    async def get_visit_count(self, page_id: str) -> int:
        """
        Get current visit count for a page
        
        Args:
            page_id: Unique identifier for the page
            
        Returns:
            Current visit count
        """
      
        return self._visit_counters.get(page_id, 0)
        
    async def increment_visit_redis(self, page_id: str) -> None:
        """
        Increment visit count for a page in the write buffer
        
        Args:
            page_id: Unique identifier for the page
        """
        key = f"visit_counter:{page_id}"
        

        async with VisitCounterService._lock:
            if key not in VisitCounterService._write_buffer:
                VisitCounterService._write_buffer[key] = 0
            VisitCounterService._write_buffer[key] += 1
        

        self._invalidate_cache(key)
        
    async def get_visit_count_redis(self, page_id: str) -> (int, str):
        """
        Get current visit count for a page from Redis or cache
        
        Args:
            page_id: Unique identifier for the page
            
        Returns:
            Tuple of (count, source) where source is 'in_memory', 'redis_7070', or 'redis_7071'
        """
        key = f"visit_counter:{page_id}"
        
        # Get pending count from write buffer
        pending_count = 0
        async with VisitCounterService._lock:
            pending_count = VisitCounterService._write_buffer.get(key, 0)
            # Remove from buffer if there are pending counts (we'll flush it)
            if pending_count > 0:
                del VisitCounterService._write_buffer[key]
        
        # Check if value is in cache and not expired
        if key in self._cache and self._cache[key]['expires_at'] > time.time():
            # Add any pending writes from the buffer
            total_count = self._cache[key]['value'] + pending_count
            return total_count, 'in_memory'
        
        # Cache miss or expired, get from Redis
        count, served_via = await self.redis_manager.get(key)
        
        # If there were pending counts, flush them to Redis
        if pending_count > 0:
            # Update Redis with the pending count
            await self.redis_manager.increment(key, pending_count)
            count += pending_count
        
        # Update cache with new value and expiration time
        self._update_cache(key, count)
        
        return count, served_via
    
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
            
    async def shutdown(self):
        """Clean up resources and flush buffer before shutdown"""
        await self._flush_buffer_to_redis()
        
        if VisitCounterService._flush_task and not VisitCounterService._flush_task.done():
            VisitCounterService._flush_task.cancel()
            try:
                await VisitCounterService._flush_task
            except asyncio.CancelledError:
                pass