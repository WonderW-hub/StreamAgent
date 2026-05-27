# \src\stream_agent\memory\layered.py
"""Industrial-grade L1 (Hot/Redis) + L2 (Cold/Persistent) Async Memory System Manager"""
import asyncio
import logging
from typing import List, Dict, Any, Optional

from stream_agent.memory.base import BaseMemory
from stream_agent.memory.redis_plugin import RedisMemoryPlugin

logger = logging.getLogger("StreamAgent.LayeredMemory")

class LayeredMemoryManager(BaseMemory):
    """
    Industrial-grade L1(Hot/Redis) + L2(Cold/Persistent) Async Memory System Manager.
    
    Guarantees:
    1. Read operations prioritize hitting the L1 cache, ensuring millisecond-level latency under high concurrency.
    2. Write operations implement旁路异步刷盘 for L2, without occupying the LLM interaction main chain response time.
    3. Comes with an automatic hot reload mechanism to defend against Redis memory jitter or cache invalidation.
    """
    def __init__(
        self, 
        agent_name: str,
        redis_url: str = "redis://localhost:6379/0",
        l1_max_len: int = 10,  # L1 cache retains the last 10 rounds (20 messages) of dialogue history by default
        l2_plugin: Optional[BaseMemory] = None  # Allows injection of any cold storage plug-in that implements BaseMemory
    ):
        self.agent_name = agent_name       
        self.l1_cache = RedisMemoryPlugin(redis_url=redis_url, max_len=l1_max_len * 2, ttl=86400)
        self.l2_storage = l2_plugin
        self._background_tasks = set()

    async def get_history(self, session_id: str, limit: int = 10) -> List[Dict[str, str]]:
        """
        Fully automated tiered read strategy (with automatic Hydration mechanism)
        """
        # 1. Try to fetch from L1 cache
        history = await self.l1_cache.get_history(session_id, limit=limit)
        
        # 2. Cache hit: return immediately
        if history:
            logger.debug(f"[Memory] 🎯 L1 Cache Hit [Session: {session_id}] | Count: {len(history)}")
            return history
            
        # 3. Cache miss: initiate emergency reload from L2
        if self.l2_storage:
            logger.warning(f"[Memory] ⚠️ L1 Cache Miss, initiating emergency reload from L2... [Session: {session_id}]")
            l2_history = await self.l2_storage.get_history(session_id, limit=limit)
            
            if l2_history:
                # 4. Swipe back to L1 in reverse order to reheat it
                for msg in l2_history:
                    await self.l1_cache.save_message(session_id, msg["role"], msg["content"])
                return l2_history
                
        return []

    async def save_message(self, session_id: str, role: str, content: str) -> None:
        """
        Dual-write strategy: L1 writes synchronously for immediate reading; L2 coroutines brush the disk asynchronously, never blocking the main bus.
        """
        if not content:
            return

        # 1. Synchronous fast lane: write to Redis L1 cache (complete at extreme speed)
        await self.l1_cache.save_message(session_id, role, content)
        
        # 2. Asynchronous slow lane: bypass and swipe into the L2 persistence layer
        if self.l2_storage:

            task = asyncio.create_task(
                self._safe_l2_write(session_id, role, content)
            )
            self._background_tasks.add(task)
            task.add_done_callback(self._background_tasks.discard)

    async def _safe_l2_write(self, session_id: str, role: str, content: str):
        try:
            await self.l2_storage.save_message(session_id, role, content)
        except Exception as e:
            logger.error(f"[Memory] ❌ Asynchronous L2 persistence failed [Session: {session_id}]: {e}")

    async def clear(self, session_id: str) -> None:
        """Dual-layer physical destruction, ensuring GDPR-level data privacy erasure"""
        await self.l1_cache.clear(session_id)
        if self.l2_storage:
            await self.l2_storage.clear(session_id)
        logger.info(f"[Memory] 🗑️ All traces of the double-layer storage of the session {session_id} have been physically erased。")