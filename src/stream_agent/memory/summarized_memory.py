# src/stream_agent/memory/summarized_memory.py
import asyncio
import logging
import json
from typing import List, Dict, Optional

from stream_agent.memory.layered import LayeredMemoryManager
from stream_agent.memory.base import BaseMemory
from stream_agent.utils.llm_engine import AsyncLLMEngine

logger = logging.getLogger("StreamAgent.SummarizedMemory")

class SummarizedMemoryManager(LayeredMemoryManager):
    """
    A hierarchical memory manager with asynchronous automatic summary function.
    When the L1 (Redis) cache reaches the threshold value, the background LLM summary task is automatically triggered，
    Compress old messages while strictly maintaining Session ID-level isolation.
    """
    def __init__(
        self, 
        agent_name: str,
        redis_url: str = "redis://localhost:6379/0",
        l1_max_len: int = 10,
        l2_plugin: Optional[BaseMemory] = None,
        compress_ratio: float = 0.8,  # Compression is triggered when the record reaches 80% of the maximum length
        retain_ratio: float = 0.4,    # After compression, keep the latest 40% of the living memory is not compressed
    ):
        super().__init__(agent_name, redis_url, l1_max_len, l2_plugin)
        self.l1_max_len = l1_max_len
        self.compress_ratio = compress_ratio
        self.retain_ratio = retain_ratio
        
        self.llm = AsyncLLMEngine()
        self._summarize_locks: Dict[str, asyncio.Lock] = {}
        # Strongly reference collections to prevent asynchronous background tasks from being garbage collected
        self._background_tasks = set()

    def _get_session_lock(self, session_id: str) -> asyncio.Lock:
        if session_id not in self._summarize_locks:
            self._summarize_locks[session_id] = asyncio.Lock()
        return self._summarize_locks[session_id]

    async def get_history(self, session_id: str, limit: int = 10) -> List[Dict[str, str]]:
        """
        Get historical records and automatically inject the latest memory summary at the front。
        """
        # 1. Get regular hierarchical history (priority L1, missed penetration L2)
        raw_history = await super().get_history(session_id, limit)
        
        # 2. Get the session-specific persistence summary from Redis
        summary_key = f"memory:summary:{session_id}"
        current_summary = await self.l1_cache.redis.get(summary_key)
        
        # 3. Dynamic stitching: If there is a summary, inject it as a supplement to System Prompt
        if current_summary:
            summary_message = {
                "role": "system",
                "content": f"【Summary of Historical Memory】:\n{current_summary}\nPlease combine the above summary to understand the user's follow-up request。"
            }
            # Insert the summary to the front of the history
            raw_history.insert(0, summary_message)
            logger.debug(f"[Memory] Summary of Injecting Historical Memory,Session: {session_id}")
            
        return raw_history

    async def save_message(self, session_id: str, role: str, content: str) -> None:
        """
        Save the message and bypass the asynchronous summary task when the proportional conditions are met, never blocking the main thread.
        """
        await super().save_message(session_id, role, content)
        
        history = await self.l1_cache.get_history(session_id, limit=self.l1_max_len)
        
        # Dynamically calculate the trigger threshold (rounded down, at least 1)
        trigger_threshold = max(1, int(self.l1_max_len * self.compress_ratio))
        
        if len(history) >= trigger_threshold:
            task = asyncio.create_task(self._safe_async_summarize(session_id, history))
            self._background_tasks.add(task)
            task.add_done_callback(self._background_tasks.discard)

    async def _safe_async_summarize(self, session_id: str, history: List[Dict[str, str]]):
        lock = self._get_session_lock(session_id)
        if lock.locked():
            return  

        async with lock:
            try:
                summary_key = f"memory:summary:{session_id}"
                old_summary = await self.l1_cache.redis.get(summary_key) or "无"
                
                # Dynamically calculate the number of latest messages that need to be kept in the L1 cache
                retain_count = max(1, int(self.l1_max_len * self.retain_ratio))
                
                # Extract the earliest conversations that need to be compressed (exclude the latest retain_data that needs to be retained)
                messages_to_compress = history[:-retain_count]
                if not messages_to_compress:
                    return

                history_text = "\n".join([f"{msg['role']}: {msg['content']}" for msg in messages_to_compress])
                
                prompt = f"""You are a professional memory compression engine.Please combine [Old Memory Summary] and [New Dialogue Record] to extract a comprehensive and concise [New Memory Summary].
requirements：
1. Contains the user's core preferences and the status of key tasks that have been completed.
2. Eliminate invalid greetings.
3. Strictly output a plain text summary, without any superfluous explanations.

【Summary of Old Memories】：
{old_summary}

【New conversation record】：
{history_text}
"""
                messages = [{"role": "user", "content": prompt}]
                logger.info(f"[Memory] Start the bypass memory compression task (dynamic threshold),Session: {session_id} ...")
                
                new_summary = await self.llm.generate_text(
                    messages=messages, 
                    temperature=0.1, 
                    max_tokens=512
                )
                
                await self.l1_cache.redis.set(summary_key, new_summary, ex=2592000)
                
                # Dynamically trim the L1 cache, keeping only the latest retain_data records
                key = self.l1_cache._get_key(session_id)
                await self.l1_cache.redis.ltrim(key, -retain_count, -1)
                
                logger.info(f"[Memory] ✅ The memory compression is completed and the L1 sliding window is updated (keep the latest {retain_data} ),Session: {session_id}")
                
            except Exception as e:
                logger.error(f"[Memory] ❌ Asynchronous memory compression failed,Session: {session_id} - Error: {str(e)}", exc_info=True)
        
    async def query_memory_summary(self, session_id: str) -> str:
        """
        Explicit query interface: allows the agent or front-end to directly retrieve the compressed memory status quo of the current session
        """
        summary_key = f"memory:summary:{session_id}"
        current_summary = await self.l1_cache.redis.get(summary_key)
        
        if not current_summary:
            # If Redis jitter causes the summary to be lost, extract the most recent records from the L2 cold storage to temporarily generate or return a prompt
            return "There is no long-term memory summary of the current session."
            
        return current_summary

    async def clear(self, session_id: str) -> None:
        """
        Physically destroy two-layer storage and summary records to ensure data privacy and erasure.
        """
        await super().clear(session_id)
        summary_key = f"memory:summary:{session_id}"
        await self.l1_cache.redis.delete(summary_key)
        
        # 清理内存锁
        if session_id in self._summarize_locks:
            del self._summarize_locks[session_id]
        logger.info(f"[Memory] 🗑️ The memory summary of the session {session_id} has been completely erased.")
