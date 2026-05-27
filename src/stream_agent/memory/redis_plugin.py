#src\stream_agent\memory\redis_plugin.py
"""Redis Memory system implementation"""
import json
import logging
from typing import List, Dict, Any
import redis.asyncio as redis

from stream_agent.memory.base import BaseMemory

logger = logging.getLogger("StreamAgent.Memory")

class RedisMemoryPlugin(BaseMemory):
    """
    An industrial-grade stateful multi-round memory plug-in implemented based on Redis List.
    Built-in sliding window truncation mechanism (to prevent the context from exceeding the maximum token limit of LLM) and TTL automatic recovery mechanism.
    """
    def __init__(
        self, 
        redis_url: str = "redis://localhost:6379/0", 
        max_len: int = 20, 
        ttl: int = 86400
    ):
        """
        Args:
            redis_url: Redis connection address
            max_len: The maximum number of message rounds allowed for a session (sliding window size)
            ttl: Memory survival time (seconds), default 24 hours of no interaction will automatically release memory, prevent OOM
        """
        self.redis = redis.from_url(redis_url, decode_responses=True)
        self.max_len = max_len
        self.ttl = ttl

    def _get_key(self, session_id: str) -> str:
        """Generate Redis Key"""
        return f"memory:session:{session_id}"

    async def get_history(self, session_id: str, limit: int = 10) -> List[Dict[str, str]]:
        """Read history message packets from Redis in reverse or sequential order"""
        key = self._get_key(session_id)
        try:
            # read read the most recent limit records (Redis stores JSON strings)
            raw_messages = await self.redis.lrange(key, -limit, -1)
            
            history = []
            for raw_msg in raw_messages:
                history.append(json.loads(raw_msg))
                
            return history
        except Exception as e:
            logger.error(f"Reading Redis memory failed [Session: {session_id}]: {e}")
            return []

    async def save_message(self, session_id: str, role: str, content: str) -> None:
        """Save a new message to Redis and execute sliding window truncation and TTL maintenance"""
        if not content:
            return
            
        key = self._get_key(session_id)
        message_packet = {"role": role, "content": content}
        
        try:
            async with self.redis.pipeline(transaction=True) as pipe:

                pipe.rpush(key, json.dumps(message_packet, ensure_ascii=False))

                pipe.ltrim(key, -self.max_len, -1)

                pipe.expire(key, self.ttl)
                
                await pipe.execute()
        except Exception as e:
            logger.error(f"Writing to Redis memory failed [Session: {session_id}]: {e}")

    async def clear(self, session_id: str) -> None:
        """Physically delete all memory traces of a user"""
        key = self._get_key(session_id)
        await self.redis.delete(key)
        logger.info(f"🗑️ All history memory of session {session_id} has been completely destroyed in Redis.")