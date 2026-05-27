"""Redis 记忆系统实现"""
import json
import logging
from typing import List, Dict, Any
import redis.asyncio as redis

from stream_agent.memory.base import BaseMemory

logger = logging.getLogger("StreamAgent.Memory")

class RedisMemoryPlugin(BaseMemory):
    """
    基于 Redis List 实现的工业级有状态多轮记忆插件。
    内置滑动窗口截断机制（防止上下文超出LLM最大Token限制）与 TTL 自动回收机制。
    """
    def __init__(
        self, 
        redis_url: str = "redis://localhost:6379/0", 
        max_len: int = 20, 
        ttl: int = 86400
    ):
        """
        Args:
            redis_url: Redis 连接地址
            max_len: 某个会话允许留存的最大消息轮数（滑动窗口大小）
            ttl: 记忆存活时间（秒），默认 24 小时无人交互则自动释放内存，防 OOM
        """
        self.redis = redis.from_url(redis_url, decode_responses=True)
        self.max_len = max_len
        self.ttl = ttl

    def _get_key(self, session_id: str) -> str:
        """生成严格带隔离前缀的 Redis Key"""
        return f"memory:session:{session_id}"

    async def get_history(self, session_id: str, limit: int = 10) -> List[Dict[str, str]]:
        """从 Redis 逆序或顺序读取历史消息包"""
        key = self._get_key(session_id)
        try:
            # 读取最近的 limit 条记录 (Redis 存储的是 JSON 字符串)
            raw_messages = await self.redis.lrange(key, -limit, -1)
            
            history = []
            for raw_msg in raw_messages:
                history.append(json.loads(raw_msg))
                
            return history
        except Exception as e:
            logger.error(f"读取 Redis 记忆失败 [Session: {session_id}]: {e}")
            return []

    async def save_message(self, session_id: str, role: str, content: str) -> None:
        """向 Redis 压入新记忆，并执行滑动窗口截断与生存期维护"""
        if not content:
            return
            
        key = self._get_key(session_id)
        message_packet = {"role": role, "content": content}
        
        try:
            async with self.redis.pipeline(transaction=True) as pipe:
                # 1. 压入队列尾部
                pipe.rpush(key, json.dumps(message_packet, ensure_ascii=False))
                # 2. 强行截断，只保留最新的 max_len 条，防止因对话过长吃干 Redis 内存
                pipe.ltrim(key, -self.max_len, -1)
                # 3. 刷新生存时间（滚动续期）
                pipe.expire(key, self.ttl)
                
                await pipe.execute()
        except Exception as e:
            logger.error(f"写入 Redis 记忆失败 [Session: {session_id}]: {e}")

    async def clear(self, session_id: str) -> None:
        """物理删除某个用户的全部记忆痕迹"""
        key = self._get_key(session_id)
        await self.redis.delete(key)
        logger.info(f"🗑️ 会话 {session_id} 的历史记忆已在 Redis 中彻底销毁。")