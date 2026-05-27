import asyncio
import logging
from typing import List, Dict, Any, Optional

from stream_agent.memory.base import BaseMemory
from stream_agent.memory.redis_plugin import RedisMemoryPlugin

logger = logging.getLogger("StreamAgent.LayeredMemory")

class LayeredMemoryManager(BaseMemory):
    """
    工业级 L1(热/Redis) + L2(冷/持久层) 异步分层记忆系统管理器。
    
    保障：
    1. 读操作优先击中 L1 缓存，保障高并发下的毫秒级延迟。
    2. 写操作对 L2 实施旁路异步刷盘，不占用 LLM 交互主链响应时间。
    3. 自带自动热重载机制，防御 Redis 内存抖动或缓存失效。
    """
    def __init__(
        self, 
        agent_name: str,
        redis_url: str = "redis://localhost:6379/0",
        l1_max_len: int = 10,  # L1 缓存保留最近 10 轮(20条消息)
        l2_plugin: Optional[BaseMemory] = None  # 允许注入任何实现了 BaseMemory 的冷库插件
    ):
        self.agent_name = agent_name
        
        # 1. 初始化 L1 热缓存层
        self.l1_cache = RedisMemoryPlugin(redis_url=redis_url, max_len=l1_max_len * 2, ttl=86400)
        
        # 2. 初始化 L2 冷持久化层 (若无注入，后续默认使用本地异步存储或抛出异常)
        self.l2_storage = l2_plugin
        
        # 维护当前内存中正在执行的异步刷盘任务集合，防止因对象销毁导致垃圾回收
        self._background_tasks = set()

    async def get_history(self, session_id: str, limit: int = 10) -> List[Dict[str, str]]:
        """
        全自动化分层读取策略 (带有自动 Hydration 机制)
        """
        # 1. 尝试从 L1 缓存拉取
        history = await self.l1_cache.get_history(session_id, limit=limit)
        
        # 2. 缓存击中：直接返回
        if history:
            logger.debug(f"[Memory] 🎯 L1 缓存击中 [Session: {session_id}] | 数量: {len(history)}")
            return history
            
        # 3. 缓存失效 (Cache Miss)：启动重载救护车
        if self.l2_storage:
            logger.warning(f"[Memory] ⚠️ L1 缓存未击中，正在从 L2 持久层紧急重载历史... [Session: {session_id}]")
            l2_history = await self.l2_storage.get_history(session_id, limit=limit)
            
            if l2_history:
                # 4. 逆序反刷回 L1，使其重新热起来
                for msg in l2_history:
                    await self.l1_cache.save_message(session_id, msg["role"], msg["content"])
                return l2_history
                
        return []

    async def save_message(self, session_id: str, role: str, content: str) -> None:
        """
        双写策略：L1 同步写以供立即读取；L2 协程异步刷盘，绝不阻塞主总线。
        """
        if not content:
            return

        # 1. 同步快车道：写入 Redis L1 缓存 (极速完成)
        await self.l1_cache.save_message(session_id, role, content)
        
        # 2. 异步慢车道：旁路刷入 L2 持久层
        if self.l2_storage:
            # 建立一个在后台运行的独立执行任务
            task = asyncio.create_task(
                self._safe_l2_write(session_id, role, content)
            )
            # 必须将其加入全局集合中，防止运行到一半被 Python 强行垃圾回收
            self._background_tasks.add(task)
            # 绑定回调，任务执行完毕后自动从集合中清除
            task.add_done_callback(self._background_tasks.discard)

    async def _safe_l2_write(self, session_id: str, role: str, content: str):
        """后台异步刷盘安全上下文隔离保护"""
        try:
            await self.l2_storage.save_message(session_id, role, content)
        except Exception as e:
            logger.error(f"[Memory] ❌ 异步持久化 L2 失败 [Session: {session_id}]: {e}")

    async def clear(self, session_id: str) -> None:
        """双层物理销毁，确保 GDPR 级数据隐私擦除"""
        await self.l1_cache.clear(session_id)
        if self.l2_storage:
            await self.l2_storage.clear(session_id)
        logger.info(f"[Memory] 🗑️ 会话 {session_id} 的双层存储痕迹已全部物理擦除。")