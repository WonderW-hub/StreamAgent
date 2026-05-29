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
    带有异步自动摘要功能的分层记忆管理器。
    当 L1 (Redis) 缓存达到阈值时，自动触发后台 LLM 摘要任务，
    将旧消息压缩，同时严格保持 Session ID 级别的隔离。
    """
    def __init__(
        self, 
        agent_name: str,
        redis_url: str = "redis://localhost:6379/0",
        l1_max_len: int = 10,
        l2_plugin: Optional[BaseMemory] = None,
        compress_ratio: float = 0.8,  # 当记录达到最大长度的 80% 时触发压缩
        retain_ratio: float = 0.4,    # 压缩后，保留最新的 40% 鲜活记忆不被压缩
    ):
        super().__init__(agent_name, redis_url, l1_max_len, l2_plugin)
        self.l1_max_len = l1_max_len
        self.compress_ratio = compress_ratio
        self.retain_ratio = retain_ratio
        
        self.llm = AsyncLLMEngine()
        self._summarize_locks: Dict[str, asyncio.Lock] = {}
        # 强引用集合，防止 asyncio 后台任务被垃圾回收
        self._background_tasks = set()

    def _get_session_lock(self, session_id: str) -> asyncio.Lock:
        if session_id not in self._summarize_locks:
            self._summarize_locks[session_id] = asyncio.Lock()
        return self._summarize_locks[session_id]

    async def get_history(self, session_id: str, limit: int = 10) -> List[Dict[str, str]]:
        """
        获取历史记录，并自动在最前方注入最新的记忆摘要。
        """
        # 1. 获取常规的分层历史记录（优先 L1，未命中穿透 L2）
        raw_history = await super().get_history(session_id, limit)
        
        # 2. 从 Redis 获取该 session 专属的持久化摘要
        summary_key = f"memory:summary:{session_id}"
        current_summary = await self.l1_cache.redis.get(summary_key)
        
        # 3. 动态拼接：如果存在摘要，将其作为 System Prompt 的补充注入
        if current_summary:
            summary_message = {
                "role": "system",
                "content": f"【历史记忆摘要】：\n{current_summary}\n请结合上述摘要理解用户的后续请求。"
            }
            # 将摘要插入到历史记录的最前面
            raw_history.insert(0, summary_message)
            logger.debug(f"[Memory] 注入历史记忆摘要，Session: {session_id}")
            
        return raw_history

    async def save_message(self, session_id: str, role: str, content: str) -> None:
        """
        保存消息，并在满足比例条件时旁路触发异步摘要任务，绝不阻塞主线程。
        """
        await super().save_message(session_id, role, content)
        
        history = await self.l1_cache.get_history(session_id, limit=self.l1_max_len)
        
        # 动态计算触发阈值 (向下取整，至少为 1)
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
                
                # 动态计算需要保留在 L1 缓存中的最新消息数
                retain_count = max(1, int(self.l1_max_len * self.retain_ratio))
                
                # 提取需要被压缩的最早期对话（排除掉需要保留的最新的 retain_count 条）
                messages_to_compress = history[:-retain_count]
                if not messages_to_compress:
                    return

                history_text = "\n".join([f"{msg['role']}: {msg['content']}" for msg in messages_to_compress])
                
                prompt = f"""你是一个专业的记忆压缩引擎。请结合【旧的记忆摘要】和【新增的对话记录】，提炼出一个全面、简练的【新记忆摘要】。
要求：
1. 包含用户的核心偏好、已完成的关键任务状态。
2. 剔除无效的寒暄。
3. 严格输出纯文本摘要，不要有任何多余的解释。

【旧的记忆摘要】：
{old_summary}

【新增的对话记录】：
{history_text}
"""
                messages = [{"role": "user", "content": prompt}]
                logger.info(f"[Memory] 启动旁路记忆压缩任务 (动态阈值)，Session: {session_id} ...")
                
                new_summary = await self.llm.generate_text(
                    messages=messages, 
                    temperature=0.1, 
                    max_tokens=512
                )
                
                await self.l1_cache.redis.set(summary_key, new_summary, ex=2592000)
                
                # 动态修剪 L1 缓存，只保留最新的 retain_count 条记录
                key = self.l1_cache._get_key(session_id)
                await self.l1_cache.redis.ltrim(key, -retain_count, -1)
                
                logger.info(f"[Memory] ✅ 记忆压缩完成并更新 L1 滑动窗口 (保留最新 {retain_count} 条)，Session: {session_id}")
                
            except Exception as e:
                logger.error(f"[Memory] ❌ 异步记忆压缩失败，Session: {session_id} - Error: {str(e)}", exc_info=True)
        
    async def query_memory_summary(self, session_id: str) -> str:
        """
        显式查询接口：允许 Agent 或前端直接调取当前会话的压缩记忆现状
        """
        summary_key = f"memory:summary:{session_id}"
        current_summary = await self.l1_cache.redis.get(summary_key)
        
        if not current_summary:
            # 如果 Redis 抖动导致摘要丢失，从 L2 冷存储中提取最近记录临时生成或返回提示
            return "当前会话暂无长期记忆摘要。"
            
        return current_summary

    async def clear(self, session_id: str) -> None:
        """
        物理销毁双层存储以及摘要记录，确保数据隐私擦除。
        """
        await super().clear(session_id)
        summary_key = f"memory:summary:{session_id}"
        await self.l1_cache.redis.delete(summary_key)
        
        # 清理内存锁
        if session_id in self._summarize_locks:
            del self._summarize_locks[session_id]
        logger.info(f"[Memory] 🗑️ 会话 {session_id} 的记忆摘要已被完全擦除。")
