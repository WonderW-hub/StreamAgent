"""异步任务挂起与 Redis 回调唤醒引擎"""
import asyncio
import logging
from typing import Dict, Any, Optional
from stream_agent.core.envelope import EventEnvelope

logger = logging.getLogger("StreamAgent.FuturePool")

class FuturePool:
    """
    异步任务挂起与回调唤醒引擎 (The "Call Buzzer" System)
    用于在无状态的 HTTP 请求与异步的 Redis Stream 之间建立对应关系。
    """
    def __init__(self):
        # 存放形如 { "trace_id": asyncio.Future } 的映射字典
        self._pool: Dict[str, asyncio.Future] = {}

    def create_future(self, trace_id: str) -> asyncio.Future:
        """
        为即将发往总线的请求创建一个“空白的取餐呼叫器” (Future)。
        """
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self._pool[trace_id] = future
        logger.debug(f"已创建 Future 挂起钩子: {trace_id}")
        return future

    def resolve_future(self, trace_id: str, envelope: EventEnvelope) -> bool:
        """
        当后台监听器从 Redis 收到结果时，调用此方法唤醒对应的 HTTP 请求。
        """
        future = self._pool.pop(trace_id, None)
        if future and not future.done():
            future.set_result(envelope)
            logger.debug(f"已精准唤醒 Future 钩子: {trace_id}")
            return True
        else:
            logger.warning(f"无法唤醒: 找不到对应的 Future 或已超时丢弃 (Trace: {trace_id})")
            return False

    def remove_future(self, trace_id: str):
        """超时或异常时，清理内存中的残余 Future"""
        self._pool.pop(trace_id, None)