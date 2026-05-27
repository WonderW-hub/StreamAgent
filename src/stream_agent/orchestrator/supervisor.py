import logging
from typing import Dict, Any, Optional, List

from stream_agent.worker.base import WorkerBase
from stream_agent.core.envelope import EventEnvelope
from stream_agent.core.context import SessionContext
# 导入我们刚刚重构的完全体引擎
from stream_agent.orchestrator.router import LLMIntentRouter, AgentRoute 
from stream_agent.utils.llm_engine import AsyncLLMEngine

logger = logging.getLogger("StreamAgent.Supervisor")

class Supervisor(WorkerBase):
    """
    分诊总控核心节点 (Orchestrator)
    负责接收网关流量，将决策权移交给 LLMIntentRouter 进行意图识别，并实施“三角路由”转发。
    """
    def __init__(self, agent_name: str = "supervisor", version: str = "v1.0", redis_url: str = "redis://localhost:6379/0"):
        super().__init__(agent_name, version, redis_url)
        self.routes: List[AgentRoute] = []
        
        # 实例化大模型，并将其注入给专门的路由引擎
        self.llm = AsyncLLMEngine()
        self.router = LLMIntentRouter(self.llm)

    def register_agent(self, target_name: str, description: str):
        """注册下游专职 Agent 的能力描述，供大模型路由参考"""
        self.routes.append(AgentRoute(name=target_name, description=description))
        logger.info(f"[{self.agent_name}] 已注册下游路由: {target_name}")

    async def determine_target(self, payload: Dict[str, Any]) -> str:
        """
        全权委托 Router 引擎进行路由决策。
        """
        user_query = payload.get("query", "")
        
        # 直接调用 Router 的完整闭环方法
        target, reason = await self.router.decide_target(
            query=user_query, 
            routes=self.routes, 
            fallback_agent=self.agent_name
        )
        
        logger.info(f"[{self.agent_name}] 🎯 决策完毕 -> 目标: {target} | 理由: {reason}")
        return target

    async def handle_event(self, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        重写 WorkerBase 的业务执行方法。执行三角路由。
        """
        target_agent = await self.determine_target(payload)
        
        if target_agent == self.agent_name or not target_agent:
            return {"summary": "对不起，总诊台目前无法理解并分发您的请求。"}

        trace_id = SessionContext.get_trace_id()
        forward_envelope = EventEnvelope(
            trace_id=trace_id,
            session_id=SessionContext.get_session_id(),
            auth_token=SessionContext.get_auth_token(),
            is_shadow=SessionContext.is_shadow_mode(),
            source="gateway", 
            target=target_agent,
            action="process",
            payload=payload
        )

        target_stream = f"bus:events:{target_agent}"
        await self.redis.xadd(target_stream, forward_envelope.to_redis_dict(), maxlen=10000, approximate=True)
        logger.info(f"[{self.agent_name}] 🔀 路由转发完成 | Trace: {trace_id} -> Stream: {target_stream}")

        return None