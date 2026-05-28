# \src\stream_agent\orchestrator\supervisor.py
import logging
from typing import Dict, Any, Optional, List

from stream_agent.worker.base import WorkerBase
from stream_agent.core.envelope import EventEnvelope
from stream_agent.core.context import SessionContext

from stream_agent.orchestrator.router import LLMIntentRouter, AgentRoute 
from stream_agent.utils.llm_engine import AsyncLLMEngine

logger = logging.getLogger("StreamAgent.Supervisor")

class Supervisor(WorkerBase):
    """
    Triage master control core node (Orchestrator)
    Responsible for receiving gateway traffic, transferring decision-making power to LLMIntentRouter for intent identification, and implementing “triangular routing” forwarding.
    """
    def __init__(self, agent_name: str = "supervisor", version: str = "v1.0", redis_url: str = "redis://localhost:6379/0"):
        super().__init__(agent_name, version, redis_url)
        self.routes: List[AgentRoute] = []
        self.llm = AsyncLLMEngine()
        self.router = LLMIntentRouter(self.llm)

    def register_agent(self, target_name: str, description: str):
        """Register an agent to the supervisor."""
        self.routes.append(AgentRoute(name=target_name, description=description))
        logger.info(f"[{self.agent_name}] Registered downstream route: {target_name}")

    async def determine_target(self, payload: Dict[str, Any]) -> str:

        user_query = payload.get("query", "")
        
        target, reason = await self.router.decide_target(
            query=user_query, 
            routes=self.routes, 
            fallback_agent=self.agent_name
        )
        
        logger.info(f"[{self.agent_name}] 🎯 Decision completed -> Target: {target} | Reason: {reason}")
        return target

    async def handle_event(self, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:

        target_agent = await self.determine_target(payload)
        
        if target_agent == self.agent_name or not target_agent:
            return {"summary": "Sorry, the triage desk currently cannot understand and dispatch your request."}

        trace_id = SessionContext.get_trace_id()
        forward_envelope = EventEnvelope(
            trace_id=trace_id,
            session_id=SessionContext.get_session_id(),
            auth_token=SessionContext.get_auth_token(),
            is_shadow=SessionContext.is_shadow_mode(),
            source=SessionContext.get_source(),
            target=target_agent,
            action="process",
            payload=payload
        )

        target_stream = f"bus:events:{target_agent}"
        await self.redis.xadd(target_stream, forward_envelope.to_redis_dict(), maxlen=10000, approximate=True)
        logger.info(f"[{self.agent_name}] 🔀 Route forwarding completed | Trace: {trace_id} -> Stream: {target_stream}")

        return None