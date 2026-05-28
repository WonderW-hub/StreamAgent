# src/stream_agent/agents/planner/agent.py

import json
import logging
import uuid
from typing import Dict, Any, List

from stream_agent.worker.base import WorkerBase
from stream_agent.utils.llm_engine import AsyncLLMEngine 
from stream_agent.agents.planner.prompts import PLANNER_SYSTEM_PROMPT
from stream_agent.core.context import SessionContext 

logger = logging.getLogger(__name__)

class PlannerAgent(WorkerBase):
    def __init__(self, agent_name: str = "planner_agent"):
        super().__init__(agent_name=agent_name)
        self.llm_engine = AsyncLLMEngine() 

    async def _get_active_agents(self) -> List[str]:
        """
        Global awareness: Get all currently alive agents from Redis.
        Suppose other agents executed at startup: redis.sadd("system:active_agents", self.agent_name)
        """
        try:
            # Get all active members in Redis Set
            active_members = await self.redis.smembers("system:active_agents")
            if active_members:
                # Redis returns bytes, which needs to be decrypted
                agents = [str(m) for m in active_members]
            else:
                # The bottom mechanism assumes that at least these two basic support agents exist.
                agents = ["coder_agent", "sandbox_agent"] 
            
            logger.info(f"Globally perceived active Agent: {agents}")
            return agents
        except Exception as e:
            logger.error(f"Failed to obtain the global agent list: {e}")
            return ["coder_agent", "default_agent"]

    async def handle_event(self, payload: dict) -> dict:
        session_id = SessionContext.get_session_id()
        trace_id = SessionContext.get_trace_id()
        
        user_goal = payload.get("query", "")
        auth_token = payload.get("auth_token")

        if not user_goal:
            return {"status": "error", "message": "Mission target is empty"}

        if not self._validate_token(auth_token):
            return {"status": "error", "message": "Unauthorized request"}

        # 1. Trigger global perception
        active_agents = await self._get_active_agents()
        agents_str = ", ".join(active_agents)

        # 2. Dynamic assembly perception Prompt
        formatted_prompt = PLANNER_SYSTEM_PROMPT.format(active_agents=agents_str)
        
        messages = [
            {"role": "system", "content": formatted_prompt},
            {"role": "user", "content": f"【Current goal】\n{user_goal}"}
        ]
        
        # 3. LLM dispatch
        parsed_response = await self.llm_engine.generate_json(
            messages=messages, 
            temperature=0.1 
        )
        
        tasks = parsed_response.get("tasks", [])
        
        if not tasks:
            tasks = [{
                "step_id": 1, 
                "agent_type": "coder_agent", 
                "instruction": f"The system planning failed, please complete the goal as a stand-in for the bottom of the pocket: {user_goal}"
            }]

        # 4. State machine persistence
        pipeline_key = f"pipeline:{session_id}:{trace_id}"
        pipeline_data = {
            "status": "RUNNING",
            "current_step": 1,
            "total_steps": len(tasks),
            "tasks": json.dumps(tasks) 
        }
        
        await self.redis.hset(pipeline_key, mapping=pipeline_data)
        await self.redis.expire(pipeline_key, 86400) 
        
        # 5. Distribute the first task
        first_task = tasks[0]
        await self.dispatch_step(first_task, session_id, trace_id, auth_token, pipeline_key)

        # ===================================================================
        # 【核心修改】直接返回 None，阻止 WorkerBase 向 Gateway 发送提前结束的 ACK 回执。
        # 真正的最终结果将由最后一棒（比如 WriterAgent）触发 _advance_pipeline 里的回传逻辑来完成。
        # ===================================================================
        return None

    def _validate_token(self, token: str) -> bool:
        if not token:
            return False
        valid_prefix = "Bearer "
        if token.startswith(valid_prefix) and len(token) > 10:
            return True
        return False

    async def dispatch_step(self, task: Dict[str, Any], session_id: str, trace_id: str, auth_token: str, pipeline_key: str):
        envelope_data = {
            "trace_id": trace_id,
            "session_id": session_id,
            "auth_token": auth_token,
            "source": self.agent_name,
            "target": task["agent_type"],
            "payload": json.dumps({
                "instruction": task["instruction"], 
                "step_id": task["step_id"],
                "pipeline_id": pipeline_key
            })
        }
        
        stream_name = f"bus:events:{task['agent_type']}"
        await self.redis.xadd(stream_name, envelope_data)
        logger.info(f"[Trace: {trace_id}] The step {task['step_id']} has been pushed to {stream_name}")