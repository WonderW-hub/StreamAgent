import json
import logging
from typing import List, Tuple
from pydantic import BaseModel

from stream_agent.utils.llm_engine import AsyncLLMEngine

logger = logging.getLogger("StreamAgent.Router")

class AgentRoute(BaseModel):
    name: str
    description: str

class LLMIntentRouter:
    """
    Plug-and-play large-model intent analyzer engine.
    A complete closed loop of LLM calls and JSON parsing is internally encapsulated to help the supervisor map the natural language to the exact agent name.
    """
    def __init__(self, llm_engine: AsyncLLMEngine):
        """
        :param llm_engine: A plug-and-play LLM engine that implements AsyncLLMEngine
        """

        self.llm = llm_engine

    def _build_system_prompt(self, routes: List[AgentRoute], fallback_agent: str) -> str:
        route_descriptions = "\n".join([f"- {r.name}: {r.description}" for r in routes])
        
        return f"""You are a central triage desk in a multi-agent system.
Your task is to allocate user inputs to the most suitable expert agents.

【Available Agents】
{route_descriptions}

【Rules】
1. You can only choose one most matching agent name from the above list.
2. If no agent matches, please output "{fallback_agent}".
3. Your output must be a valid JSON object with the following format:
{{
    "target_agent": "Selected agent name",
    "reason": "Brief allocation reason"
}}
Never output anything other than the JSON object (do not include markdown tags)."""

    async def decide_target(self, query: str, routes: List[AgentRoute], fallback_agent: str = "supervisor") -> Tuple[str, str]:
        """
        Core public method: Pass in user questions and available routes, directly return the decision result.
        Returns:
            Tuple[str, str]: (Target Agent name, Allocation reason)
        """
        if not query:
            return fallback_agent, "User input is empty"


        system_prompt = self._build_system_prompt(routes,fallback_agent)
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": query}
        ]
        

        logger.debug("🧠 [Router] Requesting a large model for intent routing analysis...")
        try:
            result_json = await self.llm.generate_json(messages=messages, temperature=0.0)
        except Exception as e:
            logger.error(f"[Router] The large model routing call completely crashed: {e}")
            return fallback_agent, "The large model service isException, triggering a security downgrade"

        target = result_json.get("target_agent", fallback_agent)
        reason = result_json.get("reason", "No reason provided")

        valid_targets = [r.name for r in routes] + [fallback_agent]
        if target not in valid_targets:
            logger.warning(f"⚠️ [Router] Intercepted the illusion of a big model!Fictitious route'{target}', has been forcibly downgraded。")
            return fallback_agent, f"The large model generated a non-existent route: {target}"
            
        return target, reason