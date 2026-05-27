import json
import logging
from typing import List, Tuple
from pydantic import BaseModel

# 引入大模型引擎
from stream_agent.utils.llm_engine import AsyncLLMEngine

logger = logging.getLogger("StreamAgent.Router")

class AgentRoute(BaseModel):
    name: str
    description: str

class LLMIntentRouter:
    """
    即插即用的大模型意图分析器引擎。
    内部封装了完整的 LLM 调用与 JSON 解析闭环，帮助 Supervisor 将自然语言映射为确切的 Agent Name。
    """
    def __init__(self, llm_engine: AsyncLLMEngine):
        # 路由引擎现在拥有了自己的大脑实例
        self.llm = llm_engine

    def _build_system_prompt(self, routes: List[AgentRoute]) -> str:
        """内部方法：构建动态路由提示词"""
        route_descriptions = "\n".join([f"- {r.name}: {r.description}" for r in routes])
        
        return f"""你是一个多智能体系统的中央分诊台。
你的任务是根据用户的输入，将其分配给最合适的专家智能体。

【可用智能体列表】
{route_descriptions}

【规则】
1. 你只能从上述列表中选择一个最匹配的智能体名称。
2. 如果没有任何智能体匹配，请输出 "supervisor"。
3. 你的输出必须是一个合法的 JSON 对象，格式如下：
{{
    "target_agent": "选定的智能体名称",
    "reason": "简短的分配理由"
}}
绝不允许输出 JSON 以外的任何内容（不要带 markdown 标记）。"""

    async def decide_target(self, query: str, routes: List[AgentRoute], fallback_agent: str = "supervisor") -> Tuple[str, str]:
        """
        核心公开方法：传入用户问题和可用路由，直接返回决策结果。
        Returns:
            Tuple[str, str]: (目标 Agent 名称, 分配理由)
        """
        if not query:
            return fallback_agent, "用户输入为空"

        # 1. 组装上下文
        system_prompt = self._build_system_prompt(routes)
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": query}
        ]
        
        # 2. 召唤大模型进行严谨的 JSON 结构化提取
        logger.debug("🧠 [Router] 正在请求大模型进行意图路由分析...")
        try:
            result_json = await self.llm.generate_json(messages=messages, temperature=0.0)
        except Exception as e:
            logger.error(f"[Router] 大模型路由调用彻底崩溃: {e}")
            return fallback_agent, "大模型服务异常，触发安全降级"

        # 3. 解析与校验
        target = result_json.get("target_agent", fallback_agent)
        reason = result_json.get("reason", "未提供理由")
        
        # 4. 安全防火墙：防止大模型产生幻觉，虚构出一个不存在的队列
        valid_targets = [r.name for r in routes] + [fallback_agent]
        if target not in valid_targets:
            logger.warning(f"⚠️ [Router] 拦截到大模型幻觉！虚构路由 '{target}'，已强制降级。")
            return fallback_agent, f"大模型产生了不存在的路由: {target}"
            
        return target, reason