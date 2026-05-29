# src/stream_agent/agents/chat/agent.py
import logging
from typing import Dict, Any

from stream_agent.worker.base import WorkerBase
from stream_agent.utils.llm_engine import AsyncLLMEngine
from stream_agent.agents.chat.prompts import CHAT_SYSTEM_PROMPT
from stream_agent.core.context import SessionContext
from stream_agent.memory.summarized_memory import SummarizedMemoryManager

logger = logging.getLogger(__name__)

class ChatAgent(WorkerBase):
    def __init__(self, agent_name: str = "chat_agent"):
        super().__init__(agent_name=agent_name)
        self.llm_engine = AsyncLLMEngine()
        # 挂载带有旁路摘要能力的记忆管理器
        self.memory_manager = SummarizedMemoryManager(
            agent_name=self.agent_name, 
            redis_url=self.redis_url
        )

    async def handle_event(self, payload: dict) -> dict:
        session_id = SessionContext.get_session_id()
        memory_summary = await self.memory_manager.query_memory_summary(session_id)
        
        # 2. 打印到控制台/日志
        logger.info(f"========== 🧠 WriterAgent 记忆摘要 (Session: {session_id}) ==========")
        logger.info(memory_summary)
        logger.info("=====================================================================")
        trace_id = SessionContext.get_trace_id()
        
        # Planner 分发任务时，内容通常在 instruction 字段；如果是直连，可能在 query 字段
        user_input = payload.get("instruction") or payload.get("query", "")
        
        if not user_input:
            logger.warning(f"[Trace: {trace_id}] ChatAgent 收到空消息")
            return {"status": "error", "message": "输入内容为空"}

        logger.info(f"[Trace: {trace_id}] ChatAgent 开始处理兜底任务: {user_input[:20]}...")

        try:
            # 1. 立即将用户的输入保存进记忆库（如果达到阈值，会自动在后台触发摘要任务）
            await self.memory_manager.save_message(session_id, "user", user_input)

            # 2. 调取包含系统摘要和滑动窗口内近期对话的历史记录
            history_messages = await self.memory_manager.get_history(session_id)

            # 3. 组装发给大模型的 payload
            messages = [
                {"role": "system", "content": CHAT_SYSTEM_PROMPT}
            ]
            # 追加历史记录（history_messages 中已经包含了刚刚 save_message 进去的那条 user_input）
            messages.extend(history_messages)
            
            # 4. 调用大模型引擎生成回复，适当调高 temperature 让聊天显得更自然
            response_text = await self.llm_engine.generate_text(
                messages=messages, 
                temperature=0.6 
            )

            # 5. 将 Agent 的回复也存入记忆库
            await self.memory_manager.save_message(session_id, "assistant", response_text)

            logger.info(f"[Trace: {trace_id}] ChatAgent 响应生成完毕。")
            
            # 返回结果交由底层的 WorkerBase 封包并通过 Redis Stream 推送回前端
            return {
                "status": "success",
                "result": response_text
            }
            
        except Exception as e:
            logger.error(f"[Trace: {trace_id}] ChatAgent 处理异常: {str(e)}", exc_info=True)
            return {"status": "error", "message": f"通用回答生成失败: {str(e)}"}