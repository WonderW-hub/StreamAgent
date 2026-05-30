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
        # Mount a memory manager with bypass summary capability
        self.memory_manager = SummarizedMemoryManager(
            agent_name=self.agent_name, 
            redis_url=self.redis_url
        )

    async def handle_event(self, payload: dict) -> dict:
        session_id = SessionContext.get_session_id()
        memory_summary = await self.memory_manager.query_memory_summary(session_id)
        
        # 2. Print to console/log
        logger.info(f"========== 🧠 WriterAgent Memory summary (Session: {session_id}) ==========")
        logger.info(memory_summary)
        logger.info("=====================================================================")
        trace_id = SessionContext.get_trace_id()
        
        # When Planner distributes tasks, the content is usually in the instruction field; if it is a direct connection, it may be in the query field.
        user_input = payload.get("instruction") or payload.get("query", "")
        
        if not user_input:
            logger.warning(f"[Trace: {trace_id}] ChatAgent Received an empty message")
            return {"status": "error", "message": "The input content is empty"}

        logger.info(f"[Trace: {trace_id}] ChatAgent Start processing the bottom task: {user_input[:20]}...")

        try:
            # 1. Immediately save the user's input into the memory bank (if the threshold is reached, the summary task will be automatically triggered in the background)
            await self.memory_manager.save_message(session_id, "user", user_input)

            # 2. Retrieve the history containing the system summary and recent conversations in the sliding window
            history_messages = await self.memory_manager.get_history(session_id)

            # 3. Assemble the payload sent to the large model
            messages = [
                {"role": "system", "content": CHAT_SYSTEM_PROMPT}
            ]
            # Append the history record (history_messages already contains the user_input that save_message just entered)
            messages.extend(history_messages)
            
            # 4. Call the big model engine to generate a reply, and adjust the temperature appropriately to make the chat appear more natural
            response_text = await self.llm_engine.generate_stream_to_pubsub(
                messages=messages,
                trace_id=trace_id,
                redis_client=self.redis,
                temperature=0.6 
            )

            # 5. Also store the agent's reply in the memory bank
            await self.memory_manager.save_message(session_id, "assistant", response_text)

            logger.info(f"[Trace: {trace_id}] ChatAgent The response is generated.")
            
            # The returned result is handed over to the underlying WorkerBase packet and pushed back to the front end through Redis Stream
            return {
                "status": "success",
                "result": response_text
            }
            
        except Exception as e:
            logger.error(f"[Trace: {trace_id}] ChatAgent Handling exceptions: {str(e)}", exc_info=True)
            return {"status": "error", "message": f"General answer generation failed: {str(e)}"}