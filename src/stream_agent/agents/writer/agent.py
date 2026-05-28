# \src\stream_agent\agents\writer\agent.py
from stream_agent.memory.layered import LayeredMemoryManager
from stream_agent.memory.sqlite_plugin import SQLiteMemoryPlugin
from stream_agent.memory.redis_plugin import RedisMemoryPlugin
from stream_agent.memory.base import ZeroHistoryPlugin
from stream_agent.utils.llm_engine import AsyncLLMEngine 
from stream_agent.worker.base import WorkerBase
from stream_agent.core.context import SessionContext 
from stream_agent.agents.writer.prompts import WRITER_SYSTEM_PROMPT
import logging

class WriterAgent(WorkerBase):
    def __init__(self):
        super().__init__(agent_name="writer")
        self.memory = LayeredMemoryManager(
            agent_name=self.agent_name, 
            l1_max_len=10
        ) 
        self.llm = AsyncLLMEngine()

    async def handle_event(self, payload: dict) -> dict:
        session_id = SessionContext.get_session_id()
        trace_id = SessionContext.get_trace_id() 
        instruction = payload.get("instruction") or payload.get("query", "")
        previous_context = payload.get("previous_context", "")
        
        query = instruction
        if previous_context:
            query += f"\n\n【Please be sure to copywriter based on the content provided below】：\n{previous_context}"
        
        logging.info(f"[{self.agent_name}] Received copywriting task，TraceID: {trace_id}")

        try:          
            history = await self.memory.get_history(session_id, limit=10)
            system_prompt = WRITER_SYSTEM_PROMPT
            messages = [{"role": "system", "content": system_prompt}] + history + [{"role": "user", "content": query}]       
            logging.info(f"[{self.agent_name}] Started to conceive explosive copywriting, and it is in the flow...")
    
            reply = await self.llm.generate_stream_to_pubsub(
                messages=messages,
                trace_id=trace_id,      
                redis_client=self.redis, 
            )
            
            await self.memory.save_message(session_id, "user", query)
            await self.memory.save_message(session_id, "assistant", reply)
            
            logging.info(f"[{self.agent_name}] ✅ Copywriting generation and streaming successfully concluded。")
            return {"summary": "Streaming output has been completed", "agent": self.agent_name, "result": reply,}
            
        except Exception as e:
            error_msg = f"System error occurred: {str(e)}"
            logging.error(f"[{self.agent_name}] Fatal error: {error_msg}", exc_info=True)
            
            if self.redis:
                pubsub_channel = f"channel:stream:{trace_id}"
                await self.redis.publish(pubsub_channel, f"\n\n[Writer node crashed: {error_msg}]")
                await self.redis.publish(pubsub_channel, "[DONE]")
                
            return {"summary": "Generation failed", "agent": self.agent_name, "status": "error"}