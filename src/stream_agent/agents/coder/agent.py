# \src\stream_agent\agents\coder\agent.py
import json
import logging
import uuid
from typing import Dict, Any, List

from stream_agent.memory.base import ZeroHistoryPlugin
from stream_agent.worker.base import WorkerBase
from stream_agent.utils.llm_engine import AsyncLLMEngine 
from stream_agent.core.context import SessionContext 
from stream_agent.agents.coder.prompts import CODER_SYSTEM_PROMPT

# Import OpenSandbox SDKs
from opensandbox import (
    SandboxPoolAsync, 
    PoolCreationSpec, 
    AcquirePolicy, 
    InMemoryAsyncPoolStateStore,
    Sandbox,
)
from stream_agent.worker.sandbox import CodeSandbox
from opensandbox.config import ConnectionConfig
from opensandbox.models import WriteEntry
from code_interpreter import CodeInterpreter

class CoderAgent(WorkerBase):
    def __init__(self):
        super().__init__(agent_name="coder")  
        self.memory = ZeroHistoryPlugin()
        self.sandbox = CodeSandbox(pool_size=3) 
        self.llm = AsyncLLMEngine()

    async def setup(self):
        await super().setup()
        await self.sandbox.start()

    async def stop(self):
        await self.sandbox.stop()
        await super().stop()

    async def handle_event(self, payload: dict) -> dict:
        session_id = SessionContext.get_session_id()
        trace_id = SessionContext.get_trace_id()
        instruction = payload.get("instruction") or payload.get("query", "")
        previous_context = payload.get("previous_context", "")
        files_to_mount = payload.get("files", {})

        is_final_step = True
        pipeline_id = payload.get("pipeline_id")
        if pipeline_id:
            pipeline_data = await self.redis.hgetall(pipeline_id)
            if pipeline_data:
                current_step = int(pipeline_data.get("current_step", 1))
                total_steps = int(pipeline_data.get("total_steps", 1))
                if current_step < total_steps:
                    is_final_step = False 

        query = instruction
        if previous_context:
            query += f"\n\n【Contextual information from the previous step】：\n{previous_context}"
        history = await self.memory.get_history(session_id)

        system_prompt = CODER_SYSTEM_PROMPT
             
        messages = [{"role": "system", "content": system_prompt}] + history + [{"role": "user", "content": query}]

        logging.info(f"[{self.agent_name}] Code is being conceived and streamed to the front end...")
        
        generated_code = await self.llm.generate_stream_to_pubsub(
            messages=messages,
            trace_id=trace_id,       
            redis_client=self.redis, 
        )
      
        clean_code = self._clean_markdown(generated_code)
        logging.info(f"[{self.agent_name}] Code generation complete,Put into the EVU sandbox for execution...")
        
        # 🟢When the sandbox is called, the Redis client, the current Trace ID, and whether the end point is notified to the sandbox engine
        is_success, execution_result = await self.sandbox.execute(
            code=clean_code, 
            redis_client=self.redis,
            trace_id=trace_id,
            is_final_step=is_final_step,
            files_to_mount=files_to_mount
        )
        
        if is_success:
            reply_summary = f"✅ The code was executed successfully!\n sandbox output:\n{execution_result}"
            logging.info(f"[{self.agent_name}] The execution was successful!output: {execution_result}")
        else:
            reply_summary = f"❌ The code execution failed!\nError tracking:\n{execution_result}"
            logging.error(f"[{self.agent_name}] Execution error: {execution_result}")

        await self.memory.save_message(session_id, "user", query)
        await self.memory.save_message(session_id, "assistant", f"```python\n{clean_code}\n```\n{reply_summary}")
        
        return {
            "summary": reply_summary, 
            "agent": self.agent_name,
            "code": clean_code,
            "result": execution_result
        }

    def _clean_markdown(self, text: str) -> str:
        lines = text.split("\n")
        code_lines = []
        in_code_block = False
        
        if not any(line.strip().startswith("```") for line in lines):
            return text.strip()

        for line in lines:
            if line.strip().startswith("```"):
                in_code_block = not in_code_block
                continue
            if in_code_block:
                code_lines.append(line)
        return "\n".join(code_lines).strip()