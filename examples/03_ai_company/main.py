import asyncio
import time
import uvicorn
from fastapi import Header, HTTPException
from pydantic import BaseModel
import logging
from stream_agent.gateway.server import GatewayServer
from stream_agent.orchestrator.supervisor import Supervisor
from stream_agent.worker.base import WorkerBase
from stream_agent.core.context import SessionContext
from stream_agent.memory.layered import LayeredMemoryManager
from stream_agent.memory.sqlite_plugin import SQLiteMemoryPlugin
from stream_agent.memory.redis_plugin import RedisMemoryPlugin
from stream_agent.memory.base import ZeroHistoryPlugin
from stream_agent.worker.sandbox import ThreadPoolEVU
from stream_agent.orchestrator.router import LLMIntentRouter
from stream_agent.utils.llm_engine import AsyncLLMEngine
from stream_agent.config.settings import settings
from stream_agent.worker.sandbox import CodeSandbox
import logging

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

noisy_loggers = [
    "websockets",              
    "websockets.server",        
    "websockets.protocol",      
    "websockets.client",        
    "uvicorn",                  
    "uvicorn.error",            
    "uvicorn.access",           
    "httpx",             
    "httpcore"
]

for logger_name in noisy_loggers:
    logging.getLogger(logger_name).setLevel(logging.WARNING)
    logging.getLogger(logger_name).propagate = False

# ==========================================
# Mock RAG retrieval function (simulate time-consuming tool calls)
# ==========================================
def mock_rag_search(query: str) -> str:

    print(f" [Vector library] Retrieving the enterprise knowledge base for'{query}'...")

    time.sleep(1) # Simulation retrieval time-consuming

    return f"[RAG recall data]: Regarding'{query}', the core principle is the calculation of vector dot multiplication and cosine similarity."


# ==========================================
# Agent 1: Researcher (streaming output + RAG + layered memory)
# ==========================================
class ResearcherAgent(WorkerBase):
    def __init__(self):
        super().__init__(agent_name="researcher")
        # Mount L1+L2 layered memory
        self.memory = LayeredMemoryManager(
            agent_name=self.agent_name, l1_max_len=5, l2_plugin=SQLiteMemoryPlugin("research_vault.db")
        )

        self.llm = AsyncLLMEngine()

    async def handle_event(self, payload: dict) -> dict:
        # Keep the session_id isolated and get the trace_id as the key to the streaming channel
        session_id = SessionContext.get_session_id()
        trace_id = SessionContext.get_trace_id()
        query = payload.get("query", "")
        
        # 1. Exclusive tool call: RAG retrieval (time-consuming operation, does not block the bus)
        rag_context = mock_rag_search(query)
        
        # 2. Pull historical memory (L1/L2 automatic scheduling)
        history = await self.memory.get_history(session_id, limit=5)
        
        # 3. Assemble the ultimate Prompt: System persona + RAG plugin + Historical records + Current question
        system_prompt = f"""You are a rigorous enterprise AI researcher.
Please answer the user's question based on the latest information retrieved from the enterprise knowledge base.
【Knowledge Base Retrieval Results】：
{rag_context}
"""
        messages = [{"role": "system", "content": system_prompt}] + history + [{"role": "user", "content": query}]
        
        # 🚀 4. Shocking upgrade: Replaced with streaming large model calls!
        reply = await self.llm.generate_stream_to_pubsub(
            messages=messages,
            trace_id=trace_id,       # 传入钥匙
            redis_client=self.redis, # 传入网线
        )
        
        # 5. Asynchronous double write and drop disk (silent execution in the background)
        await self.memory.save_message(session_id, "user", query)
        await self.memory.save_message(session_id, "assistant", reply)
        
        return {"summary": "Streaming output is complete", "agent": self.agent_name}

# ==========================================
# Agent 2: Coder (upgraded to streaming output + real EVU sandbox isolation)
# ==========================================
class CoderAgent(WorkerBase):
    def __init__(self):
        # Inherit from WorkerBase, automatically listen to bus:events:coder
        super().__init__(agent_name="coder")  
        self.memory = ZeroHistoryPlugin()
        self.sandbox = CodeSandbox(timeout=3.0)
        self.llm = AsyncLLMEngine()

    async def handle_event(self, payload: dict) -> dict:
        session_id = SessionContext.get_session_id()
        trace_id = SessionContext.get_trace_id()
        query = payload.get("query", "")
        
        # 1. 记忆系统强制拦截（此处 history 永远是空列表 []）
        history = await self.memory.get_history(session_id)
        
        # 2. 程序员专属极客 Prompt
        system_prompt = (
            "You are a top-tier Python engineer. Please write pure code based on the user's requirements.\n"
            "【Rules】\n"
            "Only output Python code, and Try not to include explanatory text.\n"
            "Be sure to use print() to print out the result, otherwise the sandbox cannot capture the result。"
        )
        messages = [{"role": "system", "content": system_prompt}] + history + [{"role": "user", "content": query}]

        logging.info(f"[{self.agent_name}] Code is being conceived and streamed to the front end...")
        
        # 🚀 3. Stream generation!The process of writing code for the large model will be pushed to the Gateway in real time
        generated_code = await self.llm.generate_stream_to_pubsub(
            messages=messages,
            trace_id=trace_id,       
            redis_client=self.redis, 
        )
      
        clean_code = self._clean_markdown(generated_code)
        logging.info(f"[{self.agent_name}] Code generation complete,Put into the EVU sandbox for execution...")
        is_success, execution_result = await self.sandbox.execute(clean_code)
        
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
            "code": clean_code
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

# ==========================================
# Agent 3:  Copywriting planning (streaming output)
# ==========================================
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
        query = payload.get("query", "")
        
        logging.info(f"[{self.agent_name}] Received copywriting task，TraceID: {trace_id}")

        try:          
            history = await self.memory.get_history(session_id, limit=10)
            system_prompt = "You are a top new media copywriter.The language style is very inflammatory.Your task is to explode the title and polish the article。"
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
# ==========================================
# Task Dispatcher Agent
# ==========================================
class TaskDispatcherAgent(Supervisor):
    def __init__(self):
        super().__init__(agent_name="dispatcher")
        # 这些注册信息，会被 LLMIntentRouter 自动组装成 Prompt
        self.register_agent("researcher", "Call when you need to consult materials, search the knowledge base, and explain concepts")
        self.register_agent("coder", "Call when you need to write code, execute scripts, and fix bugs")
        self.register_agent("writer", "Call when you need to write articles, come up with headlines, and polish text")

# ==========================================
# Assemble the startup gateway
# ==========================================
gateway = GatewayServer(title="AI Company Matrix")
app = gateway.app

async def main():
    print("🚀 Starting up the AI Company Matrix (Dispatcher + 3 Experts)...")
    
    await SQLiteMemoryPlugin("research_vault.db")._init_db()

    config = uvicorn.Config(
        app, 
        host=settings.GATEWAY_HOST, 
        port=settings.GATEWAY_PORT, 
        log_level="debug" if settings.DEBUG_MODE else "info"
    )
    server = uvicorn.Server(config)
    await asyncio.gather(
        server.serve(),
        TaskDispatcherAgent().start(),
        ResearcherAgent().start(),
        CoderAgent().start(),
        WriterAgent().start()
    )

if __name__ == "__main__":
    asyncio.run(main())