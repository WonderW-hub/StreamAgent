# example.py
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
from stream_agent.agents.planner import PlannerAgent
from stream_agent.agents.writer import WriterAgent
from stream_agent.agents.coder import CoderAgent
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
            trace_id=trace_id,      
            redis_client=self.redis,
        )
        
        # 5. Asynchronous double write and drop disk (silent execution in the background)
        await self.memory.save_message(session_id, "user", query)
        await self.memory.save_message(session_id, "assistant", reply)
        
        return {"summary": "Streaming output is complete", "agent": self.agent_name}

# ==========================================
# Task Dispatcher Agent
# ==========================================
class TaskDispatcherAgent(Supervisor):
    def __init__(self):
        super().__init__(agent_name="dispatcher")
        self.register_agent("researcher", "Call when you need to consult materials, search the knowledge base, and explain concepts")
        self.register_agent("coder", "Call when you need to write code, execute scripts, and fix bugs")
        self.register_agent("writer", "Call when you need to write articles, come up with headlines, and polish text")

        self.register_agent(
            "planner_agent", 
            "[Advanced scheduling] When the user's needs are a complex goal that contains multiple steps, and multiple agents are required to collaborate to complete it (for example: first obtain the content of the file, and then write an article based on the content; or check the information first and then write the code), the task must be forwarded to the agent for pipeline planning.。"
        )

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
        WriterAgent().start(),
        PlannerAgent().start(),
    )

if __name__ == "__main__":
    asyncio.run(main())