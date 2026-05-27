import asyncio
import uvicorn
import logging
from fastapi import Header, HTTPException
from pydantic import BaseModel
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
from stream_agent.gateway.server import GatewayServer
from stream_agent.orchestrator.supervisor import Supervisor
from stream_agent.worker.base import WorkerBase
from stream_agent.core.context import SessionContext

# ==========================================
# 1. Define the underlying worker (the executor who is specifically responsible for the task)
# ==========================================
class GreetingAgent(WorkerBase):
    def __init__(self):
        super().__init__(agent_name="greeting_agent")

    async def handle_event(self, payload: dict) -> dict:
        # 演示：优雅地获取被绝对隔离的鉴权与上下文信息！
        current_user = SessionContext.get_session_id()
        trace_id = SessionContext.get_trace_id()
        
        user_query = payload.get("query", "")
        
        # 模拟大模型思考或工具执行耗时
        await asyncio.sleep(1) 
        
        reply = f"Hello, {current_user}! I am GreetingAgent. I have received your message: '{user_query}'. Task completed successfully! (Trace: {trace_id})"
        
        return {"summary": reply, "status": "success"}

# ==========================================
# 2. Define the console supervisor (triage and triangle routing)
# ==========================================
class DemoSupervisor(Supervisor):
    def __init__(self):
        super().__init__(agent_name="supervisor")
        # Registered routing capability
        self.register_agent("greeting_agent", "When the user is greeting, invoke this agent")

    async def determine_target(self, payload: dict) -> str:
        query = payload.get("query", "")
        if "hello" in query or "hi" in query:
            return "greeting_agent" 
        return self.agent_name 

    async def handle_event(self, payload: dict) -> dict:
        # If the route is missed, the bottom reply given by the supervisor himself
        target = await self.determine_target(payload)
        if target == self.agent_name:
            return {"summary": "I am the triage desk. Your request is too complex, and I cannot find a suitable Agent to handle it."}
        
        # 否则，调用父类的默认逻辑，执行三角路由转发！
        return await super().handle_event(payload)

# ==========================================
# 3. Instantiate gateway and API routing
# ==========================================
gateway = GatewayServer(title="StreamAgent Hello World")
app = gateway.app

class ChatRequest(BaseModel):
    query: str

@app.post("/v1/chat")
async def chat_endpoint(
    request: ChatRequest,
    session_id: str = Header(..., description="User unique session ID"),
    authorization: str = Header(None, description="Optional authentication token")
):
    """The frontend only needs to call this synchronous interface, and the rest is handed over to the underlying asynchronous bus!"""
    try:
        result = await gateway.dispatch_and_wait(
            target_agent="supervisor",
            payload={"query": request.query},
            session_id=session_id,
            auth_token=authorization,
            timeout=5.0
        )
        return {"code": 200, "data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ==========================================
# 4. Magic launcher (concurrent with the three armies)
# ==========================================
async def main():
    print("Pulling up the StreamAgent cluster")
    
    supervisor = DemoSupervisor()
    
    # Online running production environment agent (version 1.0)
    greeter_prod = GreetingAgent() 
    greeter_prod.version = "v1.0"
    
    # Your newly written shadow environment agent ready for testing (2.0 beta version)
    greeter_shadow = GreetingAgent()
    greeter_shadow.version = "v2.0_beta" 

    config = uvicorn.Config(app, host="0.0.0.0", port=8000, log_level="warning")
    server = uvicorn.Server(config)

    await asyncio.gather(
        server.serve(),
        supervisor.start(is_shadow=False),       
        greeter_prod.start(is_shadow=False),    
        greeter_shadow.start(is_shadow=True)  
    )

if __name__ == "__main__":
    asyncio.run(main())