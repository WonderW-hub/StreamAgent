import asyncio
import uvicorn
import logging
from fastapi import Header, HTTPException
from pydantic import BaseModel
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# 导入我们的核心框架 (假设已在项目根目录下执行过 pip install -e .)
from stream_agent.gateway.server import GatewayServer
from stream_agent.orchestrator.supervisor import Supervisor
from stream_agent.worker.base import WorkerBase
from stream_agent.core.context import SessionContext

# ==========================================
# 1. 定义底层 Worker (专门负责打招呼的执行者)
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
        
        reply = f"你好，{current_user}！我是 GreetingAgent。我已收到你的消息：'{user_query}'。任务圆满完成！(Trace: {trace_id})"
        
        return {"summary": reply, "status": "success"}

# ==========================================
# 2. 定义总控台 Supervisor (分诊与三角路由)
# ==========================================
class DemoSupervisor(Supervisor):
    def __init__(self):
        super().__init__(agent_name="supervisor")
        # 注册路由能力
        self.register_agent("greeting_agent", "当用户在打招呼时，调用此智能体")

    async def determine_target(self, payload: dict) -> str:
        # 【生产环境】在这里调用 LLMIntentRouter 构建 Prompt 并请求大模型
        # 【演示环境】为了让你无需配置 API Key 即可跑通，这里用硬编码规则代替
        query = payload.get("query", "")
        if "你好" in query or "哈喽" in query:
            return "greeting_agent"  # 命中路由！
        return self.agent_name # 未命中，自己处理

    async def handle_event(self, payload: dict) -> dict:
        # 如果未命中路由，由 Supervisor 亲自给出的兜底回复
        target = await self.determine_target(payload)
        if target == self.agent_name:
            return {"summary": "我是总诊台。你的指令太复杂，我找不到合适的 Agent 来处理。"}
        
        # 否则，调用父类的默认逻辑，执行三角路由转发！
        return await super().handle_event(payload)

# ==========================================
# 3. 实例化网关与 API 路由
# ==========================================
gateway = GatewayServer(title="StreamAgent Hello World")
app = gateway.app

class ChatRequest(BaseModel):
    query: str

@app.post("/v1/chat")
async def chat_endpoint(
    request: ChatRequest,
    session_id: str = Header(..., description="用户唯一会话ID"),
    authorization: str = Header(None, description="可选的鉴权Token")
):
    """前端只需要调这个同步接口，剩下的全交给底层异步总线！"""
    try:
        result = await gateway.dispatch_and_wait(
            target_agent="supervisor", # 第一站永远发给分诊台
            payload={"query": request.query},
            session_id=session_id,
            auth_token=authorization,
            timeout=5.0
        )
        return {"code": 200, "data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ==========================================
# 4. 魔法启动器 (三军并发)
# ==========================================
async def main():
    print("🌟 正在拉起 StreamAgent 集群...")
    
    supervisor = DemoSupervisor()
    
    # 线上正在跑的生产环境 Agent (1.0版本)
    greeter_prod = GreetingAgent() 
    greeter_prod.version = "v1.0"
    
    # 你新写的、准备测试的影子环境 Agent (2.0 测试版)
    greeter_shadow = GreetingAgent()
    greeter_shadow.version = "v2.0_beta" 

    config = uvicorn.Config(app, host="0.0.0.0", port=8000, log_level="warning")
    server = uvicorn.Server(config)

    await asyncio.gather(
        server.serve(),
        supervisor.start(is_shadow=False),       # 分诊台正常运行
        greeter_prod.start(is_shadow=False),     # 生产节点正常接客
        greeter_shadow.start(is_shadow=True)     # ✨ 影子节点开启白嫖模式！
    )

if __name__ == "__main__":
    asyncio.run(main())