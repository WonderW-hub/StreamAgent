import asyncio
import time
import uvicorn
from fastapi import Header, HTTPException
from pydantic import BaseModel
import logging

# 导入核心基建
from stream_agent.gateway.server import GatewayServer
from stream_agent.orchestrator.supervisor import Supervisor
from stream_agent.worker.base import WorkerBase
from stream_agent.core.context import SessionContext

# 导入隔离与记忆武器库
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

# 1. 正常配置你自己的全局格式
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# 🌟 2. 物理消音：建立黑名单，把所有爱刷屏的库彻底封杀
noisy_loggers = [
    "websockets",               # websockets 根节点
    "websockets.server",        # WS 服务端
    "websockets.protocol",      # WS 底层协议帧
    "websockets.client",        # WS 客户端
    "uvicorn",                  # Uvicorn 根节点
    "uvicorn.error",            # Uvicorn 错误输出
    "uvicorn.access",           # Uvicorn 访问输出
    "httpx",                    # 大模型 API 请求底层日志
    "httpcore"
]

for logger_name in noisy_loggers:
    # 强制把它们的门槛调高到 WARNING（只有报错才会说话）
    logging.getLogger(logger_name).setLevel(logging.WARNING)
    # 彻底切断它们向外层大喇叭（Root Logger）广播的权限！
    logging.getLogger(logger_name).propagate = False

# ==========================================
# 独立能力模块 (Tools & RAG)
# ==========================================
def mock_rag_search(query: str) -> str:

    """模拟向量数据库检索 (Researcher 专属)"""

    print(f"   🔍 [向量库] 正在为 '{query}' 检索企业知识库...")

    time.sleep(1) # 模拟检索耗时

    return f"[RAG召回数据]: 关于 '{query}'，核心原理是向量点乘与余弦相似度计算。"


# ==========================================
# Agent 1: 研究员 (升级为流式输出 + RAG + 分层记忆)
# ==========================================
class ResearcherAgent(WorkerBase):
    def __init__(self):
        super().__init__(agent_name="researcher")
        # 挂载 L1+L2 分层记忆
        self.memory = LayeredMemoryManager(
            agent_name=self.agent_name, l1_max_len=5, l2_plugin=SQLiteMemoryPlugin("research_vault.db")
        )
        # 接入真实大脑
        self.llm = AsyncLLMEngine()

    async def handle_event(self, payload: dict) -> dict:
        # 🌟 保持 session_id 隔离，并获取 trace_id 作为流式频道的钥匙
        session_id = SessionContext.get_session_id()
        trace_id = SessionContext.get_trace_id()
        query = payload.get("query", "")
        
        # 1. 独占工具调用：RAG 检索 (耗时操作，不阻塞总线)
        rag_context = mock_rag_search(query)
        
        # 2. 拉取历史记忆 (L1/L2 自动调度)
        history = await self.memory.get_history(session_id, limit=5)
        
        # 3. 组装终极 Prompt：System人设 + RAG外挂 + 历史记录 + 当前提问
        system_prompt = f"""你是一名严谨的企业 AI 研究员。
请根据以下从企业知识库中检索到的最新信息来回答用户的问题。
【知识库检索结果】：
{rag_context}
"""
        messages = [{"role": "system", "content": system_prompt}] + history + [{"role": "user", "content": query}]
        
        # 🚀 4. 震撼升级：替换为流式大模型调用！
        reply = await self.llm.generate_stream_to_pubsub(
            messages=messages,
            trace_id=trace_id,       # 传入钥匙
            redis_client=self.redis, # 传入网线
        )
        
        # 5. 异步双写落盘 (后台静默执行)
        await self.memory.save_message(session_id, "user", query)
        await self.memory.save_message(session_id, "assistant", reply)
        
        return {"summary": "流式输出完毕", "agent": self.agent_name}

# ==========================================
# Agent 2: 程序员 (升级为流式输出 + 真实 EVU 沙盒隔离)
# ==========================================
class CoderAgent(WorkerBase):
    def __init__(self):
        # 继承 WorkerBase，自动监听 bus:events:coder
        super().__init__(agent_name="coder")
        # 绝对纯净：不需要记住上一次写的什么，防止变量名幻觉污染
        self.memory = ZeroHistoryPlugin()
        # 挂载真实的隔离沙盒
        self.sandbox = CodeSandbox(timeout=3.0)
        # 接入真实大脑
        self.llm = AsyncLLMEngine()

    async def handle_event(self, payload: dict) -> dict:
        session_id = SessionContext.get_session_id()
        trace_id = SessionContext.get_trace_id()
        query = payload.get("query", "")
        
        # 1. 记忆系统强制拦截（此处 history 永远是空列表 []）
        history = await self.memory.get_history(session_id)
        
        # 2. 程序员专属极客 Prompt
        system_prompt = (
            "你是顶级 Python 工程师。请根据用户需求写出纯粹的代码。\n"
            "【规则】\n"
            "只输出 Python 代码，尽量不要包含解释性文本。\n"
            "务必使用 print() 将结果打印出来，否则沙盒无法捕获结果。"
        )
        messages = [{"role": "system", "content": system_prompt}] + history + [{"role": "user", "content": query}]

        logging.info(f"[{self.agent_name}] 🧠 正在构思代码，并向前端流式推送...")
        
        # 🚀 3. 流式生成！大模型写代码的过程会实时推送到 Gateway
        generated_code = await self.llm.generate_stream_to_pubsub(
            messages=messages,
            trace_id=trace_id,       # 流式频道的钥匙
            redis_client=self.redis, # Redis 网线
        )

        # 4. 容错清理：剥离大模型顽固附带的 markdown 标记
        clean_code = self._clean_markdown(generated_code)

        # 5. 惊险时刻：将洗净的代码投入沙盒执行
        logging.info(f"[{self.agent_name}] 💻 代码生成完毕，投入 EVU 沙盒执行...")
        is_success, execution_result = await self.sandbox.execute(clean_code)
        
        if is_success:
            reply_summary = f"✅ 代码执行成功！\n沙盒输出:\n{execution_result}"
            logging.info(f"[{self.agent_name}] 执行成功！输出: {execution_result}")
        else:
            reply_summary = f"❌ 代码执行失败！\n报错追踪:\n{execution_result}"
            logging.error(f"[{self.agent_name}] 执行报错: {execution_result}")

        # 6. 后台静默落盘（ZeroHistoryPlugin 会自动拦截，但这是一种好习惯，为日后扩展留口子）
        await self.memory.save_message(session_id, "user", query)
        await self.memory.save_message(session_id, "assistant", f"```python\n{clean_code}\n```\n{reply_summary}")
        
        # 将执行结果打包返回给调度中心 (Dispatcher)
        return {
            "summary": reply_summary, 
            "agent": self.agent_name,
            "code": clean_code
        }

    def _clean_markdown(self, text: str) -> str:
        """剥离 Markdown 代码块标记"""
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
# Agent 3: 文案策划 (流式输出)
# ==========================================
class WriterAgent(WorkerBase):
    def __init__(self):
        super().__init__(agent_name="writer")
        # 🌟 修复 1：使用标准的 Manager 包装，确保 get_history 不会报错
        self.memory = LayeredMemoryManager(
            agent_name=self.agent_name, 
            l1_max_len=10
            # 如果你有特定的 redis 插件实例，可以传给 l2_plugin
        ) 
        self.llm = AsyncLLMEngine()

    async def handle_event(self, payload: dict) -> dict:
        session_id = SessionContext.get_session_id()
        # 必须拿到当前任务的 trace_id，它是打通 Pub/Sub 频道的钥匙！
        trace_id = SessionContext.get_trace_id() 
        query = payload.get("query", "")
        
        logging.info(f"[{self.agent_name}] 📥 收到文案任务，TraceID: {trace_id}")

        try:
            # 1. 安全提取记忆
            history = await self.memory.get_history(session_id, limit=10)
            
            # 2. 组装极客 Prompt
            system_prompt = "你是顶尖的新媒体文案策划。语言风格极具煽动性。你的任务是起爆款标题和润色文章。"
            messages = [{"role": "system", "content": system_prompt}] + history + [{"role": "user", "content": query}]
            
            logging.info(f"[{self.agent_name}] 🧠 开始构思爆款文案，推流中...")

            # 3. 流式生成推送到 Redis
            reply = await self.llm.generate_stream_to_pubsub(
                messages=messages,
                trace_id=trace_id,       # 传入钥匙
                redis_client=self.redis, # 传入网线
            )
            
            # 4. 后台静默落盘
            await self.memory.save_message(session_id, "user", query)
            await self.memory.save_message(session_id, "assistant", reply)
            
            logging.info(f"[{self.agent_name}] ✅ 文案生成与推流圆满结束。")
            return {"summary": "流式输出已完毕", "agent": self.agent_name, "result": reply,}
            
        except Exception as e:
            # 💥 核心防御：捕获所有异常，绝不让网关死锁！
            error_msg = f"发生系统错误: {str(e)}"
            logging.error(f"[{self.agent_name}] ❌ 致命错误: {error_msg}", exc_info=True)
            
            # 强制替大模型发一封“遗书”到前端，并附带 [DONE] 释放前端资源
            if self.redis:
                pubsub_channel = f"channel:stream:{trace_id}"
                await self.redis.publish(pubsub_channel, f"\n\n[Writer 节点崩溃: {error_msg}]")
                await self.redis.publish(pubsub_channel, "[DONE]")
                
            return {"summary": "生成失败", "agent": self.agent_name, "status": "error"}
# ==========================================
# 中央大脑: 任务分发 Agent
# ==========================================
class TaskDispatcherAgent(Supervisor):
    def __init__(self):
        super().__init__(agent_name="dispatcher")
        # 这些注册信息，会被 LLMIntentRouter 自动组装成 Prompt
        self.register_agent("researcher", "需要查阅资料、搜索知识库、解释概念时调用")
        self.register_agent("coder", "需要编写代码、执行脚本、修复 Bug 时调用")
        self.register_agent("writer", "需要写文章、起标题、润色文字时调用")

# ==========================================
# 组装启动网关
# ==========================================
gateway = GatewayServer(title="AI Company Matrix")
app = gateway.app

async def main():
    print("🚀 正在拉起 AI 公司矩阵 (Dispatcher + 3 Experts)...")
    
    # 建立研究员的数据库表
    await SQLiteMemoryPlugin("research_vault.db")._init_db()

    config = uvicorn.Config(
        app, 
        host=settings.GATEWAY_HOST, 
        port=settings.GATEWAY_PORT, 
        log_level="debug" if settings.DEBUG_MODE else "info"
    )
    server = uvicorn.Server(config)

    # 震撼：同时拉起 1个网关 + 1个分发器 + 3个干活的 Agent！
    await asyncio.gather(
        server.serve(),
        TaskDispatcherAgent().start(),
        ResearcherAgent().start(),
        CoderAgent().start(),
        WriterAgent().start()
    )

if __name__ == "__main__":
    asyncio.run(main())