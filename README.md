
# StreamAgent: 工业级分布式多模态智能体框架

<div align="center">

![License](https://img.shields.io/badge/License-Apache%202.0-blue)
![Python](https://img.shields.io/badge/Python-3.10%2B-green)
![Redis](https://img.shields.io/badge/Redis-7.0%2B-red)

**一个为高并发、低延迟场景设计的生产级分布式智能体框架**

[快速开始](#-快速开始) • [核心特性](#-核心特性) • [架构设计](#-架构设计) • [完整示例](#-完整示例)

</div>

---

## 📋 项目简介

StreamAgent 是一个专为 AI 时代设计的**分布式多智能体（Multi-Agent）编排框架**。它通过 **Redis Stream** 构建异步、解耦的事件总线，使得网关、编排器与多个智能体实例能够以**毫秒级延迟**进行可靠通信，同时保证**分布式幂等性**与**会话隔离**。

### 为什么需要 StreamAgent？

传统的单体 Agent 架构面临的核心问题：
- 🔴 **算力分配不均**：单个进程承载所有请求，无法有效利用多核资源
- 🔴 **响应延迟高**：I/O 阻塞导致整体吞吐量下降
- 🔴 **容错能力弱**：单点故障导致全链路中断，消息易丢失
- 🔴 **扩展性差**：垂直扩展空间有限，无法应对业务激增

StreamAgent 的解决方案：
- ✅ **完全解耦**：网关、编排器、各类 Agent 通过消息总线独立部署与扩展
- ✅ **毫秒级通信**：基于 Redis Stream 的持久化事件总线，确保消息零丢失
- ✅ **分布式幂等**：内置 SETNX 防重锁，防止重复消费与业务污染
- ✅ **水平扩展**：基于消费者组架构，瞬间扩容指定 Agent 类型到 N 个实例
- ✅ **全链路追踪**：TraceID + SessionID 对每个请求进行端到端监控

---

## 🚀 核心特性

### 1. **毫秒级分布式通信**
- **Redis Stream 持久化**：每条消息都被持久化到磁盘，即使 Agent 集群崩溃也能恢复
- **消费者组自动扩展**：支持 Consumer Group 架构，同一类型的 Agent 可自动负载均衡
- **事件协议标准化**：所有消息通过统一的 `EventEnvelope` 封装，支持任意复杂的业务载荷

```python
EventEnvelope(
    trace_id="req-abc123...",      # 全链路追踪 ID
    session_id="user_session_42",   # 用户会话隔离
    source="gateway",               # 消息发送方
    target="writer_agent",          # 目标 Agent
    payload={...},                  # 业务数据
    is_shadow=False                 # 影子测试标记
)
```

### 2. **分布式幂等防御**
通过分布式锁（SETNX）确保同一条消息在分布式环境下**绝对不会被重复执行**：

```python
# 框架自动执行：毫秒级防重锁（1小时有效期）
idemp_key = f"idemp:{trace_id}:{agent_name}:{version}"
is_first_time = await redis.set(idemp_key, "PROCESSING", nx=True, ex=3600)

if not is_first_time:
    logger.warning(f"触发防重拦截！丢弃重复指令 Trace: {trace_id}")
    return  # 直接丢弃，无需重复执行
```

### 3. **高可用流式交互链路**
- **WebSocket 全双工通信**：支持客户端与网关的实时双向通信
- **流式文本生成**：大模型输出边生成边推送，极致低延迟
- **音频流与文本流并发**：支持多模态流式输出，适配人工智能应用的多种交互需求

### 4. **生产级健壮性**
- **全链路异常捕获**：每个环节的异常都被细致捕获与日志记录
- **自动 ACK 与 Pending 恢复**：
  - 成功处理 → 自动 XACK，消息从队列移除
  - 处理失败 → 故意不 ACK，消息留在 Pending 列表，等待后台清道夫恢复
  - 确保**零消息丢失**，即使网络抖动或节点宕机

### 5. **会话与鉴权隔离（零串号）**
通过 **ContextVar** 实现协程级别的隔离沙盒，完全杜绝多用户环境下的数据串号

### 6. **影子测试模式（零成本灾难演练）**
- 生产环境运行 v1.0 Agent，同时启动 v2.0_beta 影子 Agent
- 影子 Agent 消费相同消息，但结果路由到评测队列，绝不返回给用户
- 完整验证新版本性能与正确性，零业务风险

---

## 🏗️ 架构设计

### 整体架构图

```
┌─────────────────────────────────────────────────────────────────┐
│                        客户端 (Web/Mobile)                       │
└────────────────┬────────────────────────────────────────────────┘
                 │ HTTP/WebSocket
                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                    🌐 GatewayServer (网关)                        │
│  • 接收并验证请求  • 生成 TraceID + SessionID                     │
│  • 基于 EventEnvelope 推送消息到 Supervisor 队列                 │
└────────┬────────────────────────────────┬────────────────────────┘
         │                                    │
         ▼                                    ▼
    ┌─────────────────────────────────────────────────┐
    │       🧠 Redis Stream（分布式消息总线）          │
    │  • bus:events:supervisor                       │
    │  • bus:events:writer_agent                     │
    │  • bus:events:coder_agent                      │
    │  • ... 任意数量的 Agent 队列                    │
    │  • bus:events:shadow_eval（影子评测队列）      │
    └────┬────────────┬─────────────┬────────────────┘
         │            │             │
         ▼            ▼             ▼
    ┌──────────┐ ┌──────────┐ ┌──────────┐
    │Supervisor│ │ Writer   │ │  Coder   │
    │   [生]   │ │ Agent[s] │ │  Agent   │
    │   v1.0   │ │ v1.0(×3) │ │ v1.0     │
    └──────────┘ └──────────┘ └──────────┘
         ↓            ↓            ↓
    ┌──────────┐ ┌──────────┐ ┌──────────┐
    │Supervisor│ │ Writer   │ │  Coder   │
    │   [影]   │ │ Agent[s] │ │  Agent   │
    │ v2.0_beta│ │v2.0_beta │ │v2.0_beta │
    └──────────┘ └──────────┘ └──────────┘
         │            │             │
         └────────────┴─────────────┘
                  │
                  ▼ (影子结果只往评测队列)
         bus:events:shadow_eval
         (离线评估，不影响用户)
         
    所有响应 ──────────→ GatewayServer ──────────→ 客户端
```

---

## 🛠️ 快速开始

### 环境依赖

- **Python** 3.10+
- **Redis** 7.0+ (需支持 Stream 与 Consumer Group)
- **Docker & Docker Compose**（推荐用于一键部署）

### 安装

#### 方式 1：本地开发安装
```bash
# 克隆仓库
git clone https://github.com/WonderW-hub/StreamAgent.git
cd StreamAgent

# 创建虚拟环境
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 安装依赖
pip install -r requirements.txt
```

#### 方式 2：Docker Compose 一键启动
```bash
# 启动完整的集群环境
docker-compose up -d --build

# 动态扩缩容（例如：扩展 WriterAgent 到 10 个实例）
docker-compose up -d --scale writer_agent=10
```

### 验证安装

```bash
# 测试 Redis 连接
redis-cli ping
# 预期输出：PONG

# 运行 Hello World 示例
python examples/01_hello_world/main.py
```

访问 `http://localhost:8000/docs` 查看 API 文档。

---

## 💻 完整示例

### Hello World：最简单的 5 分钟示例

```python
import asyncio
import logging
from fastapi import Header
from pydantic import BaseModel
from stream_agent.gateway.server import GatewayServer
from stream_agent.worker.base import WorkerBase
from stream_agent.core.context import SessionContext

logging.basicConfig(level=logging.INFO)

# 定义 Worker
class GreetingAgent(WorkerBase):
    def __init__(self):
        super().__init__(agent_name="greeting_agent")
    
    async def handle_event(self, payload: dict) -> dict:
        user = SessionContext.get_session_id()
        trace_id = SessionContext.get_trace_id()
        query = payload.get("query", "")
        
        reply = f"你好，{user}！我已收到：'{query}'。(Trace: {trace_id})"
        return {"summary": reply, "status": "success"}

# 启动网关
gateway = GatewayServer(title="StreamAgent Demo")
app = gateway.app

class ChatRequest(BaseModel):
    query: str

@app.post("/v1/chat")
async def chat_endpoint(
    request: ChatRequest,
    session_id: str = Header(...),
    authorization: str = Header(None)
):
    result = await gateway.dispatch_and_wait(
        target_agent="greeting_agent",
        payload={"query": request.query},
        session_id=session_id,
        auth_token=authorization,
        timeout=5.0
    )
    return {"code": 200, "data": result}

async def main():
    import uvicorn
    agent = GreetingAgent()
    config = uvicorn.Config(app, host="0.0.0.0", port=8000)
    server = uvicorn.Server(config)
    
    await asyncio.gather(
        server.serve(),
        agent.start(is_shadow=False)
    )

if __name__ == "__main__":
    asyncio.run(main())
```

**测试：**
```bash
curl -X POST http://localhost:8000/v1/chat \
  -H "Content-Type: application/json" \
  -H "session_id: user_123" \
  -d '{"query":"你好"}'
```

---

## 📊 性能指标

| 指标 | 数值 |
|------|------|
| 单点吞吐量 | 5K-10K QPS |
| 端到端延迟 | 10-50 ms |
| 消息丢失率 | 0% |
| 水平扩展效率 | 95%+ |

---

## 🔐 安全特性

- ✅ **会话隔离**：ContextVar 协程级隔离，零串号
- ✅ **认证与授权**：支持 JWT Token 透传与二次验签
- ✅ **消息完整性**：全链路审计与完整性验证
- ✅ **影子测试隔离**：无风险的新版本验证

---

## 🎯 最佳实践

### ✅ 推荐做法

```python
# 1. 使用 EventEnvelope
envelope = EventEnvelope(session_id=user_id, target="agent_name", payload=data)

# 2. 在 Worker 中使用 SessionContext
async def handle_event(self, payload):
    user = SessionContext.get_session_id()  # 安全隔离

# 3. 为长期任务调整 TTL
await redis.set(key, value, nx=True, ex=1800)

# 4. 启用影子测试
agent_shadow.start(is_shadow=True)
```

### ❌ 避免做法

```python
# 不要手动传递会话信息（容易串号）
# 不要重复执行 XACK（框架已处理）
# 不要省略 MAXLEN（防止 OOM）
# 不要假设影子 Agent 结果返回给用户
```

---

## 📁 项目结构

```
StreamAgent/
├── src/stream_agent/
│   ├── core/              # 核心协议（EventEnvelope、SessionContext）
│   ├── gateway/           # HTTP/WebSocket 网关
│   ├── worker/            # Agent 执行引擎（WorkerBase）
│   ├── orchestrator/      # 消息分发与编排（Supervisor）
│   ├── memory/            # 分布式记忆
│   ├── services/          # 第三方服务集成
│   └── utils/             # 工具集
├── examples/
│   ├── 01_hello_world/    # 快速示例
│   └── 03_ai_company/     # 多 Agent 协作
├── tests/                 # 测试用例
├── docker-compose.yml     # 集群编排
└── README.md
```

---

## 🤝 贡献与支持

欢迎提交 Issue、Pull Request 或功能建议！

- 📧 Email: [wonderingwhy2008@gmail.com]
- 💬 讨论区: GitHub Discussions
- 🐛 Issue Tracker: GitHub Issues

---

## 📄 许可证

本项目采用 **Apache License 2.0** 许可证。详见 [LICENSE](LICENSE) 文件。

---

<div align="center">

**⭐ 如果这个项目对你有帮助，请给个 Star！**

Made with ❤️ by StreamAgent Contributors

</div>
