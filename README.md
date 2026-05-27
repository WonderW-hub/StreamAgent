
# StreamAgent: 工业级分布式多模态智能体引擎

StreamAgent 是一个专为高并发、低延迟场景设计的分布式智能体（Agent）框架。它通过 **Redis Stream** 构建了异步、解耦的消息总线，使得不同类型的 Agent（如文案生成、代码执行、语音处理）能够像微服务一样跨物理节点协同工作。

## 💡 架构背景

在复杂的 AI 应用中，单体架构往往面临“算力分配不均”和“处理延迟高”的问题。StreamAgent 的设计初衷是：

* **彻底解耦**：网关只负责接收指令，智能体专注于处理业务，两者通过 Redis 总线互联。
* **多模态友好**：内置全双工 WebSocket 通道，支持音频流与文本流的并发下发。
* **弹性扩展**：基于消费者组（Consumer Group）架构，支持水平扩展 Agent 实例以应对流量洪峰。

## 🚀 核心优势

* **毫秒级分布式通信**：利用 Redis Stream 的持久化特性，确保任务在分布式节点间传递的可靠性。
* **智能幂等防御**：内置分布式锁（SETNX）机制，确保同一任务在分布式环境下不会被重复消费或执行。
* **高可用流式链路**：支持 WebSocket 全双工通信，结合语音合成（TTS）与文字流式生成，提供极致的拟人化交互体验。
* **生产级健壮性**：全链路异常捕获与 ACK 回执确认机制，确保在网络抖动或节点崩溃时，任务零丢失。

## 🛠️ 快速开始

### 环境依赖

* Python 3.12+
* Redis 7.0+ (支持 Stream 和 Consumer Group)

### 一键部署

使用 Docker Compose 即可拉起整套集群：

```bash
# 启动包含 Redis, Gateway, 和多个 Agent 实例的集群
docker-compose up -d --build

# 动态扩展某个类型的 Agent（例如扩容 WriterAgent 到 10 个实例）
docker-compose up -d --scale writer_agent=10

```

## ⚙️ 核心逻辑演示

### 1. 业务逻辑开发

通过继承 `WorkerBase`，开发者只需关注 `handle_event` 业务逻辑：

```python
class WriterAgent(WorkerBase):
    async def handle_event(self, payload: dict) -> dict:
        # 你的大模型逻辑，WorkerBase 已自动处理上下文与幂等拦截
        return {"summary": "生成成功", "result": "..."}

```

### 2. 分布式通讯协议

所有消息通过 `EventEnvelope` 标准化封装：

* **TraceID**：全链路追踪，确保跨 Agent 的请求与响应一一对应。
* **SessionID**：基于上下文的会话隔离，确保多用户环境下数据不串号。

## 📊 分布式架构演进

本架构支持 Redis Cluster 部署，只需在配置中指定集群地址：

```python
# 支持分布式集群模式
self.redis = RedisCluster.from_url(settings.REDIS_CLUSTER_URL)

```

---

## 🤝 贡献与支持

如果你在部署过程中遇到任何问题，欢迎提交 Issue。

* **核心基建**：由 Redis Stream 提供可靠的消息投递。
* **实时交互**：由 WebSocket 与 TTS 引擎保证模态流转速度。

---


