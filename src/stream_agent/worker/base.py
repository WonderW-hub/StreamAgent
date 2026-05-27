"""开发者继承的核心类 WorkerBase (自带自动 ACK)"""
import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional

import redis.asyncio as redis
from redis.exceptions import ResponseError

from stream_agent.core.envelope import EventEnvelope
from stream_agent.core.context import SessionContext
from stream_agent.core.exceptions import IdempotencyError

logger = logging.getLogger("StreamAgent.Worker")

class WorkerBase(ABC):
    """
    流式智能体框架核心执行基类。
    封装了复杂的 Redis Stream 消费者组管理、幂等性拦截、上下文注入与自动 XACK 逻辑。
    业务开发者只需继承此类，并实现 `handle_event` 方法。
    """

    def __init__(self, agent_name: str, version: str = "v1.0", redis_url: str = "redis://localhost:6379/0"):
        self.agent_name = agent_name
        self.version = version
        self.redis_url = redis_url
        
        # 定义当前 Agent 监听的总线名称
        self.stream_name = f"bus:events:{self.agent_name}"
        
        # 运行时状态
        self.redis: Optional[redis.Redis] = None
        self.is_shadow_mode = False
        self.group_name = f"group_{self.agent_name}"
        self.consumer_name = f"worker_{self.version}"

    async def setup(self):
        """初始化 Redis 连接并建立消费者组堡垒"""
        self.redis = redis.from_url(self.redis_url, decode_responses=True)
        
        # 核心：如果是影子模式，必须使用独立的消费者组，绝对不能和生产组抢消息
        if self.is_shadow_mode:
            self.group_name = f"group_{self.agent_name}_shadow_{self.version}"

        try:
            # mkstream=True 确保即使总线还没建，这里也不会崩溃
            await self.redis.xgroup_create(self.stream_name, self.group_name, id="0", mkstream=True)
            logger.info(f"[{self.agent_name}] 消费者组 {self.group_name} 就绪。")
        except ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise e

    async def start(self, is_shadow: bool = False):
        """启动 Worker 的死循环监听引擎"""
        self.is_shadow_mode = is_shadow
        await self.setup()
        
        mode_str = "🟢 影子评测模式" if is_shadow else "🚀 生产处理模式"
        logger.info(f"[{self.agent_name}] {self.version} 启动成功 ({mode_str}) | 监听: {self.stream_name}")

        try:
            while True:
                # 阻塞式拉取：最长等待 2 秒，避免死锁
                messages = await self.redis.xreadgroup(
                    groupname=self.group_name,
                    consumername=self.consumer_name,
                    streams={self.stream_name: ">"},
                    count=1,
                    block=2000
                )

                if not messages:
                    continue

                for stream, msg_list in messages:
                    for message_id, msg_data in msg_list:
                        await self._process_raw_message(message_id, msg_data)

        except asyncio.CancelledError:
            logger.info(f"[{self.agent_name}] 收到关闭信号，正在安全退出...")
        finally:
            if self.redis:
                await self.redis.aclose()

    async def _process_raw_message(self, message_id: str, msg_data: Dict[str, str]):
        """核心处理管道：解包 -> 鉴权 -> 执行 -> 回传 -> ACK"""
        try:
            # 1. 协议反序列化
            envelope = EventEnvelope.from_redis_dict(msg_data)
            
            # 如果当前 Worker 启动时挂载了影子标识，强制覆盖 Envelope 的标识
            if self.is_shadow_mode:
                envelope.is_shadow = True

            # 2. 绝对防御：分布式幂等性拦截 (Idempotency Barrier)
            # 使用 SETNX (Set if Not Exists) 实现毫秒级防重锁，锁定 1 小时
            idemp_key = f"idemp:{envelope.trace_id}:{self.agent_name}:{self.version}"
            is_first_time = await self.redis.set(idemp_key, "PROCESSING", nx=True, ex=3600)
            
            if not is_first_time:
                logger.warning(f"[{self.agent_name}] 触发防重拦截！丢弃重复指令 Trace: {envelope.trace_id}")
                await self.redis.xack(self.stream_name, self.group_name, message_id)
                return

            # 3. 挂载安全沙盒与依赖注入
            with SessionContext.scope(envelope):
                logger.info(f"[{self.agent_name}] 📥 开始处理任务: {envelope.trace_id}")
                
                # 4. 将控制权交还给业务开发者重写的逻辑
                result_payload = await self.handle_event(envelope.payload)
                
                # 5. 组装结果并路由打回
                if result_payload:
                    await self._route_reply(envelope, result_payload)

            # 6. 成功执行完毕，提交消费确认 (ACK)
            await self.redis.xack(self.stream_name, self.group_name, message_id)
            # 标记幂等锁为已完成
            await self.redis.set(idemp_key, "COMPLETED", ex=3600)

        except Exception as e:
            logger.error(f"[{self.agent_name}] ❌ 处理失败: {e}", exc_info=True)
            # 注意：此处故意不执行 XACK！
            # 这使得由于网络抖动或大模型 API 超时导致失败的消息，会留在 Pending 列表中，
            # 等待未来重启或被后台清道夫(Sweeper)重新捞起，实现“零消息丢失”。

    async def _route_reply(self, original_envelope: EventEnvelope, result_payload: Dict[str, Any]):
        """结果智能路由：生产环境回传网关，影子环境打入评测总线"""
        reply_envelope = EventEnvelope(
            trace_id=original_envelope.trace_id,
            session_id=original_envelope.session_id,
            source=self.agent_name,
            target=original_envelope.source, # 默认原路返回给发信人(如 gateway)
            payload=result_payload,
            is_shadow=original_envelope.is_shadow
        )

        target_stream = "bus:events:shadow_eval" if original_envelope.is_shadow else f"bus:events:{original_envelope.source}"
        
        # 极其重要：限制队列最大长度 (MAXLEN)，防止 OOM
        await self.redis.xadd(
            target_stream, 
            reply_envelope.to_redis_dict(), 
            maxlen=10000, 
            approximate=True
        )
        logger.info(f"[{self.agent_name}] 📤 结果已路由至: {target_stream}")

    @abstractmethod
    async def handle_event(self, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        供业务开发者重写的方法。
        - 从 payload 提取任务参数。
        - 随时通过 SessionContext.get_session_id() 获取安全隔离的上下文。
        - 返回的 Dict 将被自动包装回传给上游。
        """
        pass