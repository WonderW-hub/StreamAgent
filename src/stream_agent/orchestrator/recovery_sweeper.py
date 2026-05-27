import asyncio
import logging
import json
from typing import List, Tuple, Optional
import redis.asyncio as redis
from redis.exceptions import ResponseError

from stream_agent.core.envelope import EventEnvelope

# 独立日志配置
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("StreamAgent.Sweeper")

class RecoverySweeper:
    """
    分布式系统清道夫与死信队列(DLQ)管理器。
    定期巡检注册的总线，打捞超时未 ACK 的僵尸消息，执行重试重投递或降级至死信队列。
    """
    def __init__(
        self, 
        redis_url: str = "redis://localhost:6379/0",
        idle_time_ms: int = 60000, # 消息闲置超过 60 秒视为超时宕机
        max_retries: int = 3,      # 最大重试次数，超过则打入 DLQ
        sweep_interval: int = 10   # 巡检间隔时间（秒）
    ):
        self.redis_url = redis_url
        self.idle_time_ms = idle_time_ms
        self.max_retries = max_retries
        self.sweep_interval = sweep_interval
        
        self.redis: Optional[redis.Redis] = None
        self.sweeper_name = "sweeper_admin"
        self.dlq_stream = "bus:events:dlq"
        
        # 需要监控的 (Stream名称, Group名称) 列表
        self._monitored_targets: List[Tuple[str, str]] = []

    def register_target(self, stream_name: str, group_name: str):
        """注册需要监控的消费者组"""
        self._monitored_targets.append((stream_name, group_name))
        logger.info(f"[Sweeper] 已注册监控目标: Stream={stream_name}, Group={group_name}")

    async def setup(self):
        self.redis = redis.from_url(self.redis_url, decode_responses=True)
        # 确保 DLQ 队列存在，建一个伪 Group 占位
        try:
            await self.redis.xgroup_create(self.dlq_stream, "group_dlq_admin", id="0", mkstream=True)
        except ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise e

    async def start(self):
        """启动清道夫后台守护进程"""
        await self.setup()
        logger.info("🧹 恢复与死信调度引擎 (Recovery Sweeper) 启动完毕，开始静默巡航...")
        
        try:
            while True:
                await self._sweep()
                await asyncio.sleep(self.sweep_interval)
        except asyncio.CancelledError:
            logger.info("🧹 Sweeper 收到关闭信号，安全退出。")
        finally:
            if self.redis:
                await self.redis.aclose()

    async def _sweep(self):
        """执行一次全量巡检"""
        for stream, group in self._monitored_targets:
            try:
                # 使用 XAUTOCLAIM 夺取闲置时间超过 idle_time_ms 的 Pending 消息
                # 返回值结构: [next_start_id, [(msg_id, msg_data), ...]]
                claim_result = await self.redis.xautoclaim(
                    name=stream,
                    groupname=group,
                    consumername=self.sweeper_name,
                    min_idle_time=self.idle_time_ms,
                    start_id="0",
                    count=100
                )
                
                # redis-py 在不同版本的解包可能略有差异，通常 claim_result[1] 是消息列表
                messages = claim_result[1] if len(claim_result) > 1 else []
                
                if messages:
                    logger.warning(f"[Sweeper] 在 {stream} 中打捞到 {len(messages)} 条僵尸消息！准备急救...")
                    for msg_id, msg_data in messages:
                        await self._process_zombie_message(stream, group, msg_id, msg_data)

            except ResponseError as e:
                if "NOGROUP" in str(e):
                    # 队列可能还没人发消息建组，跳过
                    continue
                logger.error(f"[Sweeper] 巡检 {stream} 发生异常: {e}")
            except Exception as e:
                logger.error(f"[Sweeper] 巡检 {stream} 发生未知异常: {e}")

    async def _process_zombie_message(self, origin_stream: str, origin_group: str, msg_id: str, msg_data: dict):
        """处理被夺权的单条僵尸消息"""
        try:
            # 1. 拆开信封 (保留绝对的鉴权与上下文隔离)
            envelope = EventEnvelope.from_redis_dict(msg_data)
            
            # 2. 读取并增加重试计数器
            current_retries = envelope.metadata.get("retry_count", 0)
            
            if current_retries >= self.max_retries:
                # 💥 宣判死亡：转移到 DLQ
                logger.error(f"[DLQ] 💀 消息重试超限 ({current_retries}/{self.max_retries})！打入死信队列。Trace: {envelope.trace_id}")
                envelope.metadata["fatal_error"] = "Max retries exceeded"
                
                await self.redis.xadd(self.dlq_stream, envelope.to_redis_dict(), maxlen=50000, approximate=True)
            else:
                # 🚑 抢救重试：修改 metadata 后，重新扔回原来的队列尾部
                envelope.metadata["retry_count"] = current_retries + 1
                logger.warning(f"[Recovery] 🔄 正在重投递消息 (第 {current_retries + 1} 次重试)。Trace: {envelope.trace_id} -> {origin_stream}")
                
                await self.redis.xadd(origin_stream, envelope.to_redis_dict(), maxlen=10000, approximate=True)
                
            # 3. 擦除痕迹：无论重试还是死亡，这条老的 Pending 消息都要被 ACK 掉，否则它永远卡在 PEL 里
            await self.redis.xack(origin_stream, origin_group, msg_id)

        except Exception as e:
            logger.error(f"[Sweeper] 处理僵尸消息包极其失败 (包已损坏): {e}")
            # 对于完全无法解析的“毒药数据包”，直接 XACK 丢弃，防止永远死循环卡死 Sweeper
            await self.redis.xack(origin_stream, origin_group, msg_id)