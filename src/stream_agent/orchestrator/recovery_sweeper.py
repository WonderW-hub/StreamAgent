# \src\stream_agent\orchestrator\recovery_sweeper.py
import asyncio
import logging
import json
from typing import List, Tuple, Optional
import redis.asyncio as redis
from redis.exceptions import ResponseError
from stream_agent.core.envelope import EventEnvelope
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("StreamAgent.Sweeper")

class RecoverySweeper:
    """
    An asynchronous recovery sweeper and dead letter queue (DLQ) manager for distributed systems.
    Regularly inspects registered message streams, salvages timed-out unacknowledged zombie messages, and performs retry, re-delivery, or degradation to the DLQ.
    """
    def __init__(
        self, 
        redis_url: str = "redis://localhost:6379/0",
        idle_time_ms: int = 60000, 
        max_retries: int = 3,      
        sweep_interval: int = 10 
    ):
        self.redis_url = redis_url
        self.idle_time_ms = idle_time_ms
        self.max_retries = max_retries
        self.sweep_interval = sweep_interval
        
        self.redis: Optional[redis.Redis] = None
        self.sweeper_name = "sweeper_admin"
        self.dlq_stream = "bus:events:dlq"
        
        self._monitored_targets: List[Tuple[str, str]] = []

    def register_target(self, stream_name: str, group_name: str):
        """register a consumer group to be monitored"""
        self._monitored_targets.append((stream_name, group_name))
        logger.info(f"[Sweeper] : Stream={stream_name}, Group={group_name}")

    async def setup(self):
        self.redis = redis.from_url(self.redis_url, decode_responses=True)
        try:
            await self.redis.xgroup_create(self.dlq_stream, "group_dlq_admin", id="0", mkstream=True)
        except ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise e

    async def start(self):
        await self.setup()
        logger.info("🧹 The recovery and dead letter queue (DLQ) scheduler (Recovery Sweeper) has started, beginning silent patrol...")
        
        try:
            while True:
                await self._sweep()
                await asyncio.sleep(self.sweep_interval)
        except asyncio.CancelledError:
            logger.info("🧹 Sweeper receives a shutdown signal and exits safely。")
        finally:
            if self.redis:
                await self.redis.aclose()

    async def _sweep(self):

        for stream, group in self._monitored_targets:
            try:
                claim_result = await self.redis.xautoclaim(
                    name=stream,
                    groupname=group,
                    consumername=self.sweeper_name,
                    min_idle_time=self.idle_time_ms,
                    start_id="0",
                    count=100
                )
                
                messages = claim_result[1] if len(claim_result) > 1 else []
                
                if messages:
                    logger.warning(f"[Sweeper] Found {len(messages)} zombie messages in {stream}!Prepare for first aid...")
                    for msg_id, msg_data in messages:
                        await self._process_zombie_message(stream, group, msg_id, msg_data)

            except ResponseError as e:
                if "NOGROUP" in str(e):
                    continue
                logger.error(f"[Sweeper] {stream}: {e}")
            except Exception as e:
                logger.error(f"[Sweeper] {stream}: {e}")

    async def _process_zombie_message(self, origin_stream: str, origin_group: str, msg_id: str, msg_data: dict):
        try:

            envelope = EventEnvelope.from_redis_dict(msg_data)
            

            current_retries = envelope.metadata.get("retry_count", 0)
            
            if current_retries >= self.max_retries:

                logger.error(f"[DLQ] 💀 Message retry overrun ({current_retries}/{self.max_retries})！Enter the dead letter queue。Trace: {envelope.trace_id}")
                envelope.metadata["fatal_error"] = "Max retries exceeded"  
                await self.redis.xadd(self.dlq_stream, envelope.to_redis_dict(), maxlen=50000, approximate=True)
            else:

                envelope.metadata["retry_count"] = current_retries + 1
                logger.warning(f"[Recovery] 🔄 正在重投递消息 (第 {current_retries + 1} 次重试)。Trace: {envelope.trace_id} -> {origin_stream}")           
                await self.redis.xadd(origin_stream, envelope.to_redis_dict(), maxlen=10000, approximate=True)
            await self.redis.xack(origin_stream, origin_group, msg_id)

        except Exception as e:
            logger.error(f"[Sweeper] 处理僵尸消息包极其失败 (包已损坏): {e}")

            await self.redis.xack(origin_stream, origin_group, msg_id)