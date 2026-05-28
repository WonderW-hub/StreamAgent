# src/stream_agent/worker/base.py
"""Developer inheritance core class WorkerBase (with automatic ACK)"""
import asyncio
import json
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
    Stream-based intelligent agent framework core execution base class.
    Encapsulates complex Redis Stream consumer group management, idempotency interception, context injection, and automatic XACK logic.
    Business developers only need to inherit this class and implement the `handle_event` method.
    """

    def __init__(self, agent_name: str, version: str = "v1.0", redis_url: str = "redis://localhost:6379/0"):
        self.agent_name = agent_name
        self.version = version
        self.redis_url = redis_url
        
        # Define the current Agent's listening bus name
        self.stream_name = f"bus:events:{self.agent_name}"

        self.redis: Optional[redis.Redis] = None
        self.is_shadow_mode = False
        self.group_name = f"group_{self.agent_name}"
        self.consumer_name = f"worker_{self.version}"

    async def _heartbeat_loop(self):
        """
        [Background guardian coroutine] Maintain the survival state of the agent in the system
        """
        heartbeat_key = f"heartbeat:{self.agent_name}"
        try:
            while True:
                # 1. Add yourself to the system's "active roster”
                await self.redis.sadd("system:active_agents", self.agent_name)
                
                # 2. Set a 15-second expiration time (TTL) for your own heartbeat button
                # If the process crashes, this key will automatically disappear after 15 seconds
                await self.redis.set(heartbeat_key, "alive", ex=15)
                
                # 3. Rest for 5 seconds before the next heartbeat
                await asyncio.sleep(5)
                
        except asyncio.CancelledError:
            # Triggered when the coroutine is actively cancelled (graceful shutdown)
            logger.info(f"[{self.agent_name}] 心跳守护协程已停止。")
        except Exception as e:
            logger.error(f"[{self.agent_name}] 心跳服务异常: {e}")

    async def setup(self):
        """Initialize Redis connection and create consumer group"""
        self.redis = redis.from_url(self.redis_url, decode_responses=True)

        # Core: If in shadow mode, must use a separate consumer group, never compete with the production group
        if self.is_shadow_mode:
            self.group_name = f"group_{self.agent_name}_shadow_{self.version}"

        try:
            # mkstream=True ensures that even if the stream hasn't been created yet, this won't crash
            await self.redis.xgroup_create(self.stream_name, self.group_name, id="0", mkstream=True)
            logger.info(f"[{self.agent_name}] Consumer group {self.group_name} is ready.")
        except ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise e

    async def start(self, is_shadow: bool = False):
        self.is_shadow_mode = is_shadow
        await self.setup()
        
        mode_str = "🟢 Shadow Evaluation Mode" if is_shadow else "🚀 Production Processing Mode"
        logger.info(f"[{self.agent_name}] {self.version} started ({mode_str}) | Listening: {self.stream_name}")
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop()) #heartbeat

        try:
            while True:
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
            logger.info(f"[{self.agent_name}] Received a shutdown signal and is exiting safely...")
        finally:
            if self.redis:
                await self.redis.aclose()

    async def stop(self):
        """
        [Elegant shutdown] Actively log out to prevent becoming a ghost node
        """
        logger.info(f"🛑 ready to stop Agent: {self.agent_name}...")
        
        # 1. stop heartbeat
        if hasattr(self, '_heartbeat_task') and self._heartbeat_task:
            self._heartbeat_task.cancel()
            
        heartbeat_key = f"heartbeat:{self.agent_name}"
        try:
            # 2. Actively delete yourself from the active roster
            await self.redis.srem("system:active_agents", self.agent_name)
            # 3. Delete the exclusive heartbeat button immediately
            await self.redis.delete(heartbeat_key)
            logger.info(f"[{self.agent_name}] 已成功从注册中心注销 👋")
        except Exception as e:
            logger.error(f"[{self.agent_name}] 注销时发生异常: {e}")

    async def _advance_pipeline(self, pipeline_key: str, session_id: str, trace_id: str, step_result: str, auth_token: str):
        """
        【全局通用】推进任务状态机，自动将当前结果作为上下文传给下一个 Agent
        """
        pipeline_data = await self.redis.hgetall(pipeline_key)
        if not pipeline_data:
            return
            
        current_step = int(pipeline_data.get("current_step", 1))
        total_steps = int(pipeline_data.get("total_steps", 1))
        tasks = json.loads(pipeline_data.get("tasks", "[]"))
        
        if current_step < total_steps:
            next_step = current_step + 1
            next_task = next((t for t in tasks if t["step_id"] == next_step), None)
            
            if not next_task:
                logger.error(f"[Trace: {trace_id}] can not find {next_step} mision data。")
                return

            await self.redis.hset(pipeline_key, "current_step", next_step)
            
            envelope_data = {
                "trace_id": trace_id,
                "session_id": session_id,
                "auth_token": auth_token,
                "source": self.agent_name,
                "target": next_task["agent_type"],
                "payload": json.dumps({
                    "instruction": next_task["instruction"],
                    "previous_context": step_result, 
                    "step_id": next_task["step_id"],
                    "pipeline_id": pipeline_key,
                    "auth_token": auth_token 
                })
            }
            
            stream_name = f"bus:events:{next_task['agent_type']}"
            await self.redis.xadd(stream_name, envelope_data)
            logger.info(f"[Trace: {trace_id}] auto active next (Step {next_step}) -> {stream_name}")
        else:
            await self.redis.hset(pipeline_key, "status", "SUCCESS")
            logger.info(f"[Trace: {trace_id}] 🎉 Pipeline excuted completed")      
            reply_envelope = EventEnvelope(
                trace_id=trace_id,
                session_id=session_id,
                source=self.agent_name, 
                target="gateway",  
                payload={
                    "status": "success", 
                    "final_article": step_result 
                },
                is_shadow=False
            )
            await self.redis.xadd("bus:events:gateway", reply_envelope.to_redis_dict())

    async def _process_raw_message(self, message_id: str, msg_data: Dict[str, str]):
        """Core processing pipeline: unpacking -> authentication -> execution -> reply -> pipeline advance -> ACK"""
        try:
            # 1. Protocol deserialization
            envelope = EventEnvelope.from_redis_dict(msg_data)
            
            # If the current Worker is started with a shadow flag, force override the Envelope's flag
            if self.is_shadow_mode:
                envelope.is_shadow = True

            # 2. (Idempotency Barrier)
            idemp_key = f"idemp:{envelope.trace_id}:{self.agent_name}:{self.version}"
            is_first_time = await self.redis.set(idemp_key, "PROCESSING", nx=True, ex=3600)
            
            if not is_first_time:
                logger.warning(f"[{self.agent_name}] Triggered idempotency barrier! Discarding duplicate instruction Trace: {envelope.trace_id}")
                await self.redis.xack(self.stream_name, self.group_name, message_id)
                return

            # 3. Mount security sandboxing and dependency injection
            with SessionContext.scope(envelope):
                logger.info(f"[{self.agent_name}] start processing: {envelope.trace_id}")
                
                # 4. The logic of returning control to the business developer to rewrite
                result_payload = await self.handle_event(envelope.payload)
                
                # ================= 新增：自动推进状态机逻辑 =================
                if result_payload:
                    pipeline_id = envelope.payload.get("pipeline_id")
                    if pipeline_id and result_payload.get("status") != "error":
                        step_result = result_payload.get("result", str(result_payload))
                        auth_token = envelope.payload.get("auth_token", "") 
                        
                        await self._advance_pipeline(
                            pipeline_key=pipeline_id,
                            session_id=envelope.session_id,
                            trace_id=envelope.trace_id,
                            step_result=step_result,
                            auth_token=auth_token
                        )
                    else:
                        await self._route_reply(envelope, result_payload)
                # ==========================================================

                # 5. Assemble results and route them back
                if result_payload:
                    await self._route_reply(envelope, result_payload)

            # 6. Successfully executed, submit consumption confirmation (ACK)
            await self.redis.xack(self.stream_name, self.group_name, message_id)

            await self.redis.set(idemp_key, "COMPLETED", ex=3600)

        except Exception as e:
            logger.error(f"[{self.agent_name}] ❌ handle_event failed: {e}", exc_info=True)


    async def _route_reply(self, original_envelope: EventEnvelope, result_payload: Dict[str, Any]):

        reply_envelope = EventEnvelope(
            trace_id=original_envelope.trace_id,
            session_id=original_envelope.session_id,
            source=self.agent_name,
            target=original_envelope.source,
            payload=result_payload,
            is_shadow=original_envelope.is_shadow
        )

        target_stream = "bus:events:shadow_eval" if original_envelope.is_shadow else f"bus:events:{original_envelope.source}"
        

        await self.redis.xadd(
            target_stream, 
            reply_envelope.to_redis_dict(), 
            maxlen=10000, 
            approximate=True
        )
        logger.info(f"[{self.agent_name}] 📤 result routed to: {target_stream}")

    @abstractmethod
    async def handle_event(self, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        A method for business developers to rewrite.
        -Extract task parameters from the workload.
        - Pass SessionContext at any time.get_session_id() gets the context of security isolation.
        -The returned Dict will be automatically packaged and passed back to the upstream.
        """
        pass