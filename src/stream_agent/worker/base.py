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

logger = logging.getLogger("StreamAgent.Worker")

class WorkerBase(ABC):
    """
    Stream-based intelligent agent framework core execution base class.
    """
    def __init__(self, agent_name: str, version: str = "v1.0", redis_url: str = "redis://localhost:6379/0"):
        self.agent_name = agent_name
        self.version = version
        self.redis_url = redis_url
        self.stream_name = f"bus:events:{self.agent_name}"
        self.redis: Optional[redis.Redis] = None
        self.is_shadow_mode = False
        self.group_name = f"group_{self.agent_name}"
        self.consumer_name = f"worker_{self.version}"

    async def _heartbeat_loop(self):
        heartbeat_key = f"heartbeat:{self.agent_name}"
        try:
            while True:
                await self.redis.sadd("system:active_agents", self.agent_name)
                await self.redis.set(heartbeat_key, "alive", ex=15)
                await asyncio.sleep(5)
        except asyncio.CancelledError:
            logger.info(f"[{self.agent_name}] Heartbeat daemon stopped.")
        except Exception as e:
            logger.error(f"[{self.agent_name}] Heartbeat service exception: {e}")

    async def setup(self):
        self.redis = redis.from_url(self.redis_url, decode_responses=True)
        if self.is_shadow_mode:
            self.group_name = f"group_{self.agent_name}_shadow_{self.version}"
        try:
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
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

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
            logger.info(f"[{self.agent_name}] Received shutdown signal, exiting safely...")
        finally:
            if self.redis:
                await self.redis.aclose()

    async def stop(self):
        logger.info(f"🛑 Ready to stop Agent: {self.agent_name}...")
        if hasattr(self, '_heartbeat_task') and self._heartbeat_task:
            self._heartbeat_task.cancel()
        heartbeat_key = f"heartbeat:{self.agent_name}"
        try:
            await self.redis.srem("system:active_agents", self.agent_name)
            await self.redis.delete(heartbeat_key)
            logger.info(f"[{self.agent_name}] Successfully deregistered from registry 👋")
        except Exception as e:
            logger.error(f"[{self.agent_name}] Exception during deregistration: {e}")
    
    async def _cleanup_trace_resources(self, trace_id: str):
        try:
            sandbox_key = f"trace_sandbox:{trace_id}"
            sandbox_id = await self.redis.get(sandbox_key)
            if sandbox_id:
                from stream_agent.worker.sandbox import CodeSandbox
                asyncio.create_task(CodeSandbox.destroy_sandbox(sandbox_id))
                await self.redis.delete(sandbox_key)
        except Exception as e:
            logger.error(f"clean Trace resource fail [{trace_id}]: {e}")

    async def _advance_pipeline(self, pipeline_key: str, session_id: str, trace_id: str, step_result: str, auth_token: str, original_source: str):
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
                logger.error(f"[Trace: {trace_id}] Cannot find Step {next_step} mission data.")
                return

            await self.redis.hset(pipeline_key, "current_step", next_step)
            
            envelope_data = {
                "trace_id": trace_id,
                "session_id": session_id,
                "auth_token": auth_token,
                "source": original_source,
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
            logger.info(f"[Trace: {trace_id}] Auto activate next (Step {next_step}) -> {stream_name}")
        else:
            await self.redis.hset(pipeline_key, "status", "SUCCESS")
            await self._cleanup_trace_resources(trace_id)
            logger.info(f"[Trace: {trace_id}] 🎉 Pipeline executed completely")      
            
            reply_envelope = EventEnvelope(
                trace_id=trace_id,
                session_id=session_id,
                source=self.agent_name, 
                target=original_source,
                payload={
                    "status": "success", 
                    "final_article": step_result 
                },
                is_shadow=False
            )
            
            safe_source = original_source.replace("bus:events:", "")
            target_stream = f"bus:events:{safe_source}"
            
            await self.redis.xadd(target_stream, reply_envelope.to_redis_dict())
            logger.info(f"[{self.agent_name}] 📤 Pipeline final result routed back to: {target_stream}")

    async def _process_raw_message(self, message_id: str, msg_data: Dict[str, str]):
        envelope = EventEnvelope.from_redis_dict(msg_data)   
        step_id = envelope.payload.get("step_id", "single")
        idemp_key = f"idemp:{envelope.trace_id}:{self.agent_name}:{self.version}:step_{step_id}"
        
        try:
            if self.is_shadow_mode:
                envelope.is_shadow = True

            is_first_time = await self.redis.set(idemp_key, "PROCESSING", nx=True, ex=60)
            
            if not is_first_time:
                logger.warning(f"[{self.agent_name}] Triggered idempotency barrier! Discarding duplicate Trace: {envelope.trace_id} (Step: {step_id})")
                await self.redis.xack(self.stream_name, self.group_name, message_id)
                return

            with SessionContext.scope(envelope):
                logger.info(f"[{self.agent_name}] Start processing Trace: {envelope.trace_id} (Step: {step_id})")
                
                result_payload = await self.handle_event(envelope.payload)
                
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
                            auth_token=auth_token,
                            original_source=envelope.source
                        )
                    else:
                        await self._cleanup_trace_resources(envelope.trace_id)
                        await self._route_reply(envelope, result_payload)

            await self.redis.xack(self.stream_name, self.group_name, message_id)
            await self.redis.set(idemp_key, "COMPLETED", ex=3600)

        except Exception as e:
            logger.error(f"[{self.agent_name}] ❌ handle_event failed on Trace {envelope.trace_id} (Step: {step_id}): {e}", exc_info=True)
            await self._cleanup_trace_resources(envelope.trace_id)
            try:
                await self.redis.delete(idemp_key)
            except Exception as del_err:
                logger.error(f"Failed to release idempotency lock: {del_err}")

    async def _route_reply(self, original_envelope: EventEnvelope, result_payload: Dict[str, Any]):
        reply_envelope = EventEnvelope(
            trace_id=original_envelope.trace_id,
            session_id=original_envelope.session_id,
            source=self.agent_name,
            target=original_envelope.source,
            payload=result_payload,
            is_shadow=original_envelope.is_shadow
        )


        safe_source = original_envelope.source.replace("bus:events:", "")
        target_stream = "bus:events:shadow_eval" if original_envelope.is_shadow else f"bus:events:{safe_source}"
        
        await self.redis.xadd(
            target_stream, 
            reply_envelope.to_redis_dict(), 
            maxlen=10000, 
            approximate=True
        )
        logger.info(f"[{self.agent_name}] 📤 Result routed to: {target_stream}")

    @abstractmethod
    async def handle_event(self, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        pass