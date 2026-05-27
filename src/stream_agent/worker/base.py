# src/stream_agent/worker/base.py
"""Developer inheritance core class WorkerBase (with automatic ACK)"""
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
            logger.info(f"[{self.agent_name}]Received a shutdown signal and is exiting safely...")
        finally:
            if self.redis:
                await self.redis.aclose()

    async def _process_raw_message(self, message_id: str, msg_data: Dict[str, str]):
        """Core processing pipeline: unpacking -> authentication -> execution -> reply -> ACK"""
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