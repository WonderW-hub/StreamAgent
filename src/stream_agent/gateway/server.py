# src/stream_agent/gateway/server.py
"""Asynchronous gateway base class based on FastAPI"""
import json
import uuid
import asyncio
import logging
from fastapi import FastAPI, HTTPException, Request, Header, WebSocket, WebSocketDisconnect, Query
from contextlib import asynccontextmanager
from typing import Optional, Dict, Any
import redis.asyncio as redis
from redis.exceptions import ResponseError

from stream_agent.core.envelope import EventEnvelope
from stream_agent.gateway.future_pool import FuturePool
from stream_agent.services.asr_service import ASRService
from stream_agent.services.tts_service import TTSService
from pydantic import BaseModel
from stream_agent.config.settings import settings
from stream_agent.worker.sandbox import CodeSandbox
from fastapi.responses import StreamingResponse

class ChatRequest(BaseModel):
    query: str

logger = logging.getLogger("StreamAgent.Gateway")

class GatewayServer:
    """
    Asynchronous gateway base class.
    Integrates FastAPI lifecycle management, Redis background listener engine, and standard HTTP to Stream delivery methods.
    Also supports ultra-low latency WebSocket + Redis Pub/Sub streaming push.
    """
    def __init__(self, title: str = settings.PROJECT_NAME, redis_url: str = settings.REDIS_URL):
        self.redis_url = redis_url
        self.redis: Optional[redis.Redis] = None
        
        # 1. Initialize core engines
        self.future_pool = FuturePool()
        
        # [FIX] Generate a globally unique instance ID for horizontal scaling.
        # This solves the "routing loss" issue when multiple gateway instances are deployed.
        self.instance_id = str(uuid.uuid4())
        self.source_name = f"gateway:{self.instance_id}"
        self.return_stream = f"bus:events:{self.source_name}"
        self.gateway_group = "group_gateway"
        
        # 2. Bind FastAPI lifecycle
        self.app = FastAPI(title=title, lifespan=self._lifespan)
        
        # Background listener task handle
        self._listener_task: Optional[asyncio.Task] = None

        # 3. Mount multimodal engines
        self.asr_service = ASRService()
        self.tts_service = TTSService()
        
        # 4. Register all API and WebSocket routes
        self.setup_routes()

    @asynccontextmanager
    async def _lifespan(self, app: FastAPI):
        # --- startup ---
        self.redis = redis.from_url(self.redis_url, decode_responses=True)
        
        # Establish an independent consumer group for the gateway in Redis using the instance-specific return stream
        try:
            await self.redis.xgroup_create(self.return_stream, self.gateway_group, id="0", mkstream=True)
        except ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise e
                
        # Derive the background guardian coroutine, and listen to the callback queue in an endless loop
        self._listener_task = asyncio.create_task(self._listen_for_replies())
        logger.info(f"Gateway started successfully! Background listening on {self.return_stream} (Instance: {self.instance_id})")     
        self.tts_service.initialize()         
        yield 
       
        # --- shutdown ---
        logger.info("Gateway is closing and resources are being cleaned up...")
        if self._listener_task:
            self._listener_task.cancel()
        if self.redis:
            await self.redis.aclose()

    async def _listen_for_replies(self):
        """
        Background guardian coroutine: Keeps a close eye on the instance-specific Redis callback queue.
        """
        try:
            while True:
                messages = await self.redis.xreadgroup(
                    groupname=self.gateway_group,
                    consumername=self.instance_id,
                    streams={self.return_stream: ">"},
                    count=10,
                    block=1000
                )
                
                if not messages:
                    continue
                    
                for stream, msg_list in messages:
                    for msg_id, msg_data in msg_list:
                        try:
                            envelope = EventEnvelope.from_redis_dict(msg_data)
                            
                            # If it is the end receipt of the WebSocket, ACK directly, no need to wake up future.
                            if envelope.trace_id.startswith("req-ws-"):
                                logger.debug(f"[Gateway] Received streaming task {envelope.trace_id}: Underlying end signal recovered.")
                                await self.redis.xack(self.return_stream, self.gateway_group, msg_id)
                                continue
                                
                            # Hit the memory pool to wake up the pending HTTP coroutine
                            self.future_pool.resolve_future(envelope.trace_id, envelope)
                            await self.redis.xack(self.return_stream, self.gateway_group, msg_id)
                            
                        except Exception as e:
                            logger.error(f"Failed to parse returned result: {e}")
                            
        except asyncio.CancelledError:
            logger.info("Listening task was safely cancelled.")

    def setup_routes(self):
        """Dynamically mount the routing system"""

        @self.app.post("/v1/chat")
        async def chat_endpoint(
            request: ChatRequest, 
            session_id: str = Header(..., description="Request header authorization session ID"),
            authorization: str = Header(default=None, description="Bearer Token for auth") 
        ):
            return await self.dispatch_and_wait(
                target_agent="dispatcher", 
                payload={
                    "query": request.query,
                    "auth_token": authorization 
                },
                session_id=session_id,
                timeout=60.0
            )

        @self.app.post("/v1/sse/chat")
        async def sse_chat_endpoint(
            request: ChatRequest,
            session_id: str = Header(..., description="用户唯一会话ID"),
            authorization: Optional[str] = Header(default=None, description="鉴权Token")
        ):
            """
            SSE (Server-Sent Events) one-way text streaming interface
            Suitable for front-end typewriter effect, one-way push of tokens generated by large models through long HTTP connections
            """
            # 1. Generate the unique Trace ID and Redis subscription channel for this request
            trace_id = f"req-sse-{uuid.uuid4().hex[:8]}"
            pubsub_channel = f"channel:stream:{trace_id}"
            
            # 2. Construct the envelope delivered to the backend
            envelope = EventEnvelope(
                trace_id=trace_id, 
                session_id=session_id,
                auth_token=authorization, 
                source=self.source_name, 
                target="dispatcher",
                action="process", 
                payload={"query": request.query}
            )

            # 3. Define SSE asynchronous generator
            async def event_generator():
                pubsub = self.redis.pubsub()
                try:
                    # Subscribe first to prevent the first token from being missed
                    await pubsub.subscribe(pubsub_channel)
                    
                    # After the subscription is successful, the task will be delivered to the backend agent queue
                    await self.redis.xadd("bus:events:dispatcher", envelope.to_redis_dict())
                    logger.info(f"💬 [SSE] [{session_id}] 任务已投递，开始监听频道: {pubsub_channel}")

                    # Loop monitoring Redis Pub/Sub
                    async for message in pubsub.listen():
                        if message["type"] == "message":
                            token = message["data"]
                            
                            if token == "[DONE]":
                                # End sign
                                yield "data: [DONE]\n\n"
                                break
                            
                            # Encapsulate the token into a JSON string to prevent newline characters from breaking SSE's data format specification
                            chunk_data = json.dumps({"content": token}, ensure_ascii=False)
                            yield f"data: {chunk_data}\n\n"
                            
                except asyncio.CancelledError:
                    # Triggered when a client (such as a browser) actively disconnects
                    logger.warning(f"[SSE] [{session_id}] Client actively disconnected the streaming connection.")
                except Exception as e:
                    logger.error(f"🚨 [SSE] An exception occurred: {str(e)}", exc_info=True)
                    error_data = json.dumps({"error": str(e)}, ensure_ascii=False)
                    yield f"data: {error_data}\n\n"
                finally:
                    # Whether it ends normally or disconnects abnormally, be sure to clean up the Redis subscription
                    await pubsub.unsubscribe(pubsub_channel)

            # 4. Returns a streaming response, specifying media_type as text/event-stream
            return StreamingResponse(event_generator(), media_type="text/event-stream")
        @self.app.websocket("/v1/ws/chat")
        async def websocket_chat(
            websocket: WebSocket,
            session_id: str = Query(..., description="User session ID in URL parameter"),
            authorization: Optional[str] = Query(None, description="URL parameter for authorization token")
        ):
            if not session_id:
                await websocket.close(code=1008, reason="Missing session_id")
                return
                
            await websocket.accept()
            trace_id = f"req-ws-{uuid.uuid4().hex[:8]}"
            pubsub_channel = f"channel:stream:{trace_id}"
            
            pubsub = self.redis.pubsub()
            try:
                await pubsub.subscribe(pubsub_channel)
            except Exception as e:
                logger.error(f"🚨 Redis subscription failed, forcefully terminating: {e}")
                await websocket.close(code=1011)
                return
            
            asr_session = None
            tts_queue = asyncio.Queue()
            ws_lock = asyncio.Lock()
            enable_tts = True

            async def receive_loop():
                nonlocal asr_session, enable_tts
                try:
                    while True:
                        message = await websocket.receive()
                        if message["type"] == "websocket.disconnect":
                            await tts_queue.put(None)
                            break
                        
                        if "text" in message:
                            data = json.loads(message["text"])
                            action = data.get("action", "chat")
                            enable_tts = data.get("require_audio", True)
                            
                            if action == "start_audio":
                                logger.info(f"🎤 [{session_id}] Starting microphone, initializing ASR...")
                                asr_session = self.asr_service.create_session()
                            elif action == "stop_audio":
                                if asr_session:
                                    logger.info(f"🛑 [{session_id}] Stopping speech, waiting for ASR final result...")
                                    text_result = await asr_session.finish()
                                    asr_session = None
                                    
                                    if text_result:
                                        logger.info(f"📝 [{session_id}] ASR completed: {text_result}")
                                        envelope = EventEnvelope(
                                            trace_id=trace_id, session_id=session_id,
                                            auth_token=authorization, source=self.source_name, target="dispatcher",
                                            action="process", payload={"query": text_result}
                                        )
                                        await self.redis.xadd("bus:events:dispatcher", envelope.to_redis_dict())
                                        
                            elif action == "chat":
                                query = data.get("query", "")
                                logger.info(f"💬 [{session_id}] Gateway received frontend text: '{query}' (TTS: {enable_tts})")
                                
                                envelope = EventEnvelope(
                                    trace_id=trace_id, session_id=session_id,
                                    auth_token=authorization, source=self.source_name, target="dispatcher",
                                    action="process", payload={"query": query}
                                )
                                try:
                                    await self.redis.xadd("bus:events:dispatcher", envelope.to_redis_dict())
                                except Exception as e:
                                    logger.error(f"🚨 Fatal error in task delivery to Redis: {e}")
                            elif action == "stop_audio":
                                if asr_session:
                                    logger.info(f"🛑 [{session_id}] Stopping speech, waiting for ASR final result...")
                                    text_result = await asr_session.finish()
                                    asr_session = None
                                    
                                    if text_result:
                                        logger.info(f"📝 [{session_id}] ASR completed: {text_result}")
                                        envelope = EventEnvelope(
                                            trace_id=trace_id, session_id=session_id,
                                            auth_token=authorization, source=self.source_name, target="dispatcher",
                                            action="process", payload={"query": text_result}
                                        )
                                        await self.redis.xadd("bus:events:dispatcher", envelope.to_redis_dict())
                                    else:
                                        # Fix: Send fallback text and [DONE] signal if ASR fails
                                        await self.redis.publish(pubsub_channel, "⚠️ No speech detected or ASR API failed.")
                                        await self.redis.publish(pubsub_channel, "[DONE]")

                        elif "bytes" in message:
                            if asr_session:
                                asr_session.push_audio(message["bytes"])
                except WebSocketDisconnect:
                    logger.debug(f"[Gateway] [{session_id}] Client disconnected normally.")
                    await tts_queue.put(None)
                except Exception as e:
                    logger.error(f"🚨 Fatal error in receive channel: {str(e)}", exc_info=True)
                    await tts_queue.put(None)

            async def text_loop():
                sentence_buffer = ""
                punctuation = set("。！？；.!?;")
                try:
                    async for message in pubsub.listen():
                        if message["type"] == "message":
                            token = message["data"]
                            try:
                                if token == "[DONE]":
                                    # ADD the isalnum() check to ensure there's speakable text
                                    if sentence_buffer.strip() and enable_tts and any(c.isalnum() for c in sentence_buffer):
                                        await tts_queue.put(sentence_buffer.strip())
                                    await tts_queue.put(None) 
                                    break
                                
                                async with ws_lock:
                                    await websocket.send_text(token)
                                    
                            except (RuntimeError, WebSocketDisconnect):
                                await tts_queue.put(None)
                                break
                                
                            sentence_buffer += token
                            if any(p in token for p in punctuation):
                                sentence_to_speak = sentence_buffer.strip()
                                sentence_buffer = ""

                                # ADD the isalnum() check here as well
                                if sentence_to_speak and enable_tts and any(c.isalnum() for c in sentence_to_speak):
                                    await tts_queue.put(sentence_to_speak)
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    logger.error(f"🚨 Fatal text channel error: {str(e)}", exc_info=True)
                    await tts_queue.put(None)

            async def audio_loop():
                try:
                    while True:
                        sentence = await tts_queue.get()
                        if sentence is None:
                            try:
                                async with ws_lock:
                                    await websocket.send_text("[DONE]")
                                logger.info(f"✅ [{session_id}] Audio stream completed, ending conversation normally.")
                            except Exception:
                                pass
                            break 
                            
                        logger.debug(f"🎵 Triggering background TTS synthesis: {sentence}")
                        async for audio_chunk, _ in self.tts_service.stream_audio_generator(sentence):
                            try:
                                async with ws_lock:
                                    await websocket.send_bytes(audio_chunk)
                            except (RuntimeError, WebSocketDisconnect):
                                return
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    logger.error(f"🚨 Fatal audio channel error: {str(e)}", exc_info=True)

            try:
                await asyncio.gather(receive_loop(), text_loop(), audio_loop())
            except WebSocketDisconnect:
                logger.warning(f"[Gateway] User {session_id} disconnected unexpectedly.")
            except Exception as e:
                logger.error(f"🚨 WebSocket routing crash: {str(e)}", exc_info=True)
            finally:
                if asr_session:
                    await asr_session.finish() 
                await pubsub.unsubscribe(pubsub_channel)

    async def dispatch_and_wait(
        self, 
        target_agent: str, 
        payload: Dict[str, Any], 
        session_id: str, 
        auth_token: Optional[str] = None,
        timeout: float = 30.0,
        is_shadow: bool = False
    ) -> Dict[str, Any]:
        """
        Core methods for API routing calls: packaging, delivery, suspending, waiting, and unpacking.
        """
        envelope = EventEnvelope(
            session_id=session_id,
            auth_token=auth_token,
            source=self.source_name,  # [FIX] Use instance-specific stream as return address
            target=target_agent,
            payload=payload,
            is_shadow=is_shadow
        )
        
        target_stream = f"bus:events:{target_agent}"
        future = self.future_pool.create_future(envelope.trace_id)
        
        try:
            await self.redis.xadd(target_stream, envelope.to_redis_dict(), maxlen=10000, approximate=True)
            logger.info(f"📤 Task {envelope.trace_id} dispatched to {target_stream}")
            
            # [FIX] Introduce hard timeout to prevent memory leak/OOM caused by pending futures
            result_envelope: EventEnvelope = await asyncio.wait_for(future, timeout=timeout)
            return result_envelope.payload
            
        except asyncio.TimeoutError:
            self.future_pool.remove_future(envelope.trace_id)
            logger.error(f"⏳ Request timed out. Backend agent did not respond within {timeout}s (Trace: {envelope.trace_id})")
            try:
                sandbox_key = f"trace_sandbox:{envelope.trace_id}"
                sandbox_id = await self.redis.get(sandbox_key)
                if sandbox_id:
                    asyncio.create_task(CodeSandbox.destroy_sandbox(sandbox_id))
                    await self.redis.delete(sandbox_key)
            except Exception:
                pass
            raise HTTPException(status_code=504, detail="Gateway Timeout: Backend agent processing timeout")
        except Exception as e:
            self.future_pool.remove_future(envelope.trace_id)
            raise HTTPException(status_code=500, detail=f"Internal Error: {str(e)}")