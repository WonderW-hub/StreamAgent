"""基于 FastAPI 的异步网关基类"""
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

# 导入 BaseModel 和全局配置
from pydantic import BaseModel
from stream_agent.config.settings import settings

# 定义前端发来的请求体结构
class ChatRequest(BaseModel):
    query: str

logger = logging.getLogger("StreamAgent.Gateway")

class GatewayServer:
    """
    工业级异步网关基类。
    集成了 FastAPI 生命周期管理、Redis 后台监听引擎，以及标准的 HTTP 转 Stream 投递方法。
    同时支持极致延迟的 WebSocket + Redis Pub/Sub 流式推送。
    """
    def __init__(self, title: str = settings.PROJECT_NAME, redis_url: str = settings.REDIS_URL):
        self.redis_url = redis_url
        self.redis: Optional[redis.Redis] = None
        
        # 1. 初始化核心引擎
        self.future_pool = FuturePool()
        self.gateway_stream = "bus:events:gateway"
        self.gateway_group = "group_gateway"
        
        # 2. 绑定 FastAPI 生命周期
        self.app = FastAPI(title=title, lifespan=self._lifespan)
        
        # 后台监听任务的句柄
        self._listener_task: Optional[asyncio.Task] = None

        # 3. 挂载多模态引擎
        self.asr_service = ASRService()
        self.tts_service = TTSService()
        
        # 4. 注册所有的 API 和 WebSocket 路由（必须在多模态服务挂载之后调用）
        self.setup_routes()

    @asynccontextmanager
    async def _lifespan(self, app: FastAPI):
        """FastAPI 启动与销毁的钩子"""
        # --- 启动阶段 ---
        self.redis = redis.from_url(self.redis_url, decode_responses=True)
        
        # 在 Redis 中为网关建立独立的消费者组，专门接收大模型回传的结果
        try:
            await self.redis.xgroup_create(self.gateway_stream, self.gateway_group, id="0", mkstream=True)
        except ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise e
                
        # 派生后台守护协程，死循环监听回传队列
        self._listener_task = asyncio.create_task(self._listen_for_replies())
        logger.info(f"🚀 网关启动成功！后台已开始监听 {self.gateway_stream}")

        # 初始化 TTS 云端连接
        self.tts_service.initialize()
         
        yield # 交还控制权给 FastAPI
        
        # --- 销毁阶段 ---
        logger.info("🛑 网关正在关闭，清理资源...")
        if self._listener_task:
            self._listener_task.cancel()
        if self.redis:
            await self.redis.aclose()

    async def _listen_for_replies(self):
        """
        后台守护进程：像传菜员一样，死死盯着 Redis 回传队列。
        """
        try:
            while True:
                messages = await self.redis.xreadgroup(
                    groupname=self.gateway_group,
                    consumername="gateway_worker_1",
                    streams={self.gateway_stream: ">"},
                    count=10,
                    block=1000
                )
                
                if not messages:
                    continue
                    
                for stream, msg_list in messages:
                    for msg_id, msg_data in msg_list:
                        try:
                            envelope = EventEnvelope.from_redis_dict(msg_data)
                            
                            # 如果是 WebSocket 的完结回执，直接静默 ACK，不需要唤醒 Future
                            if envelope.trace_id.startswith("req-ws-"):
                                logger.debug(f"[Gateway] 收到流式任务 {envelope.trace_id} 的底层完结信号，已静默回收。")
                                await self.redis.xack(self.gateway_stream, self.gateway_group, msg_id)
                                continue
                                
                            # 击中内存池，唤醒挂起的 HTTP 协程
                            self.future_pool.resolve_future(envelope.trace_id, envelope)
                            await self.redis.xack(self.gateway_stream, self.gateway_group, msg_id)
                            
                        except Exception as e:
                            logger.error(f"解析回传结果失败: {e}")
                            
        except asyncio.CancelledError:
            logger.info("监听任务被安全取消。")

    def setup_routes(self):
        """🌟 动态挂载路由体系（路由必须包裹在方法体内以访问 self）"""

        # 1. 补回传统的 HTTP 同步阻塞接口
        @self.app.post("/v1/chat")
        async def chat_endpoint(request: ChatRequest, session_id: str = Header(..., description="请求头中的鉴权会话ID")):
            return await self.dispatch_and_wait(
                target_agent="dispatcher", 
                payload={"query": request.query},
                session_id=session_id,
                timeout=60.0
            )

        # 2. 挂载流式多模态双协程全双工 WebSocket
        @self.app.websocket("/v1/ws/chat")
        async def websocket_chat(
            websocket: WebSocket,
            session_id: str = Query(..., description="URL参数中的用户会话ID"),
            authorization: Optional[str] = Query(None, description="URL参数中的鉴权Token")
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
                logger.error(f"🚨 Redis 订阅失败，强行终止: {e}")
                await websocket.close(code=1011)
                return
            
            asr_session = None
            tts_queue = asyncio.Queue()
            ws_lock = asyncio.Lock()

            # 🌟 新增：动态 TTS 开关，默认开启以兼容硬件外设
            enable_tts = True

            # 🚀 协程 1：收件员
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
                            
                            # 🌟 核心拦截：读取客户端配置的 require_audio 参数
                            enable_tts = data.get("require_audio", True)
                            
                            if action == "start_audio":
                                logger.info(f"🎤 [{session_id}] 开启麦克风，初始化 ASR...")
                                asr_session = self.asr_service.create_session()
                            elif action == "stop_audio":
                                if asr_session:
                                    logger.info(f"🛑 [{session_id}] 停止说话，等待 ASR 最终结果...")
                                    text_result = await asr_session.finish()
                                    asr_session = None
                                    
                                    if text_result:
                                        logger.info(f"📝 [{session_id}] ASR 识别完毕: {text_result}")
                                        envelope = EventEnvelope(
                                            trace_id=trace_id, session_id=session_id,
                                            auth_token=authorization, source="gateway", target="dispatcher",
                                            action="process", payload={"query": text_result}
                                        )
                                        await self.redis.xadd("bus:events:dispatcher", envelope.to_redis_dict())
                                        
                            elif action == "chat":
                                query = data.get("query", "")
                                logger.info(f"💬 [{session_id}] 网关收到前端纯文本指令: '{query}' (TTS开启状态: {enable_tts})")
                                
                                envelope = EventEnvelope(
                                    trace_id=trace_id, session_id=session_id,
                                    auth_token=authorization, source="gateway", target="dispatcher",
                                    action="process", payload={"query": query}
                                )
                                try:
                                    await self.redis.xadd("bus:events:dispatcher", envelope.to_redis_dict())
                                except Exception as e:
                                    logger.error(f"🚨 任务投递 Redis 失败: {e}")

                        elif "bytes" in message:
                            if asr_session:
                                asr_session.push_audio(message["bytes"])
                except WebSocketDisconnect:
                    logger.debug(f"[Gateway] [{session_id}] 客户端正常断开。")
                    await tts_queue.put(None)
                except Exception as e:
                    logger.error(f"🚨 接收通道致命错误: {str(e)}", exc_info=True)
                    await tts_queue.put(None)

            # 🚀 协程 2：打字员
            async def text_loop():
                sentence_buffer = ""
                punctuation = set("。！？；.!?;")
                try:
                    async for message in pubsub.listen():
                        if message["type"] == "message":
                            token = message["data"]
                            try:
                                if token == "[DONE]":
                                    # 🌟 只有开启 TTS 时，才把最后一句话扔进语音队列
                                    if sentence_buffer.strip() and enable_tts:
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
                                # 🌟 只有开启 TTS 时，才触发后台语音合成
                                if sentence_to_speak and enable_tts:
                                    await tts_queue.put(sentence_to_speak)
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    logger.error(f"🚨 文本通道致命错误: {str(e)}", exc_info=True)
                    await tts_queue.put(None)

            # 🚀 协程 3：播音员 (无需任何修改，完美兼容)
            async def audio_loop():
                try:
                    while True:
                        sentence = await tts_queue.get()
                        if sentence is None:
                            try:
                                async with ws_lock:
                                    await websocket.send_text("[DONE]")
                                logger.info(f"✅ [{session_id}] 音频流下发完毕 (或被客户端跳过)，正常结束对话。")
                            except Exception:
                                pass
                            break 
                            
                        logger.debug(f"🎵 触发后台 TTS 合成: {sentence}")
                        async for audio_chunk, _ in self.tts_service.stream_audio_generator(sentence):
                            try:
                                async with ws_lock:
                                    await websocket.send_bytes(audio_chunk)
                            except (RuntimeError, WebSocketDisconnect):
                                return
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    logger.error(f"🚨 音频通道致命错误: {str(e)}", exc_info=True)

            try:
                await asyncio.gather(receive_loop(), text_loop(), audio_loop())
            except WebSocketDisconnect:
                logger.warning(f"[Gateway] 用户 {session_id} 的 WS 异常断开。")
            except Exception as e:
                logger.error(f"🚨 WebSocket 核心路由崩溃: {str(e)}", exc_info=True)
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
        供 API 路由调用的核心方法：打包、投递、挂起、等待、解包。
        """
        envelope = EventEnvelope(
            session_id=session_id,
            auth_token=auth_token,
            source="gateway",
            target=target_agent,
            payload=payload,
            is_shadow=is_shadow
        )
        
        target_stream = f"bus:events:{target_agent}"
        future = self.future_pool.create_future(envelope.trace_id)
        
        try:
            await self.redis.xadd(target_stream, envelope.to_redis_dict(), maxlen=10000, approximate=True)
            logger.info(f"📤 任务 {envelope.trace_id} 已投递至 {target_stream}")
            
            result_envelope: EventEnvelope = await asyncio.wait_for(future, timeout=timeout)
            return result_envelope.payload
            
        except asyncio.TimeoutError:
            self.future_pool.remove_future(envelope.trace_id)
            logger.error(f"⏳ 请求超时，后端 Agent 未在 {timeout}s 内响应 (Trace: {envelope.trace_id})")
            raise HTTPException(status_code=504, detail="Gateway Timeout: 后端智能体处理超时")
        except Exception as e:
            self.future_pool.remove_future(envelope.trace_id)
            raise HTTPException(status_code=500, detail=f"Internal Error: {str(e)}")