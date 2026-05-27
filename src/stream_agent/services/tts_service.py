import dashscope
from dashscope.audio.tts_v2 import SpeechSynthesizer, AudioFormat, ResultCallback
import asyncio
import threading
from stream_agent.config.settings import settings

dashscope.api_key = settings.DASHSCOPE_API_KEY

class AsyncTTSCallback(ResultCallback):
    def __init__(self, loop, queue, finish_event):
        self.loop = loop
        self.queue = queue
        self.finish_event = finish_event

    def on_open(self):
        pass 

    def on_complete(self):
        # ★ 云端数据流全部下发完毕，唤醒主阻塞线程
        self.finish_event.set()

    def on_error(self, message: str):
        print(f"!!! [TTS Cloud Error] {message}")
        # 出错也要唤醒，防止死锁
        self.finish_event.set()

    def on_event(self, message):
        pass 

    def on_data(self, data: bytes) -> None:
        # 源源不断接收云端发来的音频碎片
        if data:
            self.loop.call_soon_threadsafe(self.queue.put_nowait, data)

class TTSService:
    def __init__(self):
        self.is_ready = False

    def initialize(self):
        print(">>> [TTS] Connected to DashScope Cloud TTS. Ready.")
        self.is_ready = True

    async def stream_audio_generator(self, text: str):
        if not self.is_ready or not text:
            return

        q = asyncio.Queue()
        loop = asyncio.get_event_loop()
        SENTINEL = object()

        def producer():
            # 创建一个线程同步事件
            finish_event = threading.Event()
            try:
                callback = AsyncTTSCallback(loop, q, finish_event)
                synthesizer = SpeechSynthesizer(
                    model="cosyvoice-v3-flash", 
                    voice="longanyang",
                    format=AudioFormat.PCM_16000HZ_MONO_16BIT,
                    callback=callback
                )
                
                # 发送请求（变成非阻塞了，瞬间返回）
                synthesizer.call(text)
                
                # ★ 核心修复：在这里阻塞等待，直到被 on_complete 唤醒 (设10秒超时防卡死)
                finish_event.wait(timeout=10.0) 
                
            except Exception as e:
                print(f"!!! [TTS Generator Error] {e}")
            finally:
                # 只有真正收完了，才放入毒丸结束流
                loop.call_soon_threadsafe(q.put_nowait, SENTINEL)

        threading.Thread(target=producer, daemon=True).start()

        while True:
            chunk = await q.get()
            if chunk is SENTINEL:
                break
            
            # 持续将音频碎片 yield 给前端
            yield chunk, 16000