import dashscope
from dashscope.audio.asr import Recognition, RecognitionCallback, RecognitionResult
import asyncio
from stream_agent.config.settings import settings

dashscope.api_key = settings.DASHSCOPE_API_KEY

class AsyncASRCallback(RecognitionCallback):
    def __init__(self, loop, future):
        self.loop = loop
        self.future = future
        self.text = ""

    def on_event(self, result: RecognitionResult) -> None:
        sentence = result.get_sentence()
        if 'text' in sentence:
            self.text = sentence['text']

    def on_complete(self) -> None:
        if not self.future.done():
            self.loop.call_soon_threadsafe(self.future.set_result, self.text)

    def on_error(self, result: RecognitionResult) -> None:
        print(f"!!! [ASR Error] {result.message}")
        if not self.future.done():
            self.loop.call_soon_threadsafe(self.future.set_result, self.text)

class ASRSession:
    def __init__(self):
        self.loop = asyncio.get_event_loop()
        self.future = self.loop.create_future()
        self.callback = AsyncASRCallback(self.loop, self.future)

        self.recognition = Recognition(
            model='fun-asr-realtime',
            format='pcm', 
            sample_rate=16000,
            callback=self.callback
        )
        self.recognition.start()

    def push_audio(self, data: bytes):
        self.recognition.send_audio_frame(data)

    async def finish(self) -> str:
        self.recognition.stop()
        try:
            text = await self.future
            return text.strip()
        except Exception as e:
            print(f"!!! [ASR Finish Error] {e}")
            return ""

class ASRService:
    def __init__(self):
        pass

    async def close(self):
        pass

    def create_session(self) -> ASRSession:
        return ASRSession()