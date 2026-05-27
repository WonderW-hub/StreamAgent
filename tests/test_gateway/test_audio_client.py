import asyncio
import json
import wave
import websockets
import urllib


SERVER_IP = "127.0.0.1"
SERVER_PORT = 8000  
SESSION_ID = "MCU-Device-001"
AUTH_TOKEN = "Bearer test_token" 


query_params = urllib.parse.urlencode({
    "session_id": SESSION_ID,
    "authorization": AUTH_TOKEN
})

WS_URL = f"ws://{SERVER_IP}:{SERVER_PORT}/v1/ws/chat?{query_params}"


TEST_AUDIO_FILE = "tests/test_gateway/weather_fetcher.wav" 
OUTPUT_AUDIO_FILE = "test_output.wav"

async def send_audio_and_receive(ws, audio_chunks):

    print("[MCU] 🎤 send (start_audio)...")
    await ws.send(json.dumps({"action": "start_audio"}))
    await asyncio.sleep(0.1)

    print("[MCU] 🎶 send audio chunks...")
    for chunk in audio_chunks:
        await ws.send(chunk)
        await asyncio.sleep(0.05)


    print("[MCU] ⏹️ send mute command (stop_audio)...")
    await ws.send(json.dumps({"action": "stop_audio"}))

    print("[MCU] 🎧 The microphone is closed and the audio and text are being monitored...\n[screen display]: ", end="", flush=True)
    
    collected_pcm_data = bytearray()

    while True:
        try:

            response = await asyncio.wait_for(ws.recv(), timeout=60.0)
            
            if isinstance(response, bytes):
                collected_pcm_data.extend(response)
            else:
                if response == "[DONE]":
                    print("\n\n[MCU] ✅ 服务器宣告流式交互彻底结束。")
                    break
                else:
                    print(response, end="", flush=True)

        except asyncio.TimeoutError:
            print("\n[MCU] ❌ Reception timeout (the backend large model or TTS may have crashed)")
            return False
        except websockets.exceptions.ConnectionClosed:
            print("\n[MCU] ❌ The gateway forcibly closed the connection.")
            return False

    if len(collected_pcm_data) > 0:
        with wave.open(OUTPUT_AUDIO_FILE, "wb") as wf:
            wf.setnchannels(1)           
            wf.setsampwidth(2)          
            wf.setframerate(16000)     
            wf.writeframes(collected_pcm_data)
        print(f"[MCU] 💾 The conversation audio has been successfully saved to: {OUTPUT_AUDIO_FILE}")
        return True
    else:
        print("[MCU] ⚠️ This session did not receive any audio stream sent out。")
        return False

async def main():
    print(f"🚀 Connecting to a streaming gateway: {WS_URL} ...")
    try:
        async with websockets.connect(WS_URL) as ws:
            print("[MCU] ✅ The connection was established successfully!\n")

            audio_chunks = []
            with wave.open(TEST_AUDIO_FILE, "rb") as wf:
                if wf.getframerate() != 16000:
                    print(f"⚠️ Warning: The input audio sample rate is not 16000Hz ({wf.getframerate()}Hz), ASR recognition may produce garbled text!")
                
                while True:

                    chunk = wf.readframes(1600) 
                    if not chunk:
                        break
                    audio_chunks.append(chunk)

            await send_audio_and_receive(ws, audio_chunks)
                    
    except Exception as e:
        print(f"[MCU] 💥 Fatal error: {e}")

if __name__ == "__main__":
    asyncio.run(main())