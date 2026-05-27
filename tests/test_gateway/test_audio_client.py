import asyncio
import json
import wave
import websockets
import urllib

# ==========================================
# 🎯 网关配置 (对齐 StreamAgent)
# ==========================================
SERVER_IP = "127.0.0.1"
SERVER_PORT = 8000  
SESSION_ID = "MCU-Device-001"
AUTH_TOKEN = "Bearer test_token" 

# 🌟 使用 urlencode 自动处理空格和特殊字符
query_params = urllib.parse.urlencode({
    "session_id": SESSION_ID,
    "authorization": AUTH_TOKEN
})

WS_URL = f"ws://{SERVER_IP}:{SERVER_PORT}/v1/ws/chat?{query_params}"

# ==========================================
# 🎵 音频配置 (务必使用 16kHz, 16bit, 单声道 WAV)
# ==========================================
TEST_AUDIO_FILE = "tests/test_gateway/weather_fetcher.wav" 
OUTPUT_AUDIO_FILE = "test_output.wav"

async def send_audio_and_receive(ws, audio_chunks):
    """全双工收发核心逻辑"""
    
    # 1. 告诉网关：我要开始传音频了，快把 ASR 引擎准备好！
    print("[MCU] 🎤 发送唤醒指令 (start_audio)...")
    await ws.send(json.dumps({"action": "start_audio"}))
    await asyncio.sleep(0.1)

    # 2. 模拟真实说话，源源不断推送二进制音频块
    print("[MCU] 🎶 发送音频切片中...")
    for chunk in audio_chunks:
        await ws.send(chunk)
        await asyncio.sleep(0.05) # 模拟硬件麦克风的采样延迟

    # 3. 告诉网关：我说完了！去进行语义处理吧！
    print("[MCU] ⏹️ 发送静音指令 (stop_audio)...")
    await ws.send(json.dumps({"action": "stop_audio"}))

    print("[MCU] 🎧 麦克风已闭，正在监听下发音频与文本...\n[屏幕显示]: ", end="", flush=True)
    
    collected_pcm_data = bytearray()

    # 4. 死循环监听双通道回传
    while True:
        try:
            # 等待服务器推送，超时设长一点给大模型思考时间
            response = await asyncio.wait_for(ws.recv(), timeout=60.0)
            
            if isinstance(response, bytes):
                # 🎵 抓取到二进制帧：这是 TTS 发来的 PCM 音频流！
                collected_pcm_data.extend(response)
                # 注释掉下面这行以免控制台被刷屏
                # print(f"   [🎵 收到 {len(response)} 字节音频帧]") 
            else:
                # 💬 抓取到文本帧：判断是否结束
                if response == "[DONE]":
                    print("\n\n[MCU] ✅ 服务器宣告流式交互彻底结束。")
                    break
                else:
                    # 模拟打字机输出到控制台
                    print(response, end="", flush=True)

        except asyncio.TimeoutError:
            print("\n[MCU] ❌ 接收超时 (后端大模型或 TTS 可能已崩溃)")
            return False
        except websockets.exceptions.ConnectionClosed:
            print("\n[MCU] ❌ 网关强行断开了连接")
            return False

    # 5. 落盘保存 TTS 合成的音频
    if len(collected_pcm_data) > 0:
        with wave.open(OUTPUT_AUDIO_FILE, "wb") as wf:
            wf.setnchannels(1)           # 单声道
            wf.setsampwidth(2)           # 16-bit (2 bytes)
            wf.setframerate(16000)       # DashScope TTS 默认 16kHz
            wf.writeframes(collected_pcm_data)
        print(f"[MCU] 💾 对话音频已成功保存至: {OUTPUT_AUDIO_FILE}")
        return True
    else:
        print("[MCU] ⚠️ 本次会话没有接收到任何下发的音频流。")
        return False

async def main():
    print(f"🚀 正在连接流式网关: {WS_URL} ...")
    try:
        async with websockets.connect(WS_URL) as ws:
            print("[MCU] ✅ 连接建立成功！\n")

            # 预加载音频。
            # 注意：如果你的音频带有 WAV 文件头（前 44 个字节），标准的做法是跳过它，只发纯 PCM 数据
            audio_chunks = []
            with wave.open(TEST_AUDIO_FILE, "rb") as wf:
                # 如果帧率不是 16k，打印警告
                if wf.getframerate() != 16000:
                    print(f"⚠️ 警告: 输入音频采样率不是 16000Hz ({wf.getframerate()}Hz)，ASR 识别可能会乱码！")
                
                while True:
                    # 每次读取约 0.1 秒的帧
                    chunk = wf.readframes(1600) 
                    if not chunk:
                        break
                    audio_chunks.append(chunk)

            await send_audio_and_receive(ws, audio_chunks)
                    
    except Exception as e:
        print(f"[MCU] 💥 致命错误: {e}")

if __name__ == "__main__":
    asyncio.run(main())