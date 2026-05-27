import asyncio
import websockets
import json
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
async def test_stream():
    # 注意我们在 URL 里严格传入了 session_id
    uri = WS_URL
    
    async with websockets.connect(uri) as websocket:
        print("🔗 已连接到 StreamAgent 流式网关！")
        
        # 🌟 修复 1：补齐 action 字段，确保网关精准走到 chat 分支
        payload = {
            "action": "chat",
            "query": "给我写一篇500字的关于AI改变未来的小红书爆款文案，要求包含3个小标题，带有强烈的煽动性，并多用一些emoji表情！",
            "require_audio": False
        }
        await websocket.send(json.dumps(payload))
        
        print("🤖 正在接收实时流：", end="", flush=True)
        
        while True:
            token = await websocket.recv()
            
            # 🌟 修复 2：模态隔离！防止二进制语音包卡死终端
            if isinstance(token, bytes):
                # 这里收到了 TTS 后台下发的语音流
                # 你可以把它追加写入到 .wav 文件里，这里我们先静默忽略
                continue
                
            if token == "[DONE]":
                break
                
            # 只有纯文本才会走到这里，安全打印
            print(token, end="", flush=True)
            
        print("\n✅ 流式接收完毕。")

if __name__ == "__main__":
    asyncio.run(test_stream())