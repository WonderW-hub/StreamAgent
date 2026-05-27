import asyncio
import websockets
import json
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
async def test_stream():

    uri = WS_URL
    
    async with websockets.connect(uri) as websocket:
        print("🔗 Connected to StreamAgent streaming gateway！")
        
        payload = {
            "action": "chat",
            "query": "Write me a 500-word copywriting of Xiaohongshu's explosive style about AI changing the future. It requires 3 subheadings, which are strongly inflammatory, and use more emoji expressions.！",
            "require_audio": False
        }
        await websocket.send(json.dumps(payload))
        
        print("🤖 Connected to the streaming gateway and sent the chat request.", end="", flush=True)
        
        while True:
            token = await websocket.recv()
            

            if isinstance(token, bytes):

                continue
                
            if token == "[DONE]":
                break

            print(token, end="", flush=True)
            
        print("\n✅ Streaming reception is complete.")

if __name__ == "__main__":
    asyncio.run(test_stream())