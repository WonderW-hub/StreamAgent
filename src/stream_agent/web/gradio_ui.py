# src/stream_agent/web/gradio_ui.py
import gradio as gr
import requests
import json
import uuid
import os
import logging
import asyncio
import websockets

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("StreamAgent.GradioUI")

GATEWAY_URL = os.getenv("GATEWAY_URL", "http://127.0.0.1:8000")
SSE_ENDPOINT = f"{GATEWAY_URL}/v1/sse/chat"
WS_ENDPOINT = GATEWAY_URL.replace("http://", "ws://").replace("https://", "wss://") + "/v1/ws/chat"

BASE_SESSION_ID = f"gradio_user_{uuid.uuid4().hex[:6]}"

# ==========================================
# 1. Existing SSE text processing functions 
# ==========================================
def chat_stream(message, history):
    headers = {
        "Content-Type": "application/json",
        "session-id": BASE_SESSION_ID 
    }
    payload = {"query": message}

    try:
        response = requests.post(
            SSE_ENDPOINT, headers=headers, json=payload, stream=True, timeout=60
        )
        response.raise_for_status()
        
        partial_reply = ""
        for line in response.iter_lines():
            if line:
                decoded_line = line.decode('utf-8')
                if decoded_line.startswith("data: "):
                    data_str = decoded_line[6:].strip()
                    if data_str == "[DONE]":
                        break
                    try:
                        data_json = json.loads(data_str)
                        if "content" in data_json:
                            partial_reply += data_json["content"]
                            yield partial_reply
                    except json.JSONDecodeError:
                        continue
    except Exception as e:
        yield f"❌ SSE Request failed: {str(e)}"


# ==========================================
# 2. 【New】 WebSocket voice processing asynchronous generator
# ==========================================
async def ws_audio_chat(audio_path, history):
    """
    Upload audio through the WebSocket interface and get a streaming response
    """
    if history is None:
        history = []
        
    if not audio_path:
        history.append({"role": "assistant", "content": "⚠️ Please record or upload an audio file first!"})
        yield history
        return
    
    # 1. Insert the user's placeholder message
    history.append({"role": "user", "content": "🎵 [The voice file has been sent and is being recognized...]"})
    # 2. Insert an empty message from the robot (for subsequent streaming updates)
    history.append({"role": "assistant", "content": ""})
    yield history

    session_id = f"audio_ws_{uuid.uuid4().hex[:6]}"
    ws_url = f"{WS_ENDPOINT}?session_id={session_id}"
    
    logger.info(f"🔗 Connecting to WebSocket to test audio: {ws_url}")

    try:
        async with websockets.connect(ws_url) as websocket:
            await websocket.send(json.dumps({
                "action": "start_audio", 
                "require_audio": False
            }))
            
            with open(audio_path, "rb") as f:
                while True:
                    chunk = f.read(4096)
                    if not chunk:
                        break
                    await websocket.send(chunk)
                    await asyncio.sleep(0.01)
                    
            await websocket.send(json.dumps({"action": "stop_audio"}))
            logger.info("🎤 The recording is sent, wait for the ASR and the large model to respond...")

            # Stream receive and update content in the dictionary
            partial_reply = ""
            async for message in websocket:
                if isinstance(message, str):
                    if message == "[DONE]":
                        break
                    
                    partial_reply += message
                    # Update the content of the last (assistant) in the history
                    history[-1]["content"] = partial_reply
                    yield history
                    
    except Exception as e:
        logger.error(f"WebSocket Error: {e}", exc_info=True)
        history[-1]["content"] += f"\n\n❌ WebSocket connection/processing failed: {str(e)}"
        yield history


# ==========================================
# 3. Build a UI with double tabs
# ==========================================
def build_ui():
    with gr.Blocks(title="StreamAgent Agent network") as demo:
        gr.Markdown(
            """
            # 🚀 StreamAgent Web Terminal
            **Multi-modal testing tool for microservice architecture**.Please select the tab below to test different gateway links.
            """
        )
        
        with gr.Tabs():
            # Tab 1: Original SSE text interface
            with gr.Tab("💬 Text Chat (SSE HTTP)"):
                gr.ChatInterface(
                    fn=chat_stream,
                    chatbot=gr.Chatbot(height=500),
                    textbox=gr.Textbox(placeholder="Please enter text...", container=False, scale=7)
                )
                
            # Tab 2: 【New】WebSocket voice testing interface
            with gr.Tab("🎙️ Voice Testing (WebSocket)"):
                gr.Markdown("Upload an audio file or use the microphone to record, the data will be split into binary chunks and sent via WebSocket to `v1/ws/chat`, receiving real-time text responses from the large model.")
                
                ws_chatbot = gr.Chatbot(height=400, label="WebSocket Response Stream")
                
                with gr.Row():
                    # Gradio provided multi-functional audio component
                    audio_input = gr.Audio(sources=["microphone", "upload"], type="filepath", label="Input Voice")
                    send_audio_btn = gr.Button("Send Voice to WS Interface", variant="primary", scale=1)
                
                # Bind click event to trigger async generator and update UI
                send_audio_btn.click(
                    fn=ws_audio_chat, 
                    inputs=[audio_input, ws_chatbot], 
                    outputs=[ws_chatbot]
                )
                
    return demo

if __name__ == "__main__":
    app = build_ui()
    logger.info("Starting Gradio Web UI, connecting to backend Gateway: %s", GATEWAY_URL)
    app.launch(server_name="0.0.0.0", server_port=7860, share=False)