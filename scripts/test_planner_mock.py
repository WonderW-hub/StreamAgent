# test_planner_mock.py
import asyncio
import json
import uuid
import redis.asyncio as redis

async def inject_test_task():
    r = redis.Redis.from_url("redis://localhost:6379/0", decode_responses=True)
    
    trace_id = f"test-trace-{uuid.uuid4().hex[:8]}"
    session_id = "test_user_wutao_001"
    
    envelope_data = {
        "trace_id": trace_id,
        "session_id": session_id,
        "source": "gateway",
        "target": "planner_agent",
        "payload": json.dumps({
            "goal": "我想要通过一个工具来获取一个文件的内容",
            "auth_token": "Bearer test_valid_token_888" 
        })
    }
    
    print(f"[{trace_id}] Injecting test targets into PlannerAgent...")
    
    msg_id = await r.xadd("bus:events:planner_agent", envelope_data)
    print(f"The injection was successful!message ID: {msg_id}")
    
    await r.aclose()

if __name__ == "__main__":
    asyncio.run(inject_test_task())