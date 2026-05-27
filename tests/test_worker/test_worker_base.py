import pytest
import json
from unittest.mock import AsyncMock, patch
from stream_agent.worker.base import WorkerBase
from stream_agent.core.envelope import EventEnvelope


class DummyAgent(WorkerBase):
    def __init__(self):
        super().__init__(agent_name="dummy_tester")
        
    async def handle_event(self, payload: dict) -> dict:
        return {
            "summary": "Processed successfully",
            "received_data": payload.get("query")
        }

@pytest.mark.asyncio
@patch("stream_agent.worker.base.redis.from_url")
async def test_worker_initialization(mock_from_url):
    mock_redis = AsyncMock()
    mock_from_url.return_value = mock_redis
    
    worker = DummyAgent()
    await worker.setup()
    
    mock_from_url.assert_called_once_with(worker.redis_url, decode_responses=True)
    
    mock_redis.xgroup_create.assert_called_once_with(
        "bus:events:dummy_tester", 
        "group_dummy_tester", 
        id="0", 
        mkstream=True
    )

@pytest.mark.asyncio
@patch("stream_agent.worker.base.redis.from_url")
async def test_worker_process_pipeline(mock_from_url):

    mock_redis = AsyncMock()
    mock_from_url.return_value = mock_redis
    
    mock_redis.set.return_value = True 
    
    worker = DummyAgent()
    await worker.setup()
    

    envelope = EventEnvelope(
        session_id="Session-123",
        auth_token="Bearer test",
        source="gateway",
        target="dummy_tester",
        action="process",
        payload={"query": "Hello AI"}
    )
    msg_id = "1680000000000-0"
    

    await worker._process_raw_message(msg_id, envelope.to_redis_dict())
    

    idemp_key = f"idemp:{envelope.trace_id}:dummy_tester:v1.0"
    mock_redis.set.assert_any_call(idemp_key, "PROCESSING", nx=True, ex=3600)
    

    mock_redis.xadd.assert_called_once()
    args, kwargs = mock_redis.xadd.call_args
    assert args[0] == "bus:events:gateway" 
    
    reply_dict = args[1]
    assert reply_dict["source"] == "dummy_tester"
    assert reply_dict["session_id"] == "Session-123"
    
    reply_payload = json.loads(reply_dict["payload"])
    assert reply_payload["summary"] == "Processed successfully"
    assert reply_payload["received_data"] == "Hello AI"
    

    mock_redis.xack.assert_called_once_with(
        "bus:events:dummy_tester", 
        "group_dummy_tester", 
        msg_id
    )

@pytest.mark.asyncio
@patch("stream_agent.worker.base.redis.from_url")
async def test_worker_idempotency_block(mock_from_url):

    mock_redis = AsyncMock()
    mock_from_url.return_value = mock_redis

    mock_redis.set.return_value = False 
    
    worker = DummyAgent()
    await worker.setup()
    
    envelope = EventEnvelope(session_id="123", source="gateway", target="dummy_tester", payload={})
    msg_id = "1680000000000-0"
    
    await worker._process_raw_message(msg_id, envelope.to_redis_dict())

    mock_redis.xadd.assert_not_called()
    mock_redis.xack.assert_called_once_with("bus:events:dummy_tester", "group_dummy_tester", msg_id)