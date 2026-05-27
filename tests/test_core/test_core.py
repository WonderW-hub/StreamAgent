import pytest
import asyncio
from stream_agent.core.envelope import EventEnvelope
from stream_agent.core.context import SessionContext

def test_event_envelope_serialization_and_auth():

    original_payload = {"query": "测试文本", "require_audio": False}
    

    envelope = EventEnvelope(
        session_id="Session-Test-001",
        auth_token="Bearer my-secure-token",
        source="gateway",
        target="writer",
        action="process",
        payload=original_payload,
        is_shadow=False
    )
    

    redis_dict = envelope.to_redis_dict()
    

    assert "trace_id" in redis_dict
    assert redis_dict["session_id"] == "Session-Test-001"
    assert redis_dict["auth_token"] == "Bearer my-secure-token" 
    assert redis_dict["target"] == "writer"

    reconstructed_envelope = EventEnvelope.from_redis_dict(redis_dict)

    assert reconstructed_envelope.trace_id == envelope.trace_id
    assert reconstructed_envelope.auth_token == envelope.auth_token
    assert reconstructed_envelope.payload == original_payload


@pytest.mark.asyncio
async def test_session_context_isolation():

    async def worker_task(session_id: str, trace_id: str):

        mock_envelope = EventEnvelope(
            session_id=session_id,
            source="test",
            target="test",
            payload={}
        )

        mock_envelope.trace_id = trace_id

        with SessionContext.scope(mock_envelope):

            await asyncio.sleep(0.1)

            assert SessionContext.get_session_id() == session_id
            assert SessionContext.get_trace_id() == trace_id

    await asyncio.gather(
        worker_task("User-A", "Trace-A-123"),
        worker_task("User-B", "Trace-B-456"),
        worker_task("User-C", "Trace-C-789")
    )