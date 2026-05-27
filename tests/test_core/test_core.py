import pytest
import asyncio
from stream_agent.core.envelope import EventEnvelope
from stream_agent.core.context import SessionContext

def test_event_envelope_serialization_and_auth():
    """
    测试事件信封的序列化与反序列化，特别是鉴权令牌 (auth_token) 和 payload 的完整性
    """
    original_payload = {"query": "测试文本", "require_audio": False}
    
    # 1. 创建带完整隔离参数的信封
    envelope = EventEnvelope(
        session_id="Session-Test-001",
        auth_token="Bearer my-secure-token",
        source="gateway",
        target="writer",
        action="process",
        payload=original_payload,
        is_shadow=False
    )
    
    # 2. 转换为写入 Redis Stream 的字典格式
    redis_dict = envelope.to_redis_dict()
    
    # 断言核心数据存在
    assert "trace_id" in redis_dict
    assert redis_dict["session_id"] == "Session-Test-001"
    assert redis_dict["auth_token"] == "Bearer my-secure-token" # 鉴权必须无损
    assert redis_dict["target"] == "writer"
    
    # 3. 从 Redis 字典反序列化重构信封
    reconstructed_envelope = EventEnvelope.from_redis_dict(redis_dict)
    
    # 4. 深度验证（移除了不存在的 created_at 属性断言）
    assert reconstructed_envelope.trace_id == envelope.trace_id
    assert reconstructed_envelope.auth_token == envelope.auth_token
    assert reconstructed_envelope.payload == original_payload


@pytest.mark.asyncio
async def test_session_context_isolation():
    """
    测试异步上下文变量隔离。模拟在高并发场景下，不同协程的上下文互不干扰。
    """
    async def worker_task(session_id: str, trace_id: str):
        # 创建一个临时的 Envelope 用于注入上下文
        mock_envelope = EventEnvelope(
            session_id=session_id,
            source="test",
            target="test",
            payload={}
        )
        # 强制修改 trace_id 以便验证
        mock_envelope.trace_id = trace_id
        
        # 🌟 修复：使用真实的 scope 上下文管理器来模拟 Worker 的运行环境
        with SessionContext.scope(mock_envelope):
            
            # 模拟真实的 IO 阻塞（比如请求大模型），强制让出当前协程的执行权
            await asyncio.sleep(0.1)
            
            # 唤醒后，检查当前协程的上下文环境是否被其他正在并发的协程“污染”
            assert SessionContext.get_session_id() == session_id
            assert SessionContext.get_trace_id() == trace_id

    # 并发运行多个任务，验证底层的 ContextVar 安全隔离机制
    await asyncio.gather(
        worker_task("User-A", "Trace-A-123"),
        worker_task("User-B", "Trace-B-456"),
        worker_task("User-C", "Trace-C-789")
    )