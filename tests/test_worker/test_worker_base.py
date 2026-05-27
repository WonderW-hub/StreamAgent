import pytest
import json
from unittest.mock import AsyncMock, patch
from stream_agent.worker.base import WorkerBase
from stream_agent.core.envelope import EventEnvelope

# 构造一个用于测试的虚拟 Agent
class DummyAgent(WorkerBase):
    def __init__(self):
        super().__init__(agent_name="dummy_tester")
        
    async def handle_event(self, payload: dict) -> dict:
        # 简单回显逻辑，用于验证链路
        return {
            "summary": "处理成功",
            "received_data": payload.get("query")
        }

@pytest.mark.asyncio
@patch("stream_agent.worker.base.redis.from_url")
async def test_worker_initialization(mock_from_url):
    """测试 Worker 独立初始化逻辑，完美避开死循环"""
    mock_redis = AsyncMock()
    mock_from_url.return_value = mock_redis
    
    worker = DummyAgent()
    # 🌟 直接调用 setup()，只测试初始化，不进入 start() 的 while True
    await worker.setup()
    
    # 验证 Redis 连接参数
    mock_from_url.assert_called_once_with(worker.redis_url, decode_responses=True)
    
    # 验证消费者组创建
    mock_redis.xgroup_create.assert_called_once_with(
        "bus:events:dummy_tester", 
        "group_dummy_tester", 
        id="0", 
        mkstream=True
    )

@pytest.mark.asyncio
@patch("stream_agent.worker.base.redis.from_url")
async def test_worker_process_pipeline(mock_from_url):
    """
    测试核心处理管道：防重拦截 -> 上下文注入 -> 业务执行 -> 结果回传 -> ACK
    """
    mock_redis = AsyncMock()
    mock_from_url.return_value = mock_redis
    
    # 🌟 关键模拟：让幂等性锁 (SETNX) 返回 True，表示这是首次处理
    mock_redis.set.return_value = True 
    
    worker = DummyAgent()
    await worker.setup()
    
    # 1. 伪造一条网关发来的真实结构数据
    envelope = EventEnvelope(
        session_id="Session-123",
        auth_token="Bearer test",
        source="gateway",
        target="dummy_tester",
        action="process",
        payload={"query": "Hello AI"}
    )
    msg_id = "1680000000000-0"
    
    # 2. 直接将数据灌入核心处理管道，跳过 xreadgroup
    await worker._process_raw_message(msg_id, envelope.to_redis_dict())
    
    # 3. 断言验证：幂等性锁是否被正确设置
    idemp_key = f"idemp:{envelope.trace_id}:dummy_tester:v1.0"
    mock_redis.set.assert_any_call(idemp_key, "PROCESSING", nx=True, ex=3600)
    
    # 4. 断言验证：结果是否正确打包并 XADD 回传给网关
    mock_redis.xadd.assert_called_once()
    args, kwargs = mock_redis.xadd.call_args
    assert args[0] == "bus:events:gateway" # 目标路由必须是源地址(gateway)
    
    reply_dict = args[1]
    assert reply_dict["source"] == "dummy_tester"
    assert reply_dict["session_id"] == "Session-123"
    
    reply_payload = json.loads(reply_dict["payload"])
    assert reply_payload["summary"] == "处理成功"
    assert reply_payload["received_data"] == "Hello AI"
    
    # 5. 断言验证：处理完成后是否正确发送了 XACK
    mock_redis.xack.assert_called_once_with(
        "bus:events:dummy_tester", 
        "group_dummy_tester", 
        msg_id
    )

@pytest.mark.asyncio
@patch("stream_agent.worker.base.redis.from_url")
async def test_worker_idempotency_block(mock_from_url):
    """测试分布式幂等性拦截：如果消息重复，应立刻拦截并丢弃"""
    mock_redis = AsyncMock()
    mock_from_url.return_value = mock_redis
    
    # 🌟 关键模拟：让幂等性锁返回 False，模拟这已经被其他实例处理过了
    mock_redis.set.return_value = False 
    
    worker = DummyAgent()
    await worker.setup()
    
    envelope = EventEnvelope(session_id="123", source="gateway", target="dummy_tester", payload={})
    msg_id = "1680000000000-0"
    
    await worker._process_raw_message(msg_id, envelope.to_redis_dict())
    
    # 断言：业务逻辑不应被执行，结果不应回传，但必须被直接 ACK 掉以免堵塞队列
    mock_redis.xadd.assert_not_called()
    mock_redis.xack.assert_called_once_with("bus:events:dummy_tester", "group_dummy_tester", msg_id)