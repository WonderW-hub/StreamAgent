# tests/test_memory/test_summarized_memory.py
import pytest
import asyncio
import json
from unittest.mock import AsyncMock, patch

from stream_agent.memory.summarized_memory import SummarizedMemoryManager
from stream_agent.memory.redis_plugin import RedisMemoryPlugin

@pytest.mark.asyncio
@patch("stream_agent.memory.redis_plugin.redis.from_url")
@patch("stream_agent.memory.summarized_memory.AsyncLLMEngine.generate_text")
async def test_summarized_memory_flow(mock_generate_text, mock_redis_from_url):
    """
    测试点：
    1. 正常写入不阻塞主线程。
    2. 达到阈值后触发异步 LLM 摘要。
    3. 获取历史时能正确把摘要拼接到 System Prompt。
    4. 会话级别的 Lock 隔离正常工作。
    """
    # ==========================
    # 1. 模拟 Redis 客户端与返回值
    # ==========================
    mock_redis = AsyncMock()
    mock_redis_from_url.return_value = mock_redis
    
    # 模拟大模型生成的摘要
    mock_generate_text.return_value = "测试生成的浓缩记忆摘要：用户想要高并发部署。"
    
    # 模拟 Redis 获取历史 (假设当前缓存中有 8 条消息，刚好达到阈值)
    mock_history_data = [
        json.dumps({"role": "user", "content": f"第{i}轮对话"}) for i in range(8)
    ]
    mock_redis.lrange.return_value = mock_history_data
    
    # 模拟 Redis 获取已有的摘要
    mock_redis.get.return_value = None 

    # ==========================
    # 2. 初始化记忆管理器
    # ==========================
    session_id = "test_session_isolated_001"
    memory_manager = SummarizedMemoryManager(
        agent_name="test_agent",
        redis_url="redis://fake_url",
        l1_max_len=10,
        summarize_threshold=8 # 设置阈值为 8
    )

    # ==========================
    # 3. 测试保存消息与旁路摘要触发
    # ==========================
    # 这一步应该极速返回，后台会挂起一个 _safe_async_summarize 任务
    await memory_manager.save_message(session_id, "user", "触发摘要的新消息")
    
    # 等待事件循环中的后台任务执行完毕 (模拟真实运行中的旁路等待)
    await asyncio.sleep(0.1) 
    
    # 断言 LLM 被调用了一次（因为超过了 8 条）
    mock_generate_text.assert_called_once()
    
    # 断言生成的摘要被写回了 Redis
    summary_key = f"memory:summary:{session_id}"
    mock_redis.set.assert_any_call(summary_key, "测试生成的浓缩记忆摘要：用户想要高并发部署。", ex=2592000)
    
    # 断言 L1 缓存被安全修剪 (保留最近的 4 条，修剪掉旧的)
    history_key = f"memory:session:{session_id}"
    mock_redis.ltrim.assert_any_call(history_key, -4, -1)

    # ==========================
    # 4. 测试读取时自动注入摘要
    # ==========================
    # 改变 Redis.get 的返回值，模拟已经存有摘要
    mock_redis.get.return_value = "测试生成的浓缩记忆摘要：用户想要高并发部署。"
    
    # 获取历史记录
    history = await memory_manager.get_history(session_id)
    
    # 断言第一条消息被强行注入了 System Prompt，包含了我们压缩的摘要
    assert len(history) > 0
    assert history[0]["role"] == "system"
    assert "测试生成的浓缩记忆摘要" in history[0]["content"]
    assert "请结合上述摘要理解用户的后续请求" in history[0]["content"]

@pytest.mark.asyncio
async def test_session_isolation_locks():
    """验证多个 Session 并发触发摘要时，锁是严格物理隔离的"""
    manager = SummarizedMemoryManager(agent_name="test")
    
    lock1 = manager._get_session_lock("session_A")
    lock2 = manager._get_session_lock("session_B")
    lock3 = manager._get_session_lock("session_A")
    
    # 不同 Session 必须是不同的锁对象
    assert lock1 is not lock2
    # 同一 Session 必须复用同一个锁
    assert lock1 is lock3