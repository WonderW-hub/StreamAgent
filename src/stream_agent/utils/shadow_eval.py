"""无缝影子测试评测与 BLEU 算法工具"""
import asyncio
import json
import logging
import difflib
from typing import Dict, Any, Optional
import redis.asyncio as redis
from redis.exceptions import ResponseError

from stream_agent.core.envelope import EventEnvelope

# 单独为评测引擎配置一个干净的日志格式，避免被其他系统日志淹没
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger("ShadowEval")

class ShadowEvaluator:
    """
    LLMOps 影子测试实时评测引擎。
    作为一个独立的守护进程运行，专职消费 shadow_eval 总线中的影子流量结果，
    并实时输出对比报表。
    """
    def __init__(self, redis_url: str = "redis://localhost:6379/0"):
        self.redis_url = redis_url
        self.redis: Optional[redis.Redis] = None
        self.stream_name = "bus:events:shadow_eval"
        self.group_name = "group_shadow_evaluator"
        self.consumer_name = "evaluator_1"

    async def setup(self):
        """初始化 Redis 并创建专属消费者组"""
        self.redis = redis.from_url(self.redis_url, decode_responses=True)
        try:
            await self.redis.xgroup_create(self.stream_name, self.group_name, id="0", mkstream=True)
        except ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise e

    def calculate_similarity(self, text1: str, text2: str) -> float:
        """
        计算两段文本的相似度。
        此处使用 Python 内置的 difflib 算法作为基线演示。
        在真实的生产环境中，这里可以直接替换为调用 Qwen/DeepSeek 小模型进行语义相似度(Semantic Similarity)打分。
        """
        if not text1 or not text2:
            return 0.0
        return difflib.SequenceMatcher(None, text1, text2).ratio()

    def print_dashboard(self, envelope: EventEnvelope):
        """
        在终端打印极具科技感的影子对齐实时面板。
        """
        trace_id = envelope.trace_id
        session_id = envelope.session_id
        source_agent = envelope.source
        
        # 提取影子模型的输出内容
        shadow_result = envelope.payload.get("summary", json.dumps(envelope.payload, ensure_ascii=False))
        
        # UI 渲染
        print("\n" + "="*70)
        print(f" 🧪 [SHADOW EVAL] 影子测试实时观测面板 ")
        print("="*70)
        print(f" 🔹 追踪链路 (Trace)   : {trace_id}")
        print(f" 🔹 用户会话 (Session) : {session_id}")
        print(f" 🔹 影子节点 (Source)  : {source_agent} (is_shadow=True)")
        print("-" * 70)
        print(f" 💡 影子节点输出内容:\n {shadow_result}")
        print("-" * 70)
        print(f" ⚙️ 自动化评分诊断:")
        print(f"    - 格式合法性: {'✅ PASS' if isinstance(envelope.payload, dict) else '❌ FAIL'}")
        print(f"    - 上下文隔离: {'✅ PASS' if session_id else '❌ FAIL'}")
        
        # 如果你能在 Redis 里拿到同一条 trace_id 的生产节点(Prod)的输出，
        # 就可以在这里调用 calculate_similarity() 计算 BLEU/Rouge 并打分。
        # 为了演示，此处留出算法接入点。
        print(f"    - 语义一致性: [等待接入参考文本基线...]")
        print("="*70 + "\n")

    async def start(self):
        """启动监控引擎的死循环"""
        await self.setup()
        print(f"👁️‍🗨️ Shadow Evaluator 启动完毕，正在静默监听影子总线: {self.stream_name}...")
        
        try:
            while True:
                messages = await self.redis.xreadgroup(
                    groupname=self.group_name,
                    consumername=self.consumer_name,
                    streams={self.stream_name: ">"},
                    count=5,
                    block=2000
                )

                if not messages:
                    continue

                for stream, msg_list in messages:
                    for message_id, msg_data in msg_list:
                        try:
                            envelope = EventEnvelope.from_redis_dict(msg_data)
                            
                            # 渲染控制台仪表盘
                            self.print_dashboard(envelope)
                            
                            # 确认消费，使其从 PEL 中移除
                            await self.redis.xack(self.stream_name, self.group_name, message_id)
                        except Exception as e:
                            logger.error(f"影子评测包解析失败: {e}")

        except asyncio.CancelledError:
            print("👁️‍🗨️ 评测引擎安全关闭。")
        finally:
            if self.redis:
                await self.redis.aclose()

if __name__ == "__main__":
    # 允许直接作为独立脚本运行
    evaluator = ShadowEvaluator()
    asyncio.run(evaluator.start())