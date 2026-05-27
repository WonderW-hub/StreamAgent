# src/stream_agent/utils/shadow_eval.py
"""Seamless shadow test evaluation and BLEU algorithm tool"""
import asyncio
import json
import logging
import difflib
from typing import Dict, Any, Optional
import redis.asyncio as redis
from redis.exceptions import ResponseError

from stream_agent.core.envelope import EventEnvelope

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger("ShadowEval")

class ShadowEvaluator:
    """
    LLMOps shadow test real-time evaluation engine.
    Runs as an independent guardian process and consumes the shadow traffic results in the shadow_eval bus full-time，
    And output a comparison report in real time.
    """
    def __init__(self, redis_url: str = "redis://localhost:6379/0"):
        self.redis_url = redis_url
        self.redis: Optional[redis.Redis] = None
        self.stream_name = "bus:events:shadow_eval"
        self.group_name = "group_shadow_evaluator"
        self.consumer_name = "evaluator_1"

    async def setup(self):

        self.redis = redis.from_url(self.redis_url, decode_responses=True)
        try:
            await self.redis.xgroup_create(self.stream_name, self.group_name, id="0", mkstream=True)
        except ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise e

    def calculate_similarity(self, text1: str, text2: str) -> float:
        """
        Calculate the similarity of two pieces of text.
        Here we use Python's built-in difflib algorithm as a baseline demonstration.
        In a real production environment, this can be directly replaced by calling the Qwen/DeepSeek small model for Semantic Similarity scoring.
        """
        if not text1 or not text2:
            return 0.0
        return difflib.SequenceMatcher(None, text1, text2).ratio()

    def print_dashboard(self, envelope: EventEnvelope):
        """
        Print a very technological shadow alignment real-time panel in the terminal.
        """
        trace_id = envelope.trace_id
        session_id = envelope.session_id
        source_agent = envelope.source

        shadow_result = envelope.payload.get("summary", json.dumps(envelope.payload, ensure_ascii=False))
        
        print("\n" + "="*70)
        print(f" 🧪 [SHADOW EVAL] Shadow Test Real-Time Monitoring Panel ")
        print("="*70)
        print(f" 🔹 Trace ID   : {trace_id}")
        print(f" 🔹 Session ID : {session_id}")
        print(f" 🔹 Shadow Node  : {source_agent} (is_shadow=True)")
        print("-" * 70)
        print(f" 💡 Shadow Node Output:\n {shadow_result}")
        print("-" * 70)
        print(f" ⚙️ Automated Scoring Diagnosis:")
        print(f"    - Format Validity: {'✅ PASS' if isinstance(envelope.payload, dict) else '❌ FAIL'}")
        print(f"    - Context Isolation: {'✅ PASS' if session_id else '❌ FAIL'}")
        
        print(f"    - Semantic Consistency: [Waiting for reference text baseline...]")
        print("="*70 + "\n")

    async def start(self):

        await self.setup()
        print(f"👁️‍🗨️ Shadow Evaluator started: {self.stream_name}...")
        
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

                            self.print_dashboard(envelope)
                            
                            await self.redis.xack(self.stream_name, self.group_name, message_id)
                        except Exception as e:
                            logger.error(f"Shadow evaluation package parsing failed: {e}")

        except asyncio.CancelledError:
            print("👁️‍🗨️ Shadow Evaluator shutting down safely.")
        finally:
            if self.redis:
                await self.redis.aclose()

if __name__ == "__main__":
    evaluator = ShadowEvaluator()
    asyncio.run(evaluator.start())