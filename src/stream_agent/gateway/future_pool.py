# src\stream_agent\core\future_pool.py
"""Asynchronous task suspension and Redis callback wake up engine"""
import asyncio
import logging
from typing import Dict, Any, Optional
from stream_agent.core.envelope import EventEnvelope

logger = logging.getLogger("StreamAgent.FuturePool")

class FuturePool:
    """
    Asynchronous task suspension and Redis callback wake up engine (The "Call Buzzer" System)
    Used to establish a corresponding relationship between stateless HTTP requests and asynchronous Redis Streams.
    """
    def __init__(self):
        # Store mappings of the form { "trace_id": asyncio.Future }
        self._pool: Dict[str, asyncio.Future] = {}

    def create_future(self, trace_id: str) -> asyncio.Future:
        """
        Create a "blank order call" (Future) for requests about to be sent to the bus.
        """
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self._pool[trace_id] = future
        logger.debug(f"Future suspension hook created: {trace_id}")
        return future

    def resolve_future(self, trace_id: str, envelope: EventEnvelope) -> bool:
        """
        When the background listener receives results from Redis, call this method to wake up the corresponding HTTP request.
        """
        future = self._pool.pop(trace_id, None)
        if future and not future.done():
            future.set_result(envelope)
            logger.debug(f"Future hook awakened: {trace_id}")
            return True
        else:
            logger.warning(f"Failed to awaken: Future not found or already timed out (Trace: {trace_id})")
            return False

    def remove_future(self, trace_id: str):
        """When timeout or exceptions occur, clean up residual Futures in memory"""
        self._pool.pop(trace_id, None)