# src\stream_agent\core\memory.py
"""Memory system abstract base class (supports no-history dependency mode)"""
from abc import ABC, abstractmethod
from typing import List, Dict, Any

class BaseMemory(ABC):
    """
    Abstract base class of agent memory system.
    All specific memory storage schemes (such as Redis, SQLite, VectorDB) or stateless schemes must inherit this class.
    """

    @abstractmethod
    async def get_history(self, session_id: str, limit: int = 10) -> List[Dict[str, str]]:
        """
        Get a list of historical context messages for the specified session.
        The return format must conform to the Messages format of the large model standard: [{"role": "user", "content": "..."}, ...]
        """
        pass

    @abstractmethod
    async def save_message(self, session_id: str, role: str, content: str) -> None:
        """
        Persist a new message (user input or Assistant reply) into the current session.
        """
        pass

    @abstractmethod
    async def clear(self, session_id: str) -> None:
        """
        Completely empty all memories of a particular session.
        """
        pass


class ZeroHistoryPlugin(BaseMemory):
    """
    Built-in classic plug-in: zero history dependent memory (pure stateless mode).
    It is specially prepared for agents that do not require multiple rounds of dialogue context such as rigorous command control and parameter fine-tuning.
    """
    async def get_history(self, session_id: str, limit: int = 10) -> List[Dict[str, str]]:

        return []

    async def save_message(self, session_id: str, role: str, content: str) -> None:

        pass

    async def clear(self, session_id: str) -> None:
        pass