"""记忆系统抽象基类 (支持无历史依赖模式)"""
from abc import ABC, abstractmethod
from typing import List, Dict, Any

class BaseMemory(ABC):
    """
    智能体记忆系统抽象基类。
    所有具体的记忆存储方案（如 Redis、SQLite、VectorDB）或无状态方案都必须继承此类。
    """

    @abstractmethod
    async def get_history(self, session_id: str, limit: int = 10) -> List[Dict[str, str]]:
        """
        获取指定会话的历史上下文消息列表。
        返回格式必须符合大模型标准的 Messages 格式: [{"role": "user", "content": "..."}, ...]
        """
        pass

    @abstractmethod
    async def save_message(self, session_id: str, role: str, content: str) -> None:
        """
        将一条新的消息（User输入或Assistant回复）持久化到当前会话中。
        """
        pass

    @abstractmethod
    async def clear(self, session_id: str) -> None:
        """
        彻底清空某个特定会话的所有记忆。
        """
        pass


class ZeroHistoryPlugin(BaseMemory):
    """
    内置经典插件：零历史依赖记忆（纯净无状态模式）。
    专为严谨的命令控制、参数微调等不需要多轮对话上下文的智能体准备。
    """
    async def get_history(self, session_id: str, limit: int = 10) -> List[Dict[str, str]]:
        # 永远返回空列表，确保大模型每次推理都只聚焦于当前的单次输入，彻底杜绝历史闲聊污染
        return []

    async def save_message(self, session_id: str, role: str, content: str) -> None:
        # 静默丢弃，不占用任何内存和存储资源
        pass

    async def clear(self, session_id: str) -> None:
        pass