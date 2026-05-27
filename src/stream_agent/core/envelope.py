"""定义全网统一数据包协议 EventEnvelope"""
import json
import uuid
import time
from typing import Dict, Any, Optional
from pydantic import BaseModel, Field

class EventEnvelope(BaseModel):
    """
    全网统一数据包协议 (Event Envelope)
    用于在 Gateway、Orchestrator、Worker 以及 Shadow 节点之间进行标准化通信。
    """
    
    # ==========================================
    # 1. 链路追踪与幂等性防线 (Tracing & Idempotency)
    # ==========================================
    trace_id: str = Field(
        default_factory=lambda: f"req-{uuid.uuid4().hex}",
        description="全局唯一请求ID，用于全链路追踪与 Worker 端的防重拦截（幂等锁）"
    )
    timestamp: float = Field(
        default_factory=time.time,
        description="事件创建时间戳"
    )
    is_shadow: bool = Field(
        default=False,
        description="影子测试标记。若为 True，Worker 处理完后绝不将结果推回生产链路，而是打入影子评估总线。"
    )

    # ==========================================
    # 2. 严格的会话与鉴权隔离 (Auth & Session Isolation)
    # ==========================================
    session_id: str = Field(
        ..., 
        description="用户会话ID，用于沙盒记忆的精确隔离，绝对禁止串号"
    )
    auth_token: Optional[str] = Field(
        default=None,
        description="透传的 JWT Token 或认证凭证，Worker 层可在调用高权限外部工具时进行二次校验"
    )
    user_id: Optional[str] = Field(
        default=None,
        description="解析出的用户唯一标识"
    )

    # ==========================================
    # 3. 路由分发机制 (Routing Target)
    # ==========================================
    source: str = Field(
        ..., 
        description="消息发送方标识 (例如: 'gateway', 'supervisor')"
    )
    target: str = Field(
        ..., 
        description="消息接收方/目标 Agent 标识 (例如: 'DeviceControlAgent', 'supervisor')"
    )
    action: str = Field(
        default="process",
        description="目标执行动作指令 (例如: 'chat', 'execute_tool', 'error_reply')"
    )

    # ==========================================
    # 4. 业务载荷与扩展元数据 (Payload & Metadata)
    # ==========================================
    payload: Dict[str, Any] = Field(
        default_factory=dict,
        description="实际的业务数据。如 {'query': '调高压力', 'tool_args': {...}, 'summary': '...'}"
    )
    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="系统级扩展字段，供中间件使用。如重试次数、客户端 IP 等"
    )

    def to_redis_dict(self) -> Dict[str, str]:
        """
        序列化为 Redis Stream 支持的扁平字典格式。
        由于 Redis Stream (XADD) 的 value 必须是 string 或 bytes，
        我们需要将字典结构的 payload 和 metadata 转换为 JSON 字符串。
        """
        return {
            "trace_id": self.trace_id,
            "timestamp": str(self.timestamp),
            "is_shadow": str(self.is_shadow),
            "session_id": self.session_id,
            "auth_token": self.auth_token or "",
            "user_id": self.user_id or "",
            "source": self.source,
            "target": self.target,
            "action": self.action,
            "payload": json.dumps(self.payload, ensure_ascii=False),
            "metadata": json.dumps(self.metadata, ensure_ascii=False)
        }

    @classmethod
    def from_redis_dict(cls, data: Dict[str, Any]) -> "EventEnvelope":
        """
        从 Redis Stream 读取数据后，反序列化为 EventEnvelope 对象。
        注意：这依赖于 redis 客户端初始化时开启了 decode_responses=True，
        使得传入的 data 的 key 和 value 都是 string。
        """
        # 处理布尔值转换
        is_shadow_str = data.get("is_shadow", "False").lower()
        is_shadow = is_shadow_str == "true"

        # 处理可选字段的空字符串还原
        auth_token = data.get("auth_token")
        if not auth_token:
            auth_token = None
            
        user_id = data.get("user_id")
        if not user_id:
            user_id = None

        return cls(
            trace_id=data["trace_id"],
            timestamp=float(data.get("timestamp", time.time())),
            is_shadow=is_shadow,
            session_id=data["session_id"],
            auth_token=auth_token,
            user_id=user_id,
            source=data["source"],
            target=data["target"],
            action=data.get("action", "process"),
            payload=json.loads(data.get("payload", "{}")),
            metadata=json.loads(data.get("metadata", "{}"))
        )