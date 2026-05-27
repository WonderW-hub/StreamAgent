# src/stream_agent/core/envelope.py
"""Define a unified data packet protocol for the whole network EventEnvelope"""
import json
import uuid
import time
from typing import Dict, Any, Optional
from pydantic import BaseModel, Field

class EventEnvelope(BaseModel):
    """
    Unified Event Envelope Protocol
    Used for standardized communication between Gateway, Orchestrator, Worker, and Shadow nodes.
    """
    
    # ==========================================
    # 1. (Tracing & Idempotency)
    # ==========================================
    trace_id: str = Field(
        default_factory=lambda: f"req-{uuid.uuid4().hex}",
        description="Globally unique request ID, used for full-link tracking and anti-heavy interception on the worker side (idempotent lock）"
    )
    timestamp: float = Field(
        default_factory=time.time,
        description="Event creation timestamp"
    )
    is_shadow: bool = Field(
        default=False,
        description="Shadow test flag. If True, the Worker will not push the result back to the production chain after processing, but will instead push it to the shadow evaluation bus."
    )

    # ==========================================
    # 2. (Auth & Session Isolation)
    # ==========================================
    session_id: str = Field(
        ..., 
        description="User session ID, used for precise isolation of sandbox memory, absolutely prohibited from serial number"
    )
    auth_token: Optional[str] = Field(
        default=None,
        description="Transmitted JWT Token or authentication credential, Worker layer can perform secondary verification when calling high-privilege external tools"
    )
    user_id: Optional[str] = Field(
        default=None,
        description="Parsed user unique identifier"
    )

    # ==========================================
    # 3. (Routing Target)
    # ==========================================
    source: str = Field(
        ..., 
        description="Message sender identifier (e.g., 'gateway', 'supervisor')"
    )
    target: str = Field(
        ..., 
        description="Message receiver/target Agent identifier (e.g., 'DeviceControlAgent', 'supervisor')"
    )
    action: str = Field(
        default="process",
        description="Target execution action command (e.g., 'chat', 'execute_tool', 'error_reply')"
    )

    # ==========================================
    # 4. (Payload & Metadata)
    # ==========================================
    payload: Dict[str, Any] = Field(
        default_factory=dict,
        description="actual business data. e.g., {'query': '调高压力', 'tool_args': {...}, 'summary': '...'}"
    )
    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="System-level extension fields, used by middleware. e.g., retry count, client IP, etc."
    )

    def to_redis_dict(self) -> Dict[str, str]:
        """
        Serialize to the flat dictionary format supported by Redis Stream.
        Since Redis Stream (XADD) requires values to be strings or bytes,
        we need to convert the dictionary structures of payload and metadata to JSON strings.
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
        After reading data from Redis Stream, it is deserialized into an EventEnvelope object.
        Note: This depends on whether decode_responses=True is enabled when the redis client is initialized，
        So that the key and value of the incoming data are both string.
        """

        is_shadow_str = data.get("is_shadow", "False").lower()
        is_shadow = is_shadow_str == "true"


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