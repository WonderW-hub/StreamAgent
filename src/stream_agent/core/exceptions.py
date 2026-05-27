# \src\stream_agent\exceptions.py
"""
StreamAgent framework custom exception collection
All framework-level exceptions are inherited from the unified base class and come with HTTP status code mapping.
This allows the Gateway layer to gracefully return errors to the front end through the Global Exception Handler.
"""
from typing import Optional

class StreamAgentError(Exception):
    """
    StreamAgent framework global base exception class.
    Comes with a status_code attribute, defaulting to 500.
    """
    status_code: int = 500

    def __init__(self, message: str, status_code: Optional[int] = None):
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        super().__init__(self.message)


# ==========================================
# 1. Client & Authentication Exceptions (4xx)
# ==========================================

class AuthorizationError(StreamAgentError):
    """
    Authorization failure exception (401)。
    JWT Token 缺失、过期或签名无效时抛出。
    """
    status_code = 401


class IdempotencyError(StreamAgentError):
    """
    Idempotency interception exception (409)。
    The anti-repetition mechanism is triggered, and the exception is thrown when duplicate trace_id is received within a short time.
    """
    status_code = 409


class PayloadValidationError(StreamAgentError):
    """
    Payload validation error (400)。
    Thrown when required fields are missing (e.g., device control is missing the 'pressure' parameter).
    """
    status_code = 400


class RoutingError(StreamAgentError):
    """
    Routing failure exception (404)。
    Thrown when the Orchestrator cannot find a matching target Agent queue.
    """
    status_code = 404


# ==========================================
# 2. 服务端与基建类异常 (5xx)
# ==========================================

class ContextError(StreamAgentError):
    """
    Security context boundary exception (500)。
    Thrown when business code脱离了 SessionContext.scope 却试图读取身份信息时抛出，属严重越权风险。
    """
    status_code = 500


class AgentTimeoutError(StreamAgentError):
    """
    Asynchronous bus waiting timeout exception (504)。
    The gateway does not receive the XACK or returned result from the backend Worker within the specified time (timeout)。
    """
    status_code = 504


class LLMGenerationError(StreamAgentError):
    """
    The large model generates an exception (502).
    The upstream large model API (such as vLLM/OpenAI) goes down, or is thrown when the forced JSON output parsing fails completely.
    """
    status_code = 502


class ToolExecutionError(StreamAgentError):
    """
    Internal tool/function call exception (500).
    Thrown when the Agent attempts to call a local tool (e.g., query database, call C++ dynamic library) and a crash occurs.
    """
    status_code = 500


class ConfigurationError(StreamAgentError):
    """
    Infrastructure configuration error exception (500).
    Thrown when Redis connection fails, environment variables are missing, or consumer group conflicts occur.
    """
    status_code = 500