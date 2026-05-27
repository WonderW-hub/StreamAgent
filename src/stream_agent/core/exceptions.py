"""
StreamAgent 框架自定义异常集合
所有框架级异常均继承自统一基类，自带 HTTP 状态码映射。
这允许 Gateway 层通过全局异常处理器 (Global Exception Handler) 优雅地向前端返回错误。
"""
from typing import Optional

class StreamAgentError(Exception):
    """
    StreamAgent 框架全局基础异常类。
    自带 status_code 属性，默认 500。
    """
    status_code: int = 500

    def __init__(self, message: str, status_code: Optional[int] = None):
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        super().__init__(self.message)


# ==========================================
# 1. 客户端与鉴权类异常 (4xx)
# ==========================================

class AuthorizationError(StreamAgentError):
    """
    鉴权失败异常 (401)。
    JWT Token 缺失、过期或签名无效时抛出。
    """
    status_code = 401


class IdempotencyError(StreamAgentError):
    """
    幂等性拦截异常 (409)。
    防重机制触发，当短时间内收到重复的 trace_id 时抛出。
    """
    status_code = 409


class PayloadValidationError(StreamAgentError):
    """
    数据载荷校验异常 (400)。
    缺少必填字段（如设备控制缺少 'pressure' 参数）时抛出。
    """
    status_code = 400


class RoutingError(StreamAgentError):
    """
    路由分发失败异常 (404)。
    当 Orchestrator 找不到匹配的目标 Agent 队列时抛出。
    """
    status_code = 404


# ==========================================
# 2. 服务端与基建类异常 (5xx)
# ==========================================

class ContextError(StreamAgentError):
    """
    安全上下文越界异常 (500)。
    业务代码脱离了 SessionContext.scope 却试图读取身份信息时抛出，属严重越权风险。
    """
    status_code = 500


class AgentTimeoutError(StreamAgentError):
    """
    异步总线等待超时异常 (504)。
    网关在规定时间 (timeout) 内未收到后端 Worker 的 XACK 或回传结果。
    """
    status_code = 504


class LLMGenerationError(StreamAgentError):
    """
    大模型生成异常 (502)。
    上游大模型 API (如 vLLM/OpenAI) 宕机，或强制 JSON 输出解析彻底失败时抛出。
    """
    status_code = 502


class ToolExecutionError(StreamAgentError):
    """
    内部工具/函数调用异常 (500)。
    当 Agent 尝试调用本地工具（如查询数据库、调用 C++ 动态库）发生崩溃时抛出。
    """
    status_code = 500


class ConfigurationError(StreamAgentError):
    """
    基建配置错误异常 (500)。
    Redis 连接失败、环境变量缺失，或消费者组建组冲突时抛出。
    """
    status_code = 500