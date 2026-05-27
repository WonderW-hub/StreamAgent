"""会话与鉴权上下文隔离容器 (保障零串号)"""
import contextvars
from contextlib import contextmanager
from typing import Optional, Generator

# 由于是底座框架，我们可能需要延迟导入或者假设 EventEnvelope 在同一层级
from stream_agent.core.envelope import EventEnvelope


# ==========================================
# 1. 定义底层的协程局部变量 (ContextVars)
# ==========================================
# 这些变量对外部不可见，只能通过 SessionContext 类访问，以保证封装的安全性。
_trace_id_ctx: contextvars.ContextVar[str] = contextvars.ContextVar("trace_id", default="")
_session_id_ctx: contextvars.ContextVar[str] = contextvars.ContextVar("session_id", default="")
_auth_token_ctx: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar("auth_token", default=None)
_user_id_ctx: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar("user_id", default=None)
_is_shadow_ctx: contextvars.ContextVar[bool] = contextvars.ContextVar("is_shadow", default=False)


class SessionContext:
    """
    流式智能体框架的全局上下文管理器。
    
    提供了一个线程安全、协程隔离的沙盒执行环境。
    业务开发者无需在函数间层层传递鉴权参数，只需在任何地方调用 SessionContext.get_session_id() 即可安全获取。
    """

    @classmethod
    @contextmanager
    def scope(cls, envelope: EventEnvelope) -> Generator[None, None, None]:
        """
        创建一个安全的协程执行沙盒 (Scope)。
        
        用法:
            with SessionContext.scope(event_envelope):
                await process_task()
                
        在 with 块的生命周期内，所有对 SessionContext.get_xxx() 的调用都会指向该 envelope 的数据。
        退出 with 块时，环境将被严格清理，防止内存泄漏或污染下一个任务。
        """
        # 1. 挂载上下文 (并保存回退 Token，用于退出时的精准清理)
        t_trace = _trace_id_ctx.set(envelope.trace_id)
        t_session = _session_id_ctx.set(envelope.session_id)
        t_auth = _auth_token_ctx.set(envelope.auth_token)
        t_user = _user_id_ctx.set(envelope.user_id)
        t_shadow = _is_shadow_ctx.set(envelope.is_shadow)

        try:
            # 2. 将控制权交还给业务层进行大模型推理和工具调用
            yield
        finally:
            # 3. 执行完毕，绝对严谨地销毁当前协程的上下文痕迹
            _trace_id_ctx.reset(t_trace)
            _session_id_ctx.reset(t_session)
            _auth_token_ctx.reset(t_auth)
            _user_id_ctx.reset(t_user)
            _is_shadow_ctx.reset(t_shadow)

    # ==========================================
    # 2. 暴露给开发者的安全访问接口 (Getters)
    # ==========================================

    @classmethod
    def get_session_id(cls) -> str:
        """获取当前上下文的安全隔离 Session ID"""
        val = _session_id_ctx.get()
        if not val:
            raise RuntimeError("非法的上下文访问：当前不在 SessionContext.scope 沙盒执行范围内，或 Session ID 丢失。")
        return val

    @classmethod
    def get_trace_id(cls) -> str:
        """获取当前链路的唯一 Trace ID，常用于日志打印"""
        return _trace_id_ctx.get()

    @classmethod
    def get_auth_token(cls) -> Optional[str]:
        """获取 JWT Token，用于调用外部高权限医疗硬件或接口时进行二次验签"""
        return _auth_token_ctx.get()

    @classmethod
    def get_user_id(cls) -> Optional[str]:
        """获取解析后的用户 ID"""
        return _user_id_ctx.get()

    @classmethod
    def is_shadow_mode(cls) -> bool:
        """判断当前是否运行在无痕影子测试模式下"""
        return _is_shadow_ctx.get()