#src/stream_agent/core/context.py
"""Session and authentication context isolation container (guarantee zero serial number)"""
import contextvars
from contextlib import contextmanager
from typing import Optional, Generator
from stream_agent.core.envelope import EventEnvelope

_trace_id_ctx: contextvars.ContextVar[str] = contextvars.ContextVar("trace_id", default="")
_session_id_ctx: contextvars.ContextVar[str] = contextvars.ContextVar("session_id", default="")
_auth_token_ctx: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar("auth_token", default=None)
_user_id_ctx: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar("user_id", default=None)
_is_shadow_ctx: contextvars.ContextVar[bool] = contextvars.ContextVar("is_shadow", default=False)
_source_ctx: contextvars.ContextVar[str] = contextvars.ContextVar("source", default="")

class SessionContext:
    """
    Stream agent framework's global context manager.
    Provides a thread-safe, coroutine-isolated sandbox execution environment.
    Business developers do not need to pass authentication parameters between functions; they can simply call SessionContext.get_session_id() to safely retrieve the session ID.
    """

    @classmethod
    @contextmanager
    def scope(cls, envelope: EventEnvelope) -> Generator[None, None, None]:
        """
        Create a safe coroutine execution sandbox (Scope).
        Usage:
            with SessionContext.scope(event_envelope):
                await process_task()               
        Within the with block, all calls to SessionContext.get_xxx() will point to the data of that envelope.
        When exiting the with block, the environment will be strictly cleaned up to prevent memory leaks or contamination of the next task.
        """

        t_trace = _trace_id_ctx.set(envelope.trace_id)
        t_session = _session_id_ctx.set(envelope.session_id)
        t_auth = _auth_token_ctx.set(envelope.auth_token)
        t_user = _user_id_ctx.set(envelope.user_id)
        t_shadow = _is_shadow_ctx.set(envelope.is_shadow)
        t_source = _source_ctx.set(envelope.source)

        try:

            yield
        finally:
            _trace_id_ctx.reset(t_trace)
            _session_id_ctx.reset(t_session)
            _auth_token_ctx.reset(t_auth)
            _user_id_ctx.reset(t_user)
            _is_shadow_ctx.reset(t_shadow)
            _source_ctx.reset(t_source)

    # ==========================================
    # 2. Security access interfaces (Getters) exposed to developers
    # ==========================================

    @classmethod
    def get_session_id(cls) -> str:

        val = _session_id_ctx.get()
        if not val:
            raise RuntimeError("Illegal context access: not currently in SessionContext.Within the scope of execution of the scope sandbox, or the session ID is lost。")
        return val

    @classmethod
    def get_trace_id(cls) -> str:
        """Get the unique Trace ID of the current trace, commonly used for logging"""
        return _trace_id_ctx.get()

    @classmethod
    def get_auth_token(cls) -> Optional[str]:

        return _auth_token_ctx.get()

    @classmethod
    def get_user_id(cls) -> Optional[str]:

        return _user_id_ctx.get()

    @classmethod
    def is_shadow_mode(cls) -> bool:

        return _is_shadow_ctx.get()

    @classmethod
    def get_source(cls) -> str:
        return _source_ctx.get()