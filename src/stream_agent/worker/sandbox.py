import asyncio
import logging
import concurrent.futures
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, Optional, Tuple
import io
import traceback
import contextlib
from typing import Tuple

logger = logging.getLogger("StreamAgent.Sandbox")

class EVUBase(ABC):
    """
    执行虚拟单元 (Execution Virtual Unit, EVU) 抽象基类。
    所有特定的 Agent 沙盒执行方案都必须继承此类。
    """
    @abstractmethod
    async def execute_tool(
        self, 
        tool_func: Callable, 
        args: Tuple[Any], 
        kwargs: Dict[str, Any], 
        timeout: float = 10.0
    ) -> Any:
        """
        在受限的沙盒环境中安全执行一个工具/函数。
        """
        pass

class ThreadPoolEVU(EVUBase):
    """
    基于线程池的执行虚拟单元 (隔离级：低)。
    专为防止工具崩溃拖垮协程主进程，以及处理高延迟阻塞操作设计。
    """
    def __init__(self, max_workers: int = 10):
        # 预先分配独立的线程池资源，不占用主协程的事件循环
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        logger.info(f"[Sandbox] 线程池 EVU 已启动 (隔离级：低) | Workers: {max_workers}")

    async def execute_tool(
        self, 
        tool_func: Callable, 
        args: Tuple[Any], 
        kwargs: Dict[str, Any], 
        timeout: float = 10.0
    ) -> Any:
        """
        将同步阻塞的工具调用提交到独立线程执行，并设置硬性超时限制。
        """
        loop = asyncio.get_running_loop()
        try:
            # 1. 提交到线程池，并将 concurrent.futures.Future 包装为 asyncio.Future
            # 这使得主协程可以在此处无缝 await
            logger.debug(f"[Sandbox] 正在执行受限工具调用...")
            future = loop.run_in_executor(self._executor, tool_func, *args, **kwargs)
            
            # 2. 挂起主协程，并设置超时控制
            result = await asyncio.wait_for(future, timeout=timeout)
            logger.debug(f"[Sandbox] 工具调用执行完毕。")
            return result
        except asyncio.TimeoutError:
            logger.error(f"[Sandbox] 🛑 工具调用超时！已强制终止沙盒执行上下文。")
            raise  # 向上传递超时异常，由 WorkerBase 捕获并 ACK/死信
        except Exception as e:
            logger.error(f"[Sandbox] ❌ 工具调用发生内部崩溃: {e}")
            raise  # 向上传递崩溃异常

    def shutdown(self):
        """优雅关闭线程池"""
        self._executor.shutdown(wait=True)
        logger.info("[Sandbox] 线程池 EVU 已优雅关闭。")

class CodeSandbox:
    """
    代码执行沙盒 (EVU - Execution Virtual Unit)
    负责隔离执行动态生成的代码，提供硬性超时熔断和标准输出/错误全量捕获。
    """
    def __init__(self, timeout: float = 3.0):
        self.timeout = timeout

    async def execute(self, code: str) -> Tuple[bool, str]:
        loop = asyncio.get_running_loop()
        
        def _run_code() -> Tuple[bool, str]:
            output_buffer = io.StringIO()
            error_buffer = io.StringIO()
            
            restricted_globals = {
                "__builtins__": __builtins__,
                "print": print,
                "math": __import__("math"),
                "time": __import__("time"),
                "json": __import__("json"),
                "datetime": __import__("datetime")
            }
            
            try:
                with contextlib.redirect_stdout(output_buffer), contextlib.redirect_stderr(error_buffer):
                    exec(code, restricted_globals)
                
                std_out = output_buffer.getvalue()
                std_err = error_buffer.getvalue()
                
                if std_err:
                    return False, f"[标准错误]:\n{std_err}\n[标准输出]:\n{std_out}"
                return True, std_out if std_out else "[无终端输出]"
                
            except Exception as e:
                exc_info = traceback.format_exc()
                return False, f"[运行时错误]:\n{exc_info}"

        try:
            # 使用原生线程池隔离，防止阻塞主事件循环
            is_success, result = await asyncio.wait_for(
                loop.run_in_executor(None, _run_code),
                timeout=self.timeout
            )
            return is_success, result
            
        except asyncio.TimeoutError:
            return False, f"[安全拦截] 代码执行超时 ({self.timeout}s)！已强制熔断。"
        except Exception as e:
            return False, f"[沙盒系统异常] {str(e)}"