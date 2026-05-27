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
    @abstractmethod
    async def execute_tool(
        self, 
        tool_func: Callable, 
        args: Tuple[Any], 
        kwargs: Dict[str, Any], 
        timeout: float = 10.0
    ) -> Any:
        """Execute a tool function with given arguments and timeout control."""
        pass

class ThreadPoolEVU(EVUBase):

    def __init__(self, max_workers: int = 10):
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        logger.info(f"[Sandbox] thread pool EVU started  | Workers: {max_workers}")

    async def execute_tool(
        self, 
        tool_func: Callable, 
        args: Tuple[Any], 
        kwargs: Dict[str, Any], 
        timeout: float = 10.0
    ) -> Any:

        loop = asyncio.get_running_loop()
        try:

            logger.debug(f"[Sandbox] Executing a restricted tool call...")
            future = loop.run_in_executor(self._executor, tool_func, *args, **kwargs)
            result = await asyncio.wait_for(future, timeout=timeout)
            logger.debug(f"[Sandbox] The tool call is executed。")
            return result
        except asyncio.TimeoutError:
            logger.error(f"[Sandbox] 🛑 Tool call timed out!The sandbox execution context has been forcibly terminated。")
            raise 
        except Exception as e:
            logger.error(f"[Sandbox] ❌ Tool call failed with internal error: {e}")
            raise  

    def shutdown(self):

        self._executor.shutdown(wait=True)
        logger.info("[Sandbox] thread pool EVU has been shut down gracefully.")

class CodeSandbox:
    """
    Code Execution sandbox (EVU-Execution Virtual Unit)
    Responsible for isolating the execution of dynamically generated code, providing hard timeout fuses and full capture of standard output/errors.
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
                    return False, f"[Standard error]:\n{std_err}\n[Standard output]:\n{std_out}"
                return True, std_out if std_out else "[No terminal output]"
                
            except Exception as e:
                exc_info = traceback.format_exc()
                return False, f"[Runtime error]:\n{exc_info}"

        try:

            is_success, result = await asyncio.wait_for(
                loop.run_in_executor(None, _run_code),
                timeout=self.timeout
            )
            return is_success, result
            
        except asyncio.TimeoutError:
            return False, f"[Security interception] Code execution timeout ({self.timeout}s)！Forced fuse。"
        except Exception as e:
            return False, f"[Sandbox system exception] {str(e)}"