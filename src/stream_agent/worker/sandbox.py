import sys
import asyncio
import logging
import concurrent.futures
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, Optional, Tuple
import io
import traceback
import contextlib
import subprocess
import tempfile
import os
import httpx
from datetime import timedelta
import uuid
# Import OpenSandbox SDKs
from opensandbox import (
    SandboxPoolAsync, 
    PoolCreationSpec, 
    AcquirePolicy, 
    InMemoryAsyncPoolStateStore,
    Sandbox
)
from opensandbox.config import ConnectionConfig
from opensandbox.models import WriteEntry
from code_interpreter import CodeInterpreter


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
        logger.info(f"[Sandbox] Thread pool EVU started | Workers: {max_workers}")

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
            logger.debug(f"[Sandbox] Tool call executed successfully.")
            return result
        except asyncio.TimeoutError:
            logger.error(f"[Sandbox] 🛑 Tool call timed out! Sandbox execution context forcibly terminated.")
            raise 
        except Exception as e:
            logger.error(f"[Sandbox] ❌ Tool call failed with internal error: {e}")
            raise  

    def shutdown(self):
        self._executor.shutdown(wait=True)
        logger.info("[Sandbox] Thread pool EVU has been shut down gracefully.")

class CodeSandbox:
    def __init__(self, endpoint: str = "http://localhost:8080", pool_size: int = 3):
        self.endpoint = endpoint
        self.config = ConnectionConfig(
            domain=self.endpoint,
            use_server_proxy=False,
            request_timeout=timedelta(seconds=120),
            transport=httpx.AsyncHTTPTransport(
                limits=httpx.Limits(max_connections=20)
            ),
        )
        self.image = "sandbox-registry.cn-zhangjiakou.cr.aliyuncs.com/opensandbox/code-interpreter:v1.0.2"
        
        self._pool = SandboxPoolAsync(
            pool_name=f"coder-pool-{uuid.uuid4().hex[:6]}",
            owner_id="stream-agent-worker",
            max_idle=pool_size,
            state_store=InMemoryAsyncPoolStateStore(), 
            connection_config=self.config,
            creation_spec=PoolCreationSpec(
                image=self.image,
                entrypoint=["/opt/opensandbox/code-interpreter.sh"],
                env={"PYTHON_VERSION": "3.11"},
            )
        )
        self._pool_started = False

    async def start(self):
        if not self._pool_started:
            logger.info("🔥 Warming up the OpenSandbox container pool...")
            await self._pool.start()
            self._pool_started = True

    async def stop(self):
        if self._pool_started:
            logger.info("🛑 Gracefully closing the OpenSandbox container pool...")
            await self._pool.shutdown(graceful=True)
            self._pool_started = False
    
    @staticmethod
    async def destroy_sandbox(sandbox_id: str, endpoint: str = "http://localhost:8080"):
        try:
            logger.info(f"🧹 Cleaning up the remaining sandboxes globally [ID: {sandbox_id}]...")
            config = ConnectionConfig(
                domain=endpoint,
                use_server_proxy=False,
                request_timeout=timedelta(seconds=10), 
                transport=httpx.AsyncHTTPTransport(limits=httpx.Limits(max_connections=20)),
            )
            sandbox = await Sandbox.connect(sandbox_id, connection_config=config)
            await sandbox.kill()
            await sandbox.close()
            logger.info(f"✅ Successfully recovered the legacy sandbox [ID: {sandbox_id}]")
        except Exception as e:
            logger.warning(f"⚠️ Unable to clean up the sandbox [ID: {sandbox_id}] (may have been automatically destroyed by other nodes): {e}")

    async def execute(
        self, 
        code: str, 
        redis_client, 
        trace_id: str, 
        is_final_step: bool = True, 
        files_to_mount: Optional[Dict[str, str]] = None,
        session_id: Optional[str] = None 
    ) -> Tuple[bool, str]:
        if not self._pool_started:
            await self.start()

   
        bind_id = session_id if session_id else trace_id
        sandbox_key = f"session_sandbox:{bind_id}"
        
        sandbox_id = await redis_client.get(sandbox_key)
        
        sandbox = None
        execution_success = False

        try:
            if sandbox_id:
                logger.info(f"🔄 The session cache is detected and an attempt is being made to reconnect to the dedicated sandbox [ID: {sandbox_id}]")
                try:
                    sandbox = await Sandbox.connect(sandbox_id, connection_config=self.config)
                    # The connection is successful and the renewal period is 1 hour
                    await redis_client.expire(sandbox_key, 3600)
                except Exception as e:
                    logger.warning(f"⚠️ The sandbox that cannot be connected to the cache may have been recycled by the bottom of the system.Prepare to apply for a new sandbox... (Abnormal: {e})")
                    sandbox = None 
                    await redis_client.delete(sandbox_key) 

            # If there is no sandbox_id, or the above connect fails and the sandbox is still None, then apply for a new sandbox
            if not sandbox:
                logger.info("⚡ Requesting allocation of a new hot container...")
                
                for attempt in range(60):
                    try:
                        sandbox = await self._pool.acquire(
                            sandbox_timeout=timedelta(minutes=30),
                            policy=AcquirePolicy.FAIL_FAST,
                        )
                        break
                    except Exception as e:
                        if "idle buffer empty" in str(e) or "PoolEmptyException" in str(type(e)):
                            if attempt % 5 == 0:
                                logger.info(f"⏳ The container pool is排队... ({attempt}s/60s)")
                            await asyncio.sleep(1)
                        else:
                            raise e

                if not sandbox:
                    return False, "[Sandbox Timeout] The container pool is full, please check the service load."

                await redis_client.set(sandbox_key, sandbox.id, ex=3600)
                logger.info(f"🚀 Successfully acquired sandbox [ID: {sandbox.id}], bound to Session: {bind_id}")

            if files_to_mount:
                write_entries = [
                    WriteEntry(path=f"/workspace/{filename}", data=content, mode=644)
                    for filename, content in files_to_mount.items()
                ]
                await sandbox.files.write_files(write_entries)

            interpreter = await CodeInterpreter.create(sandbox)
            execution = await interpreter.codes.run(code)
            
            if execution.error:
                error_trace = f"[{execution.error.name}]: {execution.error.value}\n{execution.error.traceback}"
                logger.error(f"❌ Code execution failed: {execution.error.name}")
                return False, f"[Standard Error]:\n{error_trace}\n[Standard Output]:\n{execution.text}"
            else:
                output = execution.text if execution.text else "[No terminal output]"
                logger.info("🎉 Code execution successful!")
                execution_success = True
                return True, output

        except Exception as e:
            logger.error(f"🚨 OpenSandbox Abnormal infrastructure: {e}", exc_info=True)
            return False, f"[Abnormal sandbox infrastructure] {str(e)}"

        finally:
            if sandbox:
                # 【Modification】 Life cycle control core logic
                if session_id:
                    # If it is a session-based sandbox, don't kill it, and keep the environment for the next round of dialogue.
                    logger.info(f"⏸️  Session mode, sandbox [ID: {sandbox.id}] will remain running for the next use...")
                    await sandbox.close() 
                else:
                    if is_final_step or not execution_success:
                        logger.info(f"🛑  Task completed or interrupted,destroying sandbox [ID: {sandbox.id}]...")
                        try:
                            await sandbox.kill()
                            await redis_client.delete(sandbox_key)
                        except Exception as e:
                            logger.warning(f"Warning of sandbox destruction: {e}")
                        finally:
                            await sandbox.close()
                    else:
                        logger.info(f"⏸️  Task not completed, sandbox [ID: {sandbox.id}] will remain running...")
                        await sandbox.close()