import asyncio
import httpx
from datetime import timedelta

from code_interpreter import CodeInterpreter, SupportedLanguage
from opensandbox import Sandbox
from opensandbox.config import ConnectionConfig
from opensandbox.models import WriteEntry

async def main() -> None:
    config = ConnectionConfig(
        domain="http://localhost:8080",  # Docker Desktop ipport
        use_server_proxy=False,  #  server proxy 
        request_timeout=timedelta(seconds=120), 
        transport=httpx.AsyncHTTPTransport(
            limits=httpx.Limits(max_connections=20)
        ),
    )
    
    print("Creating a sandbox...")
    print(f"configure: {config}")
    
    # 1. Create a sandbox
    sandbox = await Sandbox.create(

        "sandbox-registry.cn-zhangjiakou.cr.aliyuncs.com/opensandbox/code-interpreter:v1.0.2",
        # "opensandbox/code-interpreter:v1.0.2",  # Docker Hub image
        entrypoint=["/opt/opensandbox/code-interpreter.sh"],
        env={"PYTHON_VERSION": "3.11"},
        timeout=timedelta(hours=2), 
        connection_config=config, 
        ready_timeout=timedelta(seconds=120), 
        health_check_polling_interval=timedelta(seconds=5), 
    )
    
    print(f"✓ sanbox create success! ID: {sandbox.id}")
    print(f"Press Enter continue to stop the sandbox...")
    input()

    async with sandbox:

        # 2. Execute a shell command
        execution = await sandbox.commands.run("echo 'Hello OpenSandbox!'")
        print(execution.logs.stdout[0].text)

        # 3. Write a file
        await sandbox.files.write_files([
            WriteEntry(path="/tmp/hello.txt", data="Hello World", mode=644)
        ])

        # 4. Read a file
        content = await sandbox.files.read_file("/tmp/hello.txt")
        print(f"Content: {content}") # Content: Hello World

        # 5. Create a code interpreter
        interpreter = await CodeInterpreter.create(sandbox)

        # 6. excute Python code
        result = await interpreter.codes.run(
              """
                  import sys
                  print(sys.version)
                  result = 2 + 2
                  result
              """,
              language=SupportedLanguage.PYTHON,
        )

        print(result.result[0].text) # 4
        print(result.logs.stdout[0].text) # 3.11.14

    # 7. Cleanup the sandbox
    await sandbox.kill()

if __name__ == "__main__":
    asyncio.run(main())