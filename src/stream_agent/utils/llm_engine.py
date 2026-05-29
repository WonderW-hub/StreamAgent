import os
import json
import logging
import asyncio
from typing import Dict, Any, Optional, List
from openai import AsyncOpenAI
from openai.types.chat import ChatCompletion

logger = logging.getLogger("StreamAgent.LLMEngine")

class AsyncLLMEngine:
    """
    LLM Engine Wrapper
    Encapsulates interactions with the underlying LLM API (e.g., OpenAI, vLL
    """
    def __init__(
        self, 
        api_key: Optional[str] = None, 
        base_url: Optional[str] = None, 
        model_name: Optional[str] = None,
        timeout: float = 15.0
    ):
        self.api_key = api_key or os.getenv("STREAM_AGENT_API_KEY", "EMPTY")
        self.base_url = base_url or os.getenv("STREAM_AGENT_BASE_URL", "http://192.168.9.133:8001/v1")
        self.model_name = model_name or os.getenv("STREAM_AGENT_MODEL", "Qwen2.5-7B-Instruct")
        self.timeout = timeout
        
        self.client = AsyncOpenAI(
            api_key=self.api_key, 
            base_url=self.base_url,
            timeout=self.timeout
        )
        logger.info(f"🧠 LLM Engine initialized | Model: {self.model_name} | Endpoint: {self.base_url}")

    async def generate_text(
        self, 
        messages: List[Dict[str, str]], 
        temperature: float = 0.7,
        max_tokens: int = 1024
    ) -> str:
        """
        Standard text generation interface, suitable for general Agent chat and summarization.
        """
        try:
            response: ChatCompletion = await self.client.chat.completions.create(
                model=self.model_name,
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens
            )
            return response.choices[0].message.content
        except Exception as e:
            logger.error(f"LLM Text Generation Failed: {str(e)}")
            raise

    async def generate_json(
        self, 
        messages: List[Dict[str, str]], 
        temperature: float = 0.0, 
        max_retries: int = 2
    ) -> Dict[str, Any]:
        """
        Structured data extraction interface, designed for Supervisor routing or tool invocation.
        Force the large model to return valid JSON and include built-in parsing and fallback retry mechanisms.
        """
        # Force add JSON mode prompt (compatible with some models that don't natively support response_format)
        system_injection = {"role": "system", "content": "You must and can only output data in legal JSON format, and never output any other explanatory text or Markdown tags.。"}
        if messages and messages[0].get("role") == "system":
            messages[0]["content"] += "\n" + system_injection["content"]
        else:
            messages.insert(0, system_injection)

        for attempt in range(max_retries):
            try:
                response = await self.client.chat.completions.create(
                    model=self.model_name,
                    messages=messages,
                    temperature=temperature,
                    response_format={"type": "json_object"}
                )
                
                raw_content = response.choices[0].message.content
                clean_text = raw_content.replace("```json", "").replace("```", "").strip()
                
                return json.loads(clean_text)
                
            except json.JSONDecodeError as e:
                logger.warning(f"LLM Text Generation Failed: {str(e)}")
                if attempt == max_retries - 1:
                    logger.error("LLM Force JSON Extraction Failed.")
                    return {}
            except Exception as e:
                logger.error(f"LLM JSON Generation Exception: {str(e)}")
                raise

        async def chat_with_tools(
            self, 
            messages: List[Dict[str, Any]], 
            tools: List[Dict[str, Any]], 
            temperature: float = 0.1
        ) -> Any:
            """
            支持原生 Tool Calling 的交互接口（返回完整的 message 对象，包含 tool_calls 详情）
            """
            try:
                response = await self.client.chat.completions.create(
                    model=self.model_name,
                    messages=messages,
                    temperature=temperature,
                    tools=tools,
                    tool_choice="auto"  # 让大模型自主决定是否需要调用工具
                )
                return response.choices[0].message
            except Exception as e:
                logger.error(f"LLM 工具调用生成失败: {str(e)}")
                raise
        
    async def generate_stream_to_pubsub(
        self, 
        messages: List[Dict[str, str]], 
        trace_id: str,
        redis_client: Any, 
        temperature: float = 0.7,
        max_tokens: int = 1024
    ) -> str:
        """
        Streaming generation interface (Streaming).
        Every time the big model generates a token, it is immediately pushed to the gateway through Redis Pub/Sub.
        And finally return the complete string for the worker to remember.
        """
        pubsub_channel = f"channel:stream:{trace_id}"
        full_content = ""
        
        try:

            stream = await self.client.chat.completions.create(
                model=self.model_name,
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens,
                stream=True
            )
            
            async for chunk in stream:
                if chunk.choices and chunk.choices[0].delta.content:
                    token = chunk.choices[0].delta.content
                    full_content += token
                    await redis_client.publish(pubsub_channel, token)
            
            await redis_client.publish(pubsub_channel, "[DONE]")
            return full_content
            
        except Exception as e:
            logger.error(f"LLM Streaming Generation Failed: {str(e)}")
            await redis_client.publish(pubsub_channel, f"[ERROR] Generation Interrupted: {str(e)}")
            await redis_client.publish(pubsub_channel, "[DONE]")
            raise