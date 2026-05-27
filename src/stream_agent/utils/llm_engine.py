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
    通用异步大模型引擎封装。
    默认兼容 vLLM 部署的标准 OpenAI API 协议。
    支持统一的并发限制、超时控制与结构化输出提取。
    """
    def __init__(
        self, 
        api_key: Optional[str] = None, 
        base_url: Optional[str] = None, 
        model_name: Optional[str] = None,
        timeout: float = 15.0
    ):
        # 优先读取传入参数，其次读取环境变量，默认值指向本地 vLLM 服务
        self.api_key = api_key or os.getenv("STREAM_AGENT_API_KEY", "EMPTY")
        self.base_url = base_url or os.getenv("STREAM_AGENT_BASE_URL", "http://192.168.9.133:8001/v1")
        self.model_name = model_name or os.getenv("STREAM_AGENT_MODEL", "Qwen2.5-7B-Instruct")
        self.timeout = timeout
        
        self.client = AsyncOpenAI(
            api_key=self.api_key, 
            base_url=self.base_url,
            timeout=self.timeout
        )
        logger.info(f"🧠 LLM 引擎已初始化 | 模型: {self.model_name} | Endpoint: {self.base_url}")

    async def generate_text(
        self, 
        messages: List[Dict[str, str]], 
        temperature: float = 0.7,
        max_tokens: int = 1024
    ) -> str:
        """
        标准文本生成接口，适用于普通 Agent 聊天与总结。
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
            logger.error(f"LLM 文本生成失败: {str(e)}")
            raise

    async def generate_json(
        self, 
        messages: List[Dict[str, str]], 
        temperature: float = 0.0,  # JSON 提取必须保证低温度以确保确定性
        max_retries: int = 2
    ) -> Dict[str, Any]:
        """
        结构化数据提取接口，专为 Supervisor 路由分发或工具调用设计。
        强制要求大模型返回合法 JSON，并内置解析与兜底重试机制。
        """
        # 强制添加 JSON 模式提示（兼容部分未原生支持 response_format 的模型）
        system_injection = {"role": "system", "content": "你必须且只能输出合法的 JSON 格式数据，绝对不要输出任何其他解释性文本或 Markdown 标记。"}
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
                    response_format={"type": "json_object"} # 触发 vLLM/OpenAI 的强制 JSON 约束
                )
                
                raw_content = response.choices[0].message.content
                
                # 容错清理：剥离大模型可能顽固附带的 ```json 标记
                clean_text = raw_content.replace("```json", "").replace("```", "").strip()
                
                return json.loads(clean_text)
                
            except json.JSONDecodeError as e:
                logger.warning(f"LLM 输出非合法 JSON (尝试 {attempt + 1}/{max_retries}): {raw_content}")
                if attempt == max_retries - 1:
                    logger.error("LLM 强制 JSON 提取彻底失败。")
                    return {}
            except Exception as e:
                logger.error(f"LLM JSON 生成异常: {str(e)}")
                raise
        
    async def generate_stream_to_pubsub(
        self, 
        messages: List[Dict[str, str]], 
        trace_id: str,
        redis_client: Any, # 传入当前的 redis 客户端
        temperature: float = 0.7,
        max_tokens: int = 1024
    ) -> str:
        """
        流式生成接口 (Streaming)。
        大模型每生成一个 Token，就立刻通过 Redis Pub/Sub 推送给网关。
        并最终返回完整的字符串以供 Worker 落盘记忆。
        """
        pubsub_channel = f"channel:stream:{trace_id}"
        full_content = ""
        
        try:
            # 开启 OpenAI 标准的 stream=True 模式
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
                    # 极速推送：将碎片 Token 打入专属频道
                    await redis_client.publish(pubsub_channel, token)
            
            # 生成结束，发送特殊的结束标志符
            await redis_client.publish(pubsub_channel, "[DONE]")
            return full_content
            
        except Exception as e:
            logger.error(f"LLM 流式生成彻底失败: {str(e)}")
            await redis_client.publish(pubsub_channel, f"[ERROR] 生成中断: {str(e)}")
            await redis_client.publish(pubsub_channel, "[DONE]")
            raise