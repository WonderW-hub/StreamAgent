from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional

class Settings(BaseSettings):
    # --- 基础配置 ---
    PROJECT_NAME: str = "StreamAgent"
    DEBUG_MODE: bool = False
    
    # --- Redis 总线 ---
    REDIS_URL: str = "redis://localhost:6379/0"
    
    # --- 网关网络 ---
    GATEWAY_HOST: str = "0.0.0.0"
    GATEWAY_PORT: int = 8000
    
    # --- 大模型与多模态 API Keys ---
    # 定义为可选或必填。如果没有默认值，且 .env 里没配，程序启动就会报错
    DASHSCOPE_API_KEY: str 
    OPENAI_API_KEY: Optional[str] = None
    OPENAI_BASE_URL: Optional[str] = None

    # 魔法配置：告诉 Pydantic 去哪里找环境变量文件
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,  # 区分大小写
        extra="ignore"        # 忽略 .env 中未在类里定义的额外配置
    )

# 🌟 实例化为一个单例，整个项目都引这个 `settings` 对象
settings = Settings()