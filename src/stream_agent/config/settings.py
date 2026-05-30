from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional

class Settings(BaseSettings):

    PROJECT_NAME: str = "StreamAgent"
    DEBUG_MODE: bool = False
    

    REDIS_URL: str = "redis://localhost:6379/0"
    

    GATEWAY_HOST: str = "0.0.0.0"
    GATEWAY_PORT: int = 8000

    DASHSCOPE_API_KEY: str 
    OPENAI_API_KEY: Optional[str] = None
    OPENAI_BASE_URL: Optional[str] = None

    STREAM_AGENT_API_KEY: str = None
    STREAM_AGENT_BASE_URL: str = None
    STREAM_AGENT_MODEL: str = None


    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,  
        extra="ignore"  
    )

settings = Settings()