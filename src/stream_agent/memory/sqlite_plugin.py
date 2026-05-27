import json
import logging
from typing import List, Dict, Any
import aiosqlite

from stream_agent.memory.base import BaseMemory

logger = logging.getLogger("StreamAgent.SQLiteMemory")

class SQLiteMemoryPlugin(BaseMemory):
    """
    基于本地异步 SQLite 实现的 L2 冷持久化存储插件。
    将全量历史写入本地磁盘数据库文件，无惧内存丢失。
    """
    def __init__(self, db_path: str = "stream_agent_memory.db"):
        self.db_path = db_path
        self._initialized = False

    async def _init_db(self):
        """增量安全初始化数据表"""
        if self._initialized:
            return
            
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS agent_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT NOT NULL,
                    role TEXT NOT NULL,
                    content TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_session ON agent_messages(session_id)
            """)
            await db.commit()
            
        self._initialized = True
        logger.info(f"📁 L2 持久化数据库初始化完毕: {self.db_path}")

    async def get_history(self, session_id: str, limit: int = 10) -> List[Dict[str, str]]:
        await self._init_db()
        
        # 逆序捞出最近的记录，再正序排列以符合大模型 Messages 规范
        query = """
            SELECT role, content FROM agent_messages 
            WHERE session_id = ? 
            ORDER BY id DESC LIMIT ?
        """
        try:
            async with aiosqlite.connect(self.db_path) as db:
                async with db.execute(query, (session_id, limit)) as cursor:
                    rows = await cursor.fetchall()
                    
                    history = []
                    for row in rows:
                        history.append({"role": row[0], "content": row[1]})
                        
                    # 逆序翻转，保证时间轴是从旧到新
                    history.reverse()
                    return history
        except Exception as e:
            logger.error(f"读取 L2 SQLite 异常: {e}")
            return []

    async def save_message(self, session_id: str, role: str, content: str) -> None:
        await self._init_db()
        
        query = "INSERT INTO agent_messages (session_id, role, content) VALUES (?, ?, ?)"
        try:
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute(query, (session_id, role, content))
                await db.commit()
                logger.debug(f"[L2 SQLite] 💾 全量日志异步刷盘成功 [Session: {session_id}]")
        except Exception as e:
            logger.error(f"写入 L2 SQLite 异常: {e}")

    async def clear(self, session_id: str) -> None:
        await self._init_db()
        query = "DELETE FROM agent_messages WHERE session_id = ?"
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(query, (session_id,))
            await db.commit()