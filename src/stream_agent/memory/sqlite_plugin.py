# \src\stream_agent\memory\sqlite_plugin.py
import json
import logging
from typing import List, Dict, Any
import aiosqlite

from stream_agent.memory.base import BaseMemory

logger = logging.getLogger("StreamAgent.SQLiteMemory")

class SQLiteMemoryPlugin(BaseMemory):
    """
    An asynchronous SQLite-based L2 cold persistence storage plugin.
    Writes all history to a local disk database file, immune to memory loss.
    """
    def __init__(self, db_path: str = "stream_agent_memory.db"):
        self.db_path = db_path
        self._initialized = False

    async def _init_db(self):
        """Incremental safe initialization of the data table"""
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
        logger.info(f"📁 L2  The persistent database is initialized: {self.db_path}")

    async def get_history(self, session_id: str, limit: int = 10) -> List[Dict[str, str]]:
        await self._init_db()
        
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

                    history.reverse()
                    return history
        except Exception as e:
            logger.error(f"Reading L2 SQLite failed: {e}")
            return []

    async def save_message(self, session_id: str, role: str, content: str) -> None:
        await self._init_db()
        
        query = "INSERT INTO agent_messages (session_id, role, content) VALUES (?, ?, ?)"
        try:
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute(query, (session_id, role, content))
                await db.commit()
                logger.debug(f"[L2 SQLite] 💾 All logs asynchronously flushed to disk successfully [Session: {session_id}]")
        except Exception as e:
            logger.error(f"Writing to L2 SQLite failed: {e}")

    async def clear(self, session_id: str) -> None:
        await self._init_db()
        query = "DELETE FROM agent_messages WHERE session_id = ?"
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(query, (session_id,))
            await db.commit()