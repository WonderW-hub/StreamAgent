# run_planner.py
import asyncio
import logging
from stream_agent.agents.planner import PlannerAgent

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

async def main():
    agent = PlannerAgent()
    logging.info(f"🚀 {agent.agent_name} Starting, listening to the Redis bus...")
    await agent.start(is_shadow=False)

if __name__ == "__main__":
    asyncio.run(main())