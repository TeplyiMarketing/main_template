import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from logs.logging import logger

logger.info('Scheduled task run')
scheduler = AsyncIOScheduler()


async def run_tokens():
    await asyncio.create_subprocess_exec('python3', '/your_client_name/tokens.py')


async def run_main():
    await asyncio.create_subprocess_exec('python3', '/your_client_name/main.py')


scheduler.add_job(run_tokens, 'cron', hour=11, minute=0)
scheduler.add_job(run_tokens, 'cron', hour=0, minute=50)
scheduler.add_job(run_main, 'cron', hour=1, minute=0)

scheduler.start()
asyncio.get_event_loop().run_forever()

