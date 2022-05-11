#!/usr/bin/env python3
import platform
import asyncio,aiofiles,threading
import functools

if platform.system()=='Windows':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

safe_gather_limit = 50

def async_wrap(f):
    @functools.wraps(f)
    async def run(*args, loop=None, executor=None, **kwargs):
        if loop is None:
            loop = asyncio.get_event_loop()
        p = functools.partial(f, *args, **kwargs)
        return await loop.run_in_executor(executor, p)
    return run

async def safe_gather(tasks,n=safe_gather_limit,semaphore=None):
    semaphore = semaphore if semaphore else asyncio.Semaphore(n)

    async def sem_task(task):
        async with semaphore:
            return await task
    return await asyncio.gather(*(sem_task(task) for task in tasks))
