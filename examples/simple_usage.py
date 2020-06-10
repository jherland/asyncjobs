#!/usr/bin/env python3
# Run three simple jobs in sequence
import asyncio
from asyncjobs import Scheduler


# Helper functions


def sleep():
    import time

    print(f'{time.ctime()}: Sleep for a second')
    time.sleep(1)
    print(f'{time.ctime()}: Finished sleep')


async def sleep_job(ctx):
    return await ctx.call_in_thread(sleep)


async def uptime(ctx):
    return await ctx.run_in_subprocess(['uptime'])


s = Scheduler()

# Job #1 prints uptime
s.add_job('#1', uptime)

# Job #2 waits for #1 and then sleeps in a thread
s.add_job('#2', sleep_job, deps={'#1'})

# Job #3 waits for #2 and then prints uptime (again)
s.add_job('#3', uptime, deps={'#2'})

asyncio.run(s.run())
