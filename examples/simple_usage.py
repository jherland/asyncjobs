#!/usr/bin/env python3
# Run three simple jobs in sequence
import asyncio
from asyncjobs import Scheduler
import time


def sleep():  # Run in a worker thread by job #2 below
    print(f'{time.ctime()}: Sleep for a second')
    time.sleep(1)
    print(f'{time.ctime()}: Finished sleep')


s = Scheduler()

# Job #1 prints uptime
s.add_subprocess_job('#1', ['uptime'])

# Job #2 waits for #1 and then sleeps in a thread
s.add_thread_job('#2', sleep, deps={'#1'})

# Job #3 waits for #2 and then prints uptime (again)
s.add_subprocess_job('#3', ['uptime'], deps={'#2'})

asyncio.run(s.run())
