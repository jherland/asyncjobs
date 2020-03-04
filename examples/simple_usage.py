#!/usr/bin/env python3
# Run three simple jobs in sequence
import asyncio
from asyncjobs import Job, Scheduler


# Helper function
def sleep():
    import time

    print(f'{time.ctime()}: Sleep for a second')
    time.sleep(1)
    print(f'{time.ctime()}: Finished sleep')


# Job #1 prints uptime
job1 = Job('#1')
job1.subprocess_argv = ['uptime']

# Job #2 waits for #1 and then sleeps in a thread
job2 = Job('#2', deps={'#1'})
job2.thread_func = sleep

# Job #3 waits for #2 and then prints uptime (again)
job3 = Job('#3', deps={'#2'})
job3.subprocess_argv = ['uptime']

# Run all jobs in the scheduler
s = Scheduler()
for job in [job1, job2, job3]:
    s.add(job)
asyncio.run(s.run())
