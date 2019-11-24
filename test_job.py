import asyncio
from scheduler import Job, Scheduler


class MyJob(Job):
    async def __call__(self, scheduler):
        return self.name + ' done'


def test_one_job():
    scheduler = Scheduler(None)
    j = MyJob('foo')
    scheduler.add(j)
    tasks = asyncio.run(scheduler.run())
    results = {k: v.result() for k, v in tasks.items()}
    assert results == {'foo': 'foo done'}
