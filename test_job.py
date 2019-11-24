import asyncio
from scheduler import JobWithDeps, Scheduler


class MyJob(JobWithDeps):
    def __init__(self, name, deps=None, before=None):
        self.before = set() if before is None else before
        if deps is None:
            deps = set()
        super().__init__(name=name, deps=deps)

    async def __call__(self, scheduler):
        await super().__call__(scheduler)
        for b in self.before:
            assert b in scheduler.results  # The other job has been started
            assert not scheduler.results[b].done()  # but is not yet finished
        return self.name + ' done'


def run_jobs_and_check_results(*jobs):
    scheduler = Scheduler(None)
    for job in jobs:
        scheduler.add(job)
    tasks = asyncio.run(scheduler.run())
    results = {k: v.result() for k, v in tasks.items()}
    assert results == {job.name: job.name + ' done' for job in jobs}


def test_one_job():
    run_jobs_and_check_results(MyJob('foo'))


def test_two_independent_jobs():
    run_jobs_and_check_results(MyJob('foo'), MyJob('bar'))


def test_two_dependent_jobs():
    run_jobs_and_check_results(
        MyJob('foo', before={'bar'}), MyJob('bar', {'foo'}),
    )
