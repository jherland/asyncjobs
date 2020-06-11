import pytest

from asyncjobs import external_work, signal_handling

from conftest import (
    abort_in,
    assert_elapsed_time_within,
    Cancelled,
    TExternalWorkJob,
    verified_events,
    verify_tasks,
)

pytestmark = pytest.mark.asyncio

TJob = TExternalWorkJob


@pytest.fixture
def Scheduler(scheduler_with_workers):
    return scheduler_with_workers(
        signal_handling.Scheduler, external_work.Scheduler
    )


@pytest.fixture
def run(Scheduler):
    async def _run(todo, abort_after=None):
        scheduler = Scheduler()
        with verified_events(scheduler, todo):
            for job in todo:
                scheduler.add_job(job.name, job, getattr(job, 'deps', None))
            with abort_in(abort_after):
                return await scheduler.run()

    return _run


# aborting jobs shall properly clean up all jobs + scheduler


async def test_return_before_abort(run):
    todo = [TJob('foo', async_sleep=0.1)]
    with assert_elapsed_time_within(0.2):
        done = await run(todo, abort_after=0.3)
    assert verify_tasks(done, {'foo': 'foo done'})


async def test_abort_one_job_returns_immediately(run):
    todo = [TJob('foo', async_sleep=0.3)]
    with assert_elapsed_time_within(0.2):
        done = await run(todo, abort_after=0.1)
    assert verify_tasks(done, {'foo': Cancelled})


async def test_abort_one_job_in_thread_returns_immediately(run):
    todo = [TJob('foo', thread_sleep=0.3)]
    with assert_elapsed_time_within(0.2):
        done = await run(todo, abort_after=0.1)
    assert verify_tasks(done, {'foo': Cancelled})


async def test_abort_one_job_in_subproc_returns_immediately(run):
    todo = [TJob('foo', subproc_sleep=30)]
    with assert_elapsed_time_within(0.3):
        done = await run(todo, abort_after=0.1)
    assert verify_tasks(done, {'foo': Cancelled})


async def test_abort_one_spawned_job_returns_immediately(run):
    todo = [
        TJob('foo', spawn=[TJob('bar', async_sleep=0.3)], await_spawn=True)
    ]
    with assert_elapsed_time_within(0.2):
        done = await run(todo, abort_after=0.1)
    assert verify_tasks(done, {'foo': Cancelled, 'bar': Cancelled})


async def test_abort_hundred_jobs_returns_immediately(run):
    todo = [TJob(f'foo #{i}', async_sleep=0.3) for i in range(100)]
    with assert_elapsed_time_within(0.2):
        done = await run(todo, abort_after=0.1)
    assert verify_tasks(done, {f'foo #{i}': Cancelled for i in range(100)})


async def test_abort_hundred_jobs_in_threads_returns_immediately(run):
    todo = [TJob(f'foo #{i}', thread_sleep=1.0) for i in range(100)]
    with assert_elapsed_time_within(0.5):
        done = await run(todo, abort_after=0.1)
    assert verify_tasks(done, {f'foo #{i}': Cancelled for i in range(100)})


async def test_abort_hundred_jobs_in_subprocs_returns_immediately(run):
    todo = [TJob(f'foo #{i}', subproc_sleep=5.0) for i in range(100)]
    with assert_elapsed_time_within(2.0):
        done = await run(todo, abort_after=0.1)
    assert verify_tasks(done, {f'foo #{i}': Cancelled for i in range(100)})


async def test_abort_hundred_spawned_jobs_returns_immediately(run):
    todo = [
        TJob(
            'foo',
            spawn=[TJob(f'bar #{i}', async_sleep=0.3) for i in range(100)],
            await_spawn=True,
        )
    ]
    with assert_elapsed_time_within(0.2):
        done = await run(todo, abort_after=0.1)
    expect = {f'bar #{i}': Cancelled for i in range(100)}
    expect['foo'] = Cancelled
    assert verify_tasks(done, expect)
