import pytest
from subprocess import PIPE

from asyncjobs import external_work, signal_handling

from conftest import (
    abort_in,
    assert_elapsed_time,
    Cancelled,
    mock_argv,
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
    with assert_elapsed_time(lambda t: t < 0.2):
        done = await run(todo, abort_after=0.3)
    assert verify_tasks(done, {'foo': 'foo done'})


async def test_abort_one_job_returns_immediately(run):
    todo = [TJob('foo', async_sleep=0.3)]
    with assert_elapsed_time(lambda t: t < 0.2):
        done = await run(todo, abort_after=0.1)
    assert verify_tasks(done, {'foo': Cancelled})


async def test_abort_one_job_in_thread_cannot_return_immediately(run):
    todo = [TJob('foo', thread_sleep=0.2)]
    with assert_elapsed_time(lambda t: t > 0.2):  # must wait for thread
        done = await run(todo, abort_after=0.1)
    assert verify_tasks(done, {'foo': Cancelled})


async def test_abort_one_job_in_subproc_returns_immediately(run):
    todo = [TJob('foo', subproc_sleep=30)]
    with assert_elapsed_time(lambda t: t < 0.3):
        done = await run(todo, abort_after=0.1)
    assert verify_tasks(done, {'foo': Cancelled})


async def test_abort_one_spawned_job_returns_immediately(run):
    todo = [
        TJob('foo', spawn=[TJob('bar', async_sleep=0.3)], await_spawn=True)
    ]
    with assert_elapsed_time(lambda t: t < 0.2):
        done = await run(todo, abort_after=0.1)
    assert verify_tasks(done, {'foo': Cancelled, 'bar': Cancelled})


async def test_abort_one_non_terminating_job_teminates_then_kills(run):
    argv = mock_argv('ignore:SIGTERM', 'ignore:SIGINT', 'FOO', 'sleep:30')

    async def coro(tjob, ctx):
        with tjob.subprocess_xevents(argv, result='kill'):
            async with ctx.subprocess(
                argv, stdout=PIPE, kill_delay=0.1
            ) as proc:
                assert b'FOO\n' == await proc.stdout.readline()
                # Subprocess will now ignore SIGTERM when we are cancelled
                with abort_in(0.1):
                    await proc.wait()

    todo = [TJob('foo', subproc=coro)]
    with assert_elapsed_time(lambda t: t < 0.5):
        done = await run(todo)
    assert verify_tasks(done, {'foo': Cancelled})
    # assert False


async def test_abort_job_with_two_subprocs_terminates_both(run, num_workers):
    if num_workers < 2:
        pytest.skip('need Scheduler with at least 2 workers')

    argv = mock_argv('sleep:30')

    async def coro(tjob, ctx):
        with tjob.subprocess_xevents(argv, result='terminate'):
            async with ctx.subprocess(argv) as proc1:
                with tjob.subprocess_xevents(argv, result='terminate'):
                    async with ctx.subprocess(argv) as proc2:
                        await proc2.wait()
                await proc1.wait()

    todo = [TJob('foo', subproc=coro)]
    with assert_elapsed_time(lambda t: t < 0.5):
        done = await run(todo, abort_after=0.2)
    assert verify_tasks(done, {'foo': Cancelled})


async def test_abort_job_with_two_non_terminating_kills_both(run, num_workers):
    if num_workers < 2:
        pytest.skip('need Scheduler with at least 2 workers')

    argv1 = mock_argv('ignore:SIGTERM', 'ignore:SIGINT', 'FOO', 'sleep:30')
    argv2 = mock_argv('ignore:SIGTERM', 'ignore:SIGINT', 'BAR', 'sleep:30')

    async def coro(tjob, ctx):
        with tjob.subprocess_xevents(argv1, result='kill'):
            async with ctx.subprocess(
                argv1, stdout=PIPE, kill_delay=0.1
            ) as proc1:
                assert b'FOO\n' == await proc1.stdout.readline()
                with tjob.subprocess_xevents(argv2, result='kill'):
                    async with ctx.subprocess(
                        argv2, stdout=PIPE, kill_delay=0.1
                    ) as proc2:
                        assert b'BAR\n' == await proc2.stdout.readline()
                        # Both subprocesses will now ignore SIGTERM and we can
                        # proceed with cancelling.
                        with abort_in(0.1):
                            await proc2.wait()
                await proc1.wait()

    todo = [TJob('foo', subproc=coro)]
    with assert_elapsed_time(lambda t: t < 1.0):
        done = await run(todo)
    assert verify_tasks(done, {'foo': Cancelled})


async def test_abort_hundred_jobs_returns_immediately(run):
    todo = [TJob(f'foo #{i}', async_sleep=0.5) for i in range(100)]
    with assert_elapsed_time(lambda t: t < 0.3):
        done = await run(todo, abort_after=0.1)
    assert verify_tasks(done, {f'foo #{i}': Cancelled for i in range(100)})


async def test_abort_hundred_jobs_in_threads_cannot_return_immediately(run):
    todo = [TJob(f'foo #{i}', thread_sleep=0.3) for i in range(100)]
    with assert_elapsed_time(lambda t: t > 0.3):  # must wait for all threads
        done = await run(todo, abort_after=0.1)
    assert verify_tasks(done, {f'foo #{i}': Cancelled for i in range(100)})


async def test_abort_hundred_jobs_in_subprocs_returns_immediately(run):
    todo = [TJob(f'foo #{i}', subproc_sleep=5.0) for i in range(100)]
    with assert_elapsed_time(lambda t: t < 2.0):
        done = await run(todo, abort_after=0.1)
    assert verify_tasks(done, {f'foo #{i}': Cancelled for i in range(100)})


async def test_abort_hundred_spawned_jobs_returns_immediately(run):
    todo = [
        TJob(
            'foo',
            spawn=[TJob(f'bar #{i}', async_sleep=0.5) for i in range(100)],
            await_spawn=True,
        )
    ]
    with assert_elapsed_time(lambda t: t < 0.3):
        done = await run(todo, abort_after=0.1)
    expect = {f'bar #{i}': Cancelled for i in range(100)}
    expect['foo'] = Cancelled
    assert verify_tasks(done, expect)
