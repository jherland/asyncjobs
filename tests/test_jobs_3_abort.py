from functools import partial
import logging
import pytest

from jobs import ExternalWorkScheduler, SignalHandlingScheduler

from conftest import (
    assert_elapsed_time_within,
    Cancelled,
    setup_and_run_scheduler,
    TExternalWorkJob,
    verify_tasks,
)

logger = logging.getLogger(__name__)


@pytest.fixture(params=[1, 2, 4, 100])
def scheduler_cls(request):
    class MyScheduler(SignalHandlingScheduler, ExternalWorkScheduler):
        pass

    logger.info(f'creating scheduler with {request.param} worker threads')
    yield partial(MyScheduler, workers=request.param)


@pytest.fixture
def run_jobs(scheduler_cls):
    def _run_jobs(todo, **run_args):
        return setup_and_run_scheduler(scheduler_cls, todo, **run_args)

    return _run_jobs


TJob = TExternalWorkJob

# aborting jobs shall properly clean up all jobs + scheduler


def test_abort_one_job_returns_immediately(run_jobs):
    todo = [TJob('foo', async_sleep=0.3)]
    with assert_elapsed_time_within(0.2):
        done = run_jobs(todo, abort_after=0.1)
    assert verify_tasks(done, {'foo': Cancelled})


def test_abort_one_job_in_thread_returns_immediately(run_jobs):
    todo = [TJob('foo', thread_sleep=0.3)]
    with assert_elapsed_time_within(0.2):
        done = run_jobs(todo, abort_after=0.1)
    assert verify_tasks(done, {'foo': Cancelled})


def test_abort_one_job_in_subproc_returns_immediately(run_jobs):
    todo = [TJob('foo', subproc_sleep=30)]
    with assert_elapsed_time_within(0.3):
        done = run_jobs(todo, abort_after=0.1)
    assert verify_tasks(done, {'foo': Cancelled})


def test_abort_one_spawned_job_returns_immediately(run_jobs):
    todo = [
        TJob('foo', spawn=[TJob('bar', async_sleep=0.3)], await_spawn=True)
    ]
    with assert_elapsed_time_within(0.2):
        done = run_jobs(todo, abort_after=0.1)
    assert verify_tasks(done, {'foo': Cancelled, 'bar': Cancelled})


def test_abort_hundred_jobs_returns_immediately(run_jobs):
    todo = [TJob(f'foo #{i}', async_sleep=0.3) for i in range(100)]
    with assert_elapsed_time_within(0.5):
        done = run_jobs(todo, abort_after=0.1)
    assert verify_tasks(done, {f'foo #{i}': Cancelled for i in range(100)})


def test_abort_hundred_jobs_in_threads_returns_immediately(run_jobs):
    todo = [TJob(f'foo #{i}', thread_sleep=0.3) for i in range(100)]
    with assert_elapsed_time_within(0.5):
        done = run_jobs(todo, abort_after=0.1)
    assert verify_tasks(done, {f'foo #{i}': Cancelled for i in range(100)})


def test_abort_hundred_jobs_in_subprocs_returns_immediately(run_jobs):
    todo = [TJob(f'foo #{i}', subproc_sleep=30) for i in range(100)]
    with assert_elapsed_time_within(2.0):
        done = run_jobs(todo, abort_after=0.1)
    assert verify_tasks(done, {f'foo #{i}': Cancelled for i in range(100)})


def test_abort_hundred_spawned_jobs_returns_immediately(run_jobs):
    todo = [
        TJob(
            'foo',
            spawn=[TJob(f'bar #{i}', async_sleep=0.3) for i in range(100)],
            await_spawn=True,
        )
    ]
    with assert_elapsed_time_within(0.5):
        done = run_jobs(todo, abort_after=0.1)
    expect = {f'bar #{i}': Cancelled for i in range(100)}
    expect['foo'] = Cancelled
    assert verify_tasks(done, expect)
