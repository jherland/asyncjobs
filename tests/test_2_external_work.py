import pytest
from subprocess import CalledProcessError

from asyncjobs import external_work

from conftest import (
    Cancelled,
    TExternalWorkJob,
    verified_events,
    verify_tasks,
)

pytestmark = pytest.mark.asyncio

TJob = TExternalWorkJob


@pytest.fixture
def Scheduler(scheduler_with_workers):
    return scheduler_with_workers(external_work.Scheduler)


@pytest.fixture
def run(Scheduler):
    async def _run(todo):
        scheduler = Scheduler()
        with verified_events(scheduler, todo):
            for job in todo:
                scheduler.add_job(job.name, job, getattr(job, 'deps', None))
            return await scheduler.run()

    return _run


# simple scenarios with threads/subprocesses


async def test_one_ok_job_in_thread(run):
    todo = [TJob('foo', thread=lambda ctx: 'foo worked')]
    done = await run(todo)
    assert verify_tasks(done, {'foo': 'foo worked'})


async def test_one_ok_job_in_subproc(run, tmp_path):
    path = tmp_path / 'foo'
    todo = [TJob('foo', subproc=['touch', str(path)])]
    done = await run(todo)
    assert verify_tasks(done, {'foo': 0})
    assert path.is_file()


async def test_one_failed_between_two_ok_jobs_in_threads_cancels_last(run):
    def raiseUGH(ctx):
        raise ValueError('UGH')

    todo = [
        TJob('foo', before={'bar'}, thread=lambda ctx: 'foo worked'),
        TJob('bar', {'foo'}, before={'baz'}, thread=raiseUGH),
        TJob('baz', {'bar'}, thread=lambda ctx: 'baz worked'),
    ]
    done = await run(todo)
    assert verify_tasks(
        done, {'foo': 'foo worked', 'bar': ValueError('UGH'), 'baz': Cancelled}
    )


async def test_one_failed_between_two_in_subprocs_cancels_last(run, tmp_path):
    foo_path = tmp_path / 'foo'
    baz_path = tmp_path / 'baz'
    todo = [
        TJob('foo', before={'bar'}, subproc=['touch', str(foo_path)]),
        TJob('bar', {'foo'}, before={'baz'}, subproc=['false']),
        TJob('baz', {'bar'}, subproc=['touch', str(baz_path)]),
    ]
    done = await run(todo)
    assert verify_tasks(
        done,
        {'foo': 0, 'bar': CalledProcessError(1, ['false']), 'baz': Cancelled},
    )
    assert foo_path.is_file()
    assert not baz_path.exists()
