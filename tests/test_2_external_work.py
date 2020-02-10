import logging
import pytest
from subprocess import CalledProcessError

from asyncjobs import external_work

from conftest import (
    Cancelled,
    setup_scheduler,
    TExternalWorkJob,
    verify_tasks,
)

pytestmark = pytest.mark.asyncio

logger = logging.getLogger(__name__)


@pytest.fixture
def run(scheduler_with_workers):
    Scheduler = scheduler_with_workers(external_work.Scheduler)

    async def _run(todo, **run_args):
        with setup_scheduler(Scheduler, todo) as scheduler:
            return await scheduler.run(**run_args)

    return _run


TJob = TExternalWorkJob


async def test_one_ok_job_in_thread(run):
    todo = [TJob('foo', thread=lambda: 'foo worked')]
    done = await run(todo)
    assert verify_tasks(done, {'foo': 'foo worked'})


async def test_one_ok_job_in_subproc(run, tmp_path):
    path = tmp_path / 'foo'
    todo = [TJob('foo', subproc=['touch', str(path)])]
    done = await run(todo)
    assert verify_tasks(done, {'foo': 0})
    assert path.is_file()


async def test_one_failed_between_two_ok_jobs_in_threads_cancels_last(run):
    def raiseUGH():
        raise ValueError('UGH')

    todo = [
        TJob('foo', before={'bar'}, thread=lambda: 'foo worked'),
        TJob('bar', {'foo'}, before={'baz'}, thread=raiseUGH),
        TJob('baz', {'bar'}, thread=lambda: 'baz worked'),
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
