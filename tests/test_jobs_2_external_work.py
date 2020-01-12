from functools import partial
import logging
import pytest
from subprocess import CalledProcessError

from jobs import ExternalWorkScheduler

from conftest import (
    Cancelled,
    setup_and_run_scheduler,
    TExternalWorkJob,
    verify_tasks,
)

logger = logging.getLogger(__name__)


@pytest.fixture(params=[1, 2, 4, 100])
def scheduler_cls(request):
    logger.info(f'creating scheduler with {request.param} worker threads')
    yield partial(ExternalWorkScheduler, workers=request.param)


@pytest.fixture
def run_jobs(scheduler_cls):
    def _run_jobs(todo, **run_args):
        return setup_and_run_scheduler(scheduler_cls, todo, **run_args)

    return _run_jobs


TJob = TExternalWorkJob


def test_one_ok_job_in_thread(run_jobs):
    todo = [TJob('foo', thread=lambda: 'foo worked')]
    done = run_jobs(todo)
    assert verify_tasks(done, {'foo': 'foo worked'})


def test_one_ok_job_in_subproc(run_jobs, tmp_path):
    path = tmp_path / 'foo'
    todo = [TJob('foo', subproc=['touch', str(path)])]
    done = run_jobs(todo)
    assert verify_tasks(done, {'foo': 0})
    assert path.is_file()


def test_one_failed_between_two_ok_jobs_in_threads_cancels_last(run_jobs):
    def raiseUGH():
        raise ValueError('UGH')

    todo = [
        TJob('foo', before={'bar'}, thread=lambda: 'foo worked'),
        TJob('bar', {'foo'}, before={'baz'}, thread=raiseUGH),
        TJob('baz', {'bar'}, thread=lambda: 'baz worked'),
    ]
    done = run_jobs(todo)
    assert verify_tasks(
        done, {'foo': 'foo worked', 'bar': ValueError('UGH'), 'baz': Cancelled}
    )


def test_one_failed_between_two_in_subprocs_cancels_last(run_jobs, tmp_path):
    foo_path = tmp_path / 'foo'
    baz_path = tmp_path / 'baz'
    todo = [
        TJob('foo', before={'bar'}, subproc=['touch', str(foo_path)]),
        TJob('bar', {'foo'}, before={'baz'}, subproc=['false']),
        TJob('baz', {'bar'}, subproc=['touch', str(baz_path)]),
    ]
    done = run_jobs(todo)
    assert verify_tasks(
        done,
        {'foo': 0, 'bar': CalledProcessError(1, ['false']), 'baz': Cancelled},
    )
    assert foo_path.is_file()
    assert not baz_path.exists()
