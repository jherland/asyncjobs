import pytest
import sys

from jobs import Scheduler

from conftest import setup_and_run_scheduler, TSimpleJob, verify_tasks


@pytest.fixture
def run_jobs():
    def _run_jobs(todo, **kwargs):
        return setup_and_run_scheduler(Scheduler, todo, **kwargs)

    return _run_jobs


class TJob(TSimpleJob):
    def __init__(self, *args, prefix='', **kwargs):
        super().__init__(*args, **kwargs)
        self.prefix = prefix


def test_output_from_one_job(run_jobs, capfd):
    out = "This is foo's stdout\n"
    err = "This is foo's stderr\n"

    def print_something():
        print(out, end='')
        print(err, end='', file=sys.stderr)

    todo = [TJob('foo', call=print_something)]
    done = run_jobs(todo)
    assert verify_tasks(done, {'foo': 'foo done'})
    actual = capfd.readouterr()
    assert out == actual.out
    assert err == actual.err
