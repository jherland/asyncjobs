import sys

from conftest import TSimpleJob, verify_tasks

TJob = TSimpleJob


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
