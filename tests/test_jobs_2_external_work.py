from subprocess import CalledProcessError

from conftest import Cancelled, TExternalWorkJob, verify_tasks

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
