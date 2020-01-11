import pytest

from conftest import Cancelled, TSimpleJob, verify_tasks


TJob = TSimpleJob


def test_zero_jobs_does_nothing(run_jobs):
    assert run_jobs([]) == {}


def test_one_ok_job(run_jobs):
    todo = [TJob('foo')]
    done = run_jobs(todo)
    assert verify_tasks(done, {'foo': 'foo done'})


def test_one_failed_job(run_jobs):
    done = run_jobs([TJob('foo', result=ValueError('UGH'))])
    assert len(done) == 1
    e = done['foo'].exception()
    assert isinstance(e, ValueError) and e.args == ('UGH',)
    with pytest.raises(ValueError, match='UGH'):
        done['foo'].result()


def test_cannot_add_second_job_with_same_name(run_jobs):
    with pytest.raises(ValueError):
        run_jobs([TJob('foo'), TJob('foo')])


def test_job_with_nonexisting_dependency_raises_KeyError(run_jobs):
    done = run_jobs([TJob('foo', {'MISSING'})])
    with pytest.raises(KeyError, match='MISSING'):
        done['foo'].result()


def test_two_independent_ok_jobs(run_jobs):
    todo = [TJob('foo'), TJob('bar')]
    done = run_jobs(todo)
    assert verify_tasks(done, {'foo': 'foo done', 'bar': 'bar done'})


def test_one_ok_before_another_ok_job(run_jobs):
    todo = [
        TJob('foo', before={'bar'}),
        TJob('bar', {'foo'}),
    ]
    done = run_jobs(todo)
    assert verify_tasks(done, {'foo': 'foo done', 'bar': 'bar done'})


def test_one_ok_before_one_failed_job(run_jobs):
    todo = [
        TJob('foo', before={'bar'}),
        TJob('bar', {'foo'}, result=ValueError('UGH')),
    ]
    done = run_jobs(todo)
    assert verify_tasks(done, {'foo': 'foo done', 'bar': ValueError('UGH')})


def test_one_failed_job_before_one_ok_job_cancels_second_job(run_jobs):
    todo = [
        TJob('foo', before={'bar'}, result=ValueError('UGH')),
        TJob('bar', {'foo'}),
    ]
    done = run_jobs(todo)
    assert verify_tasks(done, {'foo': ValueError('UGH'), 'bar': Cancelled})


def test_one_failed_job_before_two_ok_jobs_cancels_two_jobs(run_jobs):
    todo = [
        TJob('foo', before={'bar', 'baz'}, result=ValueError('UGH')),
        TJob('bar', {'foo'}),
        TJob('baz', {'foo'}),
    ]
    done = run_jobs(todo)
    assert verify_tasks(
        done, {'foo': ValueError('UGH'), 'bar': Cancelled, 'baz': Cancelled}
    )


def test_one_failed_job_before_two_dependent_jobs_cancels_two_jobs(run_jobs):
    todo = [
        TJob('foo', before={'bar'}, result=ValueError('UGH')),
        TJob('bar', {'foo'}, before={'baz'}),
        TJob('baz', {'bar'}),
    ]
    done = run_jobs(todo)
    assert verify_tasks(
        done, {'foo': ValueError('UGH'), 'bar': Cancelled, 'baz': Cancelled}
    )


def test_one_failed_job_between_two_ok_jobs_cancels_last_job(run_jobs):
    todo = [
        TJob('foo', before={'bar'}),
        TJob('bar', {'foo'}, before={'baz'}, result=ValueError('UGH')),
        TJob('baz', {'bar'}),
    ]
    done = run_jobs(todo)
    assert verify_tasks(
        done, {'foo': 'foo done', 'bar': ValueError('UGH'), 'baz': Cancelled}
    )


def test_one_ok_and_one_failed_job_without_keep_going_cancels_ok_job(run_jobs):
    todo = [
        TJob('foo', result=ValueError('UGH')),
        TJob('bar', async_sleep=0.01),  # allow time for potential cancellation
    ]
    done = run_jobs(todo, keep_going=False)
    assert verify_tasks(done, {'foo': ValueError('UGH'), 'bar': Cancelled})


def test_one_ok_and_one_failed_job_with_keep_going_runs_ok_job(run_jobs):
    todo = [
        TJob('foo', result=ValueError('UGH')),
        TJob('bar', async_sleep=0.01),  # allow time for potential cancellation
    ]
    done = run_jobs(todo, keep_going=True)
    assert verify_tasks(done, {'foo': ValueError('UGH'), 'bar': 'bar done'})


# jobs that spawn child jobs


def test_one_job_spawns_another(run_jobs):
    todo = [TJob('foo', spawn=[TJob('bar')])]
    done = run_jobs(todo)
    assert verify_tasks(done, {'foo': 'foo done', 'bar': 'bar done'})


def test_one_job_spawns_two_with_deps(run_jobs):
    # foo start bar and baz, baz depends on bar, foo waits for both to finish
    todo = [
        TJob(
            'foo',
            spawn=[
                TJob('bar', before={'foo', 'baz'}),
                TJob('baz', {'bar'}, before={'foo'}),
            ],
            await_spawn=True,
        )
    ]
    done = run_jobs(todo)
    assert verify_tasks(
        done, {'foo': 'foo done', 'bar': 'bar done', 'baz': 'baz done'}
    )


def test_one_job_spawns_failing_job(run_jobs):
    todo = [TJob('foo', spawn=[TJob('bar', result=ValueError('UGH'))])]
    done = run_jobs(todo)
    assert verify_tasks(done, {'foo': 'foo done', 'bar': ValueError('UGH')})


def test_job_is_cancelled_when_waiting_for_failing_spawn(run_jobs):
    todo = [
        TJob(
            'foo',
            spawn=[TJob('bar', result=ValueError('UGH'))],
            await_spawn=True,
        )
    ]
    done = run_jobs(todo)
    assert verify_tasks(done, {'foo': Cancelled, 'bar': ValueError('UGH')})
