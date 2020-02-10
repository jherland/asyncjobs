import pytest

from asyncjobs.basic import Scheduler

from conftest import (
    Cancelled,
    setup_scheduler,
    TSimpleJob,
    verify_tasks,
)

pytestmark = pytest.mark.asyncio


async def run(todo, **run_args):
    with setup_scheduler(Scheduler, todo) as scheduler:
        return await scheduler.run(**run_args)


TJob = TSimpleJob


async def test_zero_jobs_does_nothing():
    assert await run([]) == {}


async def test_one_ok_job():
    todo = [TJob('foo')]
    done = await run(todo)
    assert verify_tasks(done, {'foo': 'foo done'})


async def test_one_failed_job():
    done = await run([TJob('foo', result=ValueError('UGH'))])
    assert len(done) == 1
    e = done['foo'].exception()
    assert isinstance(e, ValueError) and e.args == ('UGH',)
    with pytest.raises(ValueError, match='UGH'):
        done['foo'].result()


async def test_cannot_add_second_job_with_same_name():
    with pytest.raises(ValueError):
        await run([TJob('foo'), TJob('foo')])


async def test_job_with_nonexisting_dependency_raises_KeyError():
    done = await run([TJob('foo', {'MISSING'})])
    with pytest.raises(KeyError, match='MISSING'):
        done['foo'].result()


async def test_two_independent_ok_jobs():
    todo = [TJob('foo'), TJob('bar')]
    done = await run(todo)
    assert verify_tasks(done, {'foo': 'foo done', 'bar': 'bar done'})


async def test_one_ok_before_another_ok_job():
    todo = [
        TJob('foo', before={'bar'}),
        TJob('bar', {'foo'}),
    ]
    done = await run(todo)
    assert verify_tasks(done, {'foo': 'foo done', 'bar': 'bar done'})


async def test_one_ok_before_one_failed_job():
    todo = [
        TJob('foo', before={'bar'}),
        TJob('bar', {'foo'}, result=ValueError('UGH')),
    ]
    done = await run(todo)
    assert verify_tasks(done, {'foo': 'foo done', 'bar': ValueError('UGH')})


async def test_one_failed_job_before_one_ok_job_cancels_second_job():
    todo = [
        TJob('foo', before={'bar'}, result=ValueError('UGH')),
        TJob('bar', {'foo'}),
    ]
    done = await run(todo)
    assert verify_tasks(done, {'foo': ValueError('UGH'), 'bar': Cancelled})


async def test_one_failed_job_before_two_ok_jobs_cancels_two_jobs():
    todo = [
        TJob('foo', before={'bar', 'baz'}, result=ValueError('UGH')),
        TJob('bar', {'foo'}),
        TJob('baz', {'foo'}),
    ]
    done = await run(todo)
    assert verify_tasks(
        done, {'foo': ValueError('UGH'), 'bar': Cancelled, 'baz': Cancelled}
    )


async def test_one_failed_job_before_two_dependent_jobs_cancels_two_jobs():
    todo = [
        TJob('foo', before={'bar'}, result=ValueError('UGH')),
        TJob('bar', {'foo'}, before={'baz'}),
        TJob('baz', {'bar'}),
    ]
    done = await run(todo)
    assert verify_tasks(
        done, {'foo': ValueError('UGH'), 'bar': Cancelled, 'baz': Cancelled}
    )


async def test_one_failed_job_between_two_ok_jobs_cancels_last_job():
    todo = [
        TJob('foo', before={'bar'}),
        TJob('bar', {'foo'}, before={'baz'}, result=ValueError('UGH')),
        TJob('baz', {'bar'}),
    ]
    done = await run(todo)
    assert verify_tasks(
        done, {'foo': 'foo done', 'bar': ValueError('UGH'), 'baz': Cancelled}
    )


async def test_one_ok_and_one_failed_job_without_keep_going_cancels_ok_job():
    todo = [
        TJob('foo', result=ValueError('UGH')),
        TJob('bar', async_sleep=0.01),  # allow time for potential cancellation
    ]
    done = await run(todo, keep_going=False)
    assert verify_tasks(done, {'foo': ValueError('UGH'), 'bar': Cancelled})


async def test_one_ok_and_one_failed_job_with_keep_going_runs_ok_job():
    todo = [
        TJob('foo', result=ValueError('UGH')),
        TJob('bar', async_sleep=0.01),  # allow time for potential cancellation
    ]
    done = await run(todo, keep_going=True)
    assert verify_tasks(done, {'foo': ValueError('UGH'), 'bar': 'bar done'})


# jobs that spawn child jobs


async def test_one_job_spawns_another():
    todo = [TJob('foo', spawn=[TJob('bar')])]
    done = await run(todo)
    assert verify_tasks(done, {'foo': 'foo done', 'bar': 'bar done'})


async def test_one_job_spawns_two_with_deps():
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
    done = await run(todo)
    assert verify_tasks(
        done, {'foo': 'foo done', 'bar': 'bar done', 'baz': 'baz done'}
    )


async def test_one_job_spawns_failing_job():
    todo = [TJob('foo', spawn=[TJob('bar', result=ValueError('UGH'))])]
    done = await run(todo)
    assert verify_tasks(done, {'foo': 'foo done', 'bar': ValueError('UGH')})


async def test_job_is_cancelled_when_waiting_for_failing_spawn():
    todo = [
        TJob(
            'foo',
            spawn=[TJob('bar', result=ValueError('UGH'))],
            await_spawn=True,
        )
    ]
    done = await run(todo)
    assert verify_tasks(done, {'foo': Cancelled, 'bar': ValueError('UGH')})


async def test_spawn_outliving_parent_is_not_cancelled_by_scheduler():
    todo = [TJob('foo', spawn=[TJob('bar', async_sleep=0.01)])]
    done = await run(todo)
    assert verify_tasks(done, {'foo': 'foo done', 'bar': 'bar done'})
