import asyncio
from functools import partial
import logging
import pytest
from subprocess import CalledProcessError
import time

from conftest import abort_in, assert_elapsed_time_within
from scheduler import (
    Job,
    ExternalWorkScheduler,
    SignalHandlingScheduler,
)


logger = logging.getLogger('test_job')


class TJob(Job):
    """A job with test instrumentation."""

    def __init__(
        self,
        name,
        deps=None,
        *,
        result=None,
        before=None,
        async_sleep=0,
        thread_sleep=0,
        subproc_sleep=0,
        thread=None,
        subproc=None,
    ):
        super().__init__(name=name, deps=deps)
        self.result = '{} done'.format(name) if result is None else result
        self.before = set() if before is None else set(before)
        self.async_sleep = async_sleep
        self.thread_sleep = thread_sleep
        self.subproc_sleep = subproc_sleep
        self.thread = thread
        self.subproc = subproc

    async def __call__(self, scheduler):
        dep_results = await super().__call__(scheduler)
        self.logger.debug(f'Results from deps: {dep_results}')

        thread_result, subproc_result = None, None
        if self.async_sleep:
            self.logger.info(f'Async sleep for {self.async_sleep} seconds…')
            await asyncio.sleep(self.async_sleep)
            self.logger.info(f'Finished async sleep')
        if self.thread_sleep:
            self.logger.debug(f'time.sleep({self.thread_sleep}) in thread…')
            thread_result = await scheduler.call_in_thread(
                partial(time.sleep, self.thread_sleep)
            )
            self.logger.debug(f'Finished thread sleep: {thread_result}')
        if self.subproc_sleep:
            self.logger.debug(f'sleep {self.subproc_sleep} in subprocess…')
            subproc_result = await scheduler.run_in_subprocess(
                ['sleep', str(self.subproc_sleep)]
            )
            self.logger.debug(f'Finished subproc sleep: {subproc_result}')
        if self.thread:
            self.logger.debug(f'Await call {self.thread} in thread…')
            try:
                thread_result = await scheduler.call_in_thread(self.thread)
            except Exception as e:
                thread_result = e
            self.logger.debug(f'Finished thread call: {thread_result}')
        if self.subproc:
            self.logger.debug(f'Await run {self.subproc} in subprocess…')
            try:
                subproc_result = await scheduler.run_in_subprocess(
                    self.subproc
                )
            except Exception as e:
                subproc_result = e
            self.logger.debug(f'Finished subprocess run: {subproc_result}')

        for b in self.before:
            assert b in scheduler.tasks  # The other job has been started
            assert not scheduler.tasks[b].done()  # but is not yet finished

        if isinstance(thread_result, Exception):
            self.logger.info(f'Raising thread exception: {thread_result}')
            raise thread_result
        elif isinstance(subproc_result, Exception):
            self.logger.info(f'Raising subproc exception: {subproc_result}')
            raise subproc_result
        elif isinstance(self.result, Exception):
            self.logger.info(f'Raising exception: {self.result}')
            raise self.result
        elif thread_result is not None:
            self.logger.info(f'Returning thread result: {thread_result}')
            return thread_result
        elif subproc_result is not None:
            self.logger.info(f'Returning subproc result: {subproc_result}')
            return subproc_result
        else:
            self.logger.info(f'Returning result: {self.result}')
            return self.result


class TScheduler(SignalHandlingScheduler, ExternalWorkScheduler):
    pass


@pytest.fixture(params=[1, 2, 4, 100])
def scheduler(request):
    logger.info(f'Creating scheduler with {request.param} worker threads')
    yield TScheduler(workers=request.param)


@pytest.fixture
def run_jobs(scheduler):
    def _run_jobs(jobs, abort_after=0, **kwargs):
        for job in jobs:
            scheduler.add(job)
        with abort_in(abort_after):
            return asyncio.run(scheduler.run(**kwargs), debug=True)

    return _run_jobs


Cancelled = object()


def verify_tasks(tasks, expects):
    errors = 0

    def fail(job_name, expect, actual=None):
        nonlocal errors
        if actual is None:
            logger.error(f'{job_name}: {expect}')
        else:
            logger.error(f'{job_name}: expected {expect!r}, actual {actual!r}')
        errors += 1

    for name in set(tasks.keys()) | set(expects.keys()):
        try:
            expect = expects[name]
            task = tasks[name]
        except KeyError:
            e = 'present' if name in expects else 'missing'
            t = 'present' if name in tasks else 'missing'
            fail(name, f'{e} in expects, {t} in tasks')
            continue
        if expect is Cancelled:
            if not task.cancelled():
                fail(name, Cancelled, task)
        elif isinstance(expect, Exception):
            e = task.exception()
            if not isinstance(e, expect.__class__):
                fail(name, expect.__class__, type(e))
            if e.args != expect.args:
                fail(name, expect.args, e.args)
        else:
            if task.result() != expect:
                fail(name, expect, task.result())
    return errors == 0


def verify_all_jobs_succeeded(tasks, jobs):
    return verify_tasks(tasks, {job.name: job.result for job in jobs})


def test_zero_jobs_does_nothing(run_jobs):
    done = run_jobs([])
    assert done == {}


# simple async jobs, no threading or subprocesses


def test_one_ok_job(run_jobs):
    todo = [TJob('foo')]
    done = run_jobs(todo)
    assert verify_all_jobs_succeeded(done, todo)


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
    assert verify_all_jobs_succeeded(done, todo)


def test_one_ok_before_another_ok_job(run_jobs):
    todo = [
        TJob('foo', before={'bar'}),
        TJob('bar', {'foo'}),
    ]
    done = run_jobs(todo)
    assert verify_all_jobs_succeeded(done, todo)


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


# jobs with work performed in threads and/or subprocesses


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


def test_abort_hundred_jobs_returns_immediately(run_jobs):
    todo = [TJob(f'foo #{i}', async_sleep=0.3) for i in range(100)]
    with assert_elapsed_time_within(0.2):
        done = run_jobs(todo, abort_after=0.1)
    assert verify_tasks(done, {f'foo #{i}': Cancelled for i in range(100)})


def test_abort_hundred_jobs_in_threads_returns_immediately(run_jobs):
    todo = [TJob(f'foo #{i}', thread_sleep=0.3) for i in range(100)]
    with assert_elapsed_time_within(0.2):
        done = run_jobs(todo, abort_after=0.1)
    assert verify_tasks(done, {f'foo #{i}': Cancelled for i in range(100)})


def test_abort_hundred_jobs_in_subprocs_returns_immediately(run_jobs):
    todo = [TJob(f'foo #{i}', subproc_sleep=30) for i in range(100)]
    with assert_elapsed_time_within(0.5):
        done = run_jobs(todo, abort_after=0.1)
    assert verify_tasks(done, {f'foo #{i}': Cancelled for i in range(100)})
