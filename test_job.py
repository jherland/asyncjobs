import asyncio
import concurrent.futures
import pytest

from scheduler import JobWithDeps, Scheduler


class TJob(JobWithDeps):
    """A job with test instrumentation."""

    def __init__(
        self, name, deps=None, *, result=None, before=None, delay=False
    ):
        self.result = '{} done'.format(name) if result is None else result
        self.before = set() if before is None else before
        self.delay = delay  # Allow other jobs to run before we conclude
        super().__init__(name=name, deps=deps or set())

    async def __call__(self, scheduler):
        await super().__call__(scheduler)
        if self.delay:
            await asyncio.sleep(0.01)
        for b in self.before:
            assert b in scheduler.results  # The other job has been started
            assert not scheduler.results[b].done()  # but is not yet finished
        if isinstance(self.result, Exception):
            raise self.result
        else:
            return self.result


@pytest.fixture(params=[0, 1, 2, 4, -1, -2, -4])
def scheduler(request):
    if request.param == 0:  # run everything syncronously
        workers = None
    elif request.param > 0:  # number of worker threads
        workers = concurrent.futures.ThreadPoolExecutor(request.param)
    else:  # number of worker processes
        workers = concurrent.futures.ProcessPoolExecutor(-request.param)
    return Scheduler(workers)


@pytest.fixture
def run_jobs(scheduler):
    def _run_jobs(jobs, keep_going=False):
        for job in jobs:
            scheduler.add(job)
        return asyncio.run(scheduler.run(keep_going=keep_going))

    return _run_jobs


def assert_all_ok(tasks, jobs):
    assert len(jobs) == len(tasks)
    for job in jobs:
        assert tasks[job.name].result() == job.result


Cancelled = object()


def assert_tasks(tasks, expects):
    for job_name in tasks.keys() & expects.keys():
        expect = expects[job_name]
        task = tasks[job_name]
        if expect is Cancelled:
            assert task.cancelled()
        elif isinstance(expect, Exception):
            e = task.exception()
            assert isinstance(e, expect.__class__)
            assert e.args == expect.args
        else:
            assert task.result() == expect


def test_one_ok_job(run_jobs):
    todo = [TJob('foo')]
    done = run_jobs(todo)
    assert_all_ok(done, todo)


def test_two_independent_ok_jobs(run_jobs):
    todo = [TJob('foo'), TJob('bar')]
    done = run_jobs(todo)
    assert_all_ok(done, todo)


def test_two_dependent_ok_jobs(run_jobs):
    todo = [
        TJob('foo', before={'bar'}),
        TJob('bar', {'foo'}),
    ]
    done = run_jobs(todo)
    assert_all_ok(done, todo)


def test_cannot_add_second_job_with_same_name(run_jobs):
    with pytest.raises(ValueError):
        run_jobs([TJob('foo'), TJob('foo')])


def test_job_with_nonexisting_dependency_raises_KeyError(run_jobs):
    done = run_jobs([TJob('foo', {'MISSING'})])
    with pytest.raises(KeyError, match='MISSING'):
        done['foo'].result()


def test_one_failed_job(run_jobs):
    done = run_jobs([TJob('foo', result=ValueError('UGH'))])
    assert len(done) == 1
    e = done['foo'].exception()
    assert isinstance(e, ValueError) and e.args == ('UGH',)
    with pytest.raises(ValueError, match='UGH'):
        done['foo'].result()


def test_one_ok_before_one_failed_job(run_jobs):
    todo = [
        TJob('foo', before={'bar'}),
        TJob('bar', {'foo'}, result=ValueError('UGH')),
    ]
    done = run_jobs(todo)
    assert_tasks(done, {'foo': 'foo done', 'bar': ValueError('UGH')})


def test_one_ok_after_one_failed_job(run_jobs):
    todo = [
        TJob('foo', before={'bar'}, result=ValueError('UGH')),
        TJob('bar', {'foo'}),
    ]
    done = run_jobs(todo)
    assert_tasks(done, {'foo': ValueError('UGH'), 'bar': Cancelled})


def test_one_ok_and_one_failed_job_without_keep_going(run_jobs):
    todo = [
        TJob('foo', result=ValueError('UGH')),
        TJob('bar', delay=True),
    ]
    done = run_jobs(todo, keep_going=False)
    assert_tasks(done, {'foo': ValueError('UGH'), 'bar': Cancelled})


def test_one_ok_and_one_failed_job_with_keep_going(run_jobs):
    todo = [
        TJob('foo', result=ValueError('UGH')),
        TJob('bar', delay=True),
    ]
    done = run_jobs(todo, keep_going=True)
    assert_tasks(done, {'foo': ValueError('UGH'), 'bar': 'bar done'})
