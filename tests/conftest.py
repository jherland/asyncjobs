import asyncio
from contextlib import contextmanager
from functools import partial
import logging
import os
import pytest
import signal
from subprocess import CalledProcessError
import time

from asyncjobs import basic, external_work

logger = logging.getLogger(__name__)


@contextmanager
def abort_in(when=None, assert_on_escape=True):
    """Simulate Ctrl+C (aka. KeyboardInterrupt/SIGINT) after the given delay.

    This will schedule a SIGINT signal in the current process after the given
    number of seconds if we're still within the context. Upon context exit,
    the signal is cancelled.

    If the signal is handled within the context, no further action is taken.
    Otherwise, if the signal/KeyboardInterrupt escapes the context, an error
    is logged and (unless assert_on_escape is explicitly disabled) an
    assertion is raised (to fail the current test).
    """
    if when is None:  # Do nothing
        yield
        return

    async def wait_and_kill():
        await asyncio.sleep(when)
        logger.warning('Raising SIGINT to simulate Ctrl+C…')
        os.kill(os.getpid(), signal.SIGINT)

    task = asyncio.create_task(wait_and_kill())
    try:
        yield
    except KeyboardInterrupt:
        logger.error('SIGINT/KeyboardInterrupt escaped the context!')
        if assert_on_escape:
            assert False, 'SIGINT/KeyboardInterrupt escaped the context!'
    finally:
        if not task.done():
            logger.debug(f'context complete before {when}')
            task.cancel()


@contextmanager
def assert_elapsed_time_within(time_limit):
    """Measure time used in context and fail test if not within given limit."""
    before = time.time()
    try:
        yield
    finally:
        after = time.time()
        assert after < before + time_limit


# Wildcard used for don't-care values in expected event fields
Whatever = object()


def verify_event(expect, actual):
    expect_keys = set(expect.keys()) - {'MAY_CANCEL'}
    assert expect_keys == set(actual.keys())
    keys = sorted(expect_keys)
    for e, a in zip((expect[k] for k in keys), (actual[k] for k in keys)):
        if e is not Whatever:
            assert e == a
    return True


class TSimpleJob(basic.Job):
    """Async jobs with test instrumentation."""

    def __init__(
        self,
        name,
        deps=None,
        *,
        before=None,
        call=None,
        async_sleep=0,
        spawn=None,
        await_spawn=False,
        result=None,
    ):
        super().__init__(name=name, deps=deps)
        self.before = set() if before is None else set(before)
        self.call = call
        self.async_sleep = async_sleep
        self.spawn = [] if spawn is None else spawn
        self.await_spawn = await_spawn
        self.result = '{} done'.format(name) if result is None else result

        self._expected_events = []
        self._expect_event('add'),
        self._expect_event('start'),

    def _expect_event(self, event, *, may_cancel=False, **kwargs):
        d = {'event': event, 'job': self.name, 'timestamp': Whatever}
        d.update(kwargs)
        if may_cancel:
            d['MAY_CANCEL'] = True
        self._expected_events.append(d)

    async def do_work(self, scheduler):
        result = None
        if self.call:
            result = self.call()
        return self.result if result is None else result

    def expected_events(self):
        if self._expected_events[-1]['event'] != 'finish':  # we were cancelled
            self._expect_event('finish', fate='cancelled')
        return self._expected_events

    async def __call__(self, scheduler):
        if self.deps:
            if self.deps == {'MISSING'}:
                self._expect_event('finish', fate='failed')  # expect KeyError
            else:
                self._expect_event(
                    'await results', jobs=list(self.deps), pending=Whatever
                )
        dep_results = await super().__call__(scheduler)
        if self.deps:
            self._expect_event('awaited results')
        self.logger.debug(f'Results from deps: {dep_results}')

        if self.async_sleep:
            self.logger.info(f'Async sleep for {self.async_sleep} seconds…')
            await asyncio.sleep(self.async_sleep)
            self.logger.info('Finished async sleep')

        result = await self.do_work(scheduler)

        for job in self.spawn:
            scheduler.add(job)

        if self.await_spawn and self.spawn:
            spawn = [job.name for job in self.spawn]
            self._expect_event('await results', jobs=spawn, pending=spawn)
            await scheduler.results(*[job.name for job in self.spawn])
            self._expect_event('awaited results')

        for b in self.before:
            assert b in scheduler.tasks  # The other job has been started
            assert not scheduler.tasks[b].done()  # but is not yet finished

        if isinstance(result, Exception):
            self.logger.info(f'Raising exception: {result}')
            self._expect_event('finish', fate='failed')
            raise result
        else:
            self.logger.info(f'Returning result: {result!r}')
            self._expect_event('finish', fate='success')
            return result


class TExternalWorkJob(TSimpleJob, external_work.Job):
    """Test jobs with thread/subprocess capabilities."""

    def __init__(
        self,
        *args,
        thread_sleep=0,
        thread=None,
        subproc_sleep=0,
        subproc=None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        if thread_sleep:
            if thread is not None:
                raise ValueError('Cannot both sleep and work in thread')
            self.thread = partial(time.sleep, thread_sleep)
        else:
            self.thread = thread
        if subproc_sleep:
            if subproc is not None:
                raise ValueError('Cannot both sleep and work in subprocess')
            self.subproc = ['sleep', str(subproc_sleep)]
        else:
            self.subproc = subproc

    async def _do_thread_stuff(self, scheduler):
        self.logger.debug(f'Await call {self.thread} in thread…')
        self._expect_event('await worker slot')
        self._expect_event('awaited worker slot', may_cancel=True)
        self._expect_event(
            'await worker thread', may_cancel=True, func=Whatever
        )
        try:
            ret = await self.call_in_thread(self.thread)
            self._expect_event('awaited worker thread', fate='success')
        except asyncio.CancelledError:
            self._expect_event(
                'awaited worker thread', may_cancel=True, fate='cancelled',
            )
            raise
        except Exception as e:
            ret = e
            self._expect_event('awaited worker thread', fate='failed')
        self.logger.debug(f'Finished thread call: {ret!r}')
        return ret

    async def _do_subproc_stuff(self, scheduler):
        self.logger.debug(f'Await run {self.subproc} in subprocess…')
        self._expect_event('await worker slot')
        self._expect_event('awaited worker slot', may_cancel=True)
        self._expect_event(
            'await worker proc', may_cancel=True, argv=self.subproc
        )
        try:
            ret = await self.run_in_subprocess(self.subproc)
            self._expect_event('awaited worker proc', exit=0)
        except asyncio.CancelledError:
            self._expect_event(
                'awaited worker proc', may_cancel=True, exit=-15
            )
            raise
        except Exception as e:
            ret = e
            if isinstance(e, CalledProcessError):
                self._expect_event('awaited worker proc', exit=e.returncode)
        self.logger.debug(f'Finished subprocess run: {ret}')
        return ret

    async def do_work(self, scheduler):
        result = await super().do_work(scheduler)
        if self.thread:
            result = await self._do_thread_stuff(scheduler)
        if self.subproc:
            result = await self._do_subproc_stuff(scheduler)
        return result


def verify_events(scheduler, start_time, actual, todo):
    after = time.time()
    num_jobs = len(todo)
    num_tasks = len(scheduler.tasks)
    expect = {j.name: j.expected_events() for j in todo}

    # Timestamps are in sorted order and all between 'start_time' and 'after'
    timestamps = [e['timestamp'] for e in actual]
    assert timestamps == sorted(timestamps)
    assert timestamps[0] >= start_time
    assert timestamps[-1] <= after

    # Initial jobs are added before execution starts
    expect_adds = [e.pop(0) for e in expect.values()]
    for e, a in zip(expect_adds, actual[:num_jobs]):
        assert verify_event(e, a)
    actual = actual[num_jobs:]

    # Overall execution start and finish
    overall_start, overall_finish = actual.pop(0), actual.pop()
    assert verify_event(
        {
            'event': 'start',
            'num_jobs': num_jobs,
            'keep_going': Whatever,
            'timestamp': Whatever,
        },
        overall_start,
    )
    assert verify_event(
        {'event': 'finish', 'num_tasks': num_tasks, 'timestamp': Whatever},
        overall_finish,
    )

    if expect:
        # Jobs are started
        expect_starts = [e.pop(0) for e in expect.values()]
        for e, a in zip(expect_starts, actual[:num_jobs]):
            assert verify_event(e, a)
        actual = actual[num_jobs:]

        # Await task execution
        await_tasks, awaited_tasks = actual.pop(0), actual.pop()
        assert verify_event(
            {
                'event': 'await tasks',
                'jobs': [j.name for j in todo],
                'timestamp': Whatever,
            },
            await_tasks,
        )
        assert verify_event(
            {'event': 'awaited tasks', 'timestamp': Whatever}, awaited_tasks,
        )

        # Remaining events belong to individual tasks
        # This includes expected events from tasks that were spawned from
        # other tasks (hence not part of the original 'todo').
        for name, job in scheduler.jobs.items():
            if name not in expect:  # job was spawned after .run()
                expect[name] = job.expected_events()
        # Verify each event by pairing it w/that job's next expected event
        while actual:
            a = actual.pop(0)
            job_name = a['job']
            e = expect[job_name].pop(0)
            if a['event'] == 'finish' and a['fate'] == 'cancelled':
                while e.get('MAY_CANCEL', False):
                    e = expect[job_name].pop(0)
            assert verify_event(e, a)

        # No more expected events
        assert all(len(e) == 0 for e in expect.values())

    assert not actual  # no more actual events
    return True


@pytest.fixture(params=[1, 2, 4, 100])
def num_workers(request):
    return request.param


@pytest.fixture
def scheduler_with_workers(num_workers):
    def make_scheduler_class(*bases):
        class _Scheduler(*bases):
            pass

        logger.info(f'creating scheduler with {num_workers} worker threads')
        return partial(_Scheduler, workers=num_workers)

    return make_scheduler_class


@contextmanager
def setup_scheduler(scheduler_class, todo):
    events = []
    scheduler = scheduler_class(event_handler=events.append)
    before = time.time()
    for job in todo:
        scheduler.add(job)
    yield scheduler
    verify_events(scheduler, before, events, todo)


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


@pytest.fixture
def verify_output(capfd):
    def _verify_one_output(expect_lines_from_streams, actual_text):
        logger.debug(
            f'Verifying {actual_text!r} against {expect_lines_from_streams}'
        )
        actual_lines = actual_text.split('\n')
        assert actual_lines.pop() == ''
        for actual_line in actual_lines:
            # Expect this line is the next line from one of our streams
            expected = set()
            for expect_lines in expect_lines_from_streams:
                if expect_lines and actual_line == expect_lines[0]:  # found it
                    expect_lines.pop(0)
                    break
                elif expect_lines:
                    expected.add(expect_lines[0])
            else:  # no match for any stream
                assert False, f'actual: {actual_line!r}, expect: {expected!r}'
        # no more lines expected
        assert all(e == [] for e in expect_lines_from_streams)

    def _verify_output(expect_stdout_streams, expect_stderr_streams=None):
        actual = capfd.readouterr()
        _verify_one_output(expect_stdout_streams, actual.out)
        if expect_stderr_streams is not None:
            _verify_one_output(expect_stderr_streams, actual.err)
        return True

    return _verify_output
