import asyncio
from contextlib import contextmanager
from functools import partial
import itertools
import logging
import os
import pytest
import signal
from subprocess import CalledProcessError
import time

from verify_events import EventVerifier, ExpectedJobEvents, Whatever

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


class TBasicJob:
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
        self.name = name
        self.deps = deps
        self.before = set() if before is None else set(before)
        self.call = call
        self.async_sleep = async_sleep
        self.spawn = [] if spawn is None else spawn
        self.await_spawn = await_spawn
        self.result = '{} done'.format(name) if result is None else result

        self.xevents = ExpectedJobEvents(name)
        self.xevents.add('add')
        self.xevents.add('start')

        # self.deps is processed _before_ .__call__() is invoked
        if self.deps:
            if 'MISSING' in self.deps:
                self.xevents.add('finish', fate='failed')  # expect KeyError
            else:
                self.xevents.add(
                    'await results', jobs=list(self.deps), pending=Whatever
                )

    def descendants(self):
        for spawn in self.spawn:
            yield spawn
            yield from spawn.descendants()

    async def do_work(self, ctx):
        result = None
        if self.call:
            result = self.call()
        return self.result if result is None else result

    async def __call__(self, ctx):
        dep_results = ctx.deps
        if self.deps:
            self.xevents.add('awaited results')
        ctx.logger.debug(f'Results from deps: {dep_results}')

        if self.async_sleep:
            ctx.logger.info(f'Async sleep for {self.async_sleep} seconds…')
            await asyncio.sleep(self.async_sleep)
            ctx.logger.info('Finished async sleep')

        result = await self.do_work(ctx)

        for job in self.spawn:
            ctx.add_job(job.name, job, job.deps)

        if self.await_spawn and self.spawn:
            spawn = [job.name for job in self.spawn]
            self.xevents.add('await results', jobs=spawn, pending=spawn)
            await ctx.results(*[job.name for job in self.spawn])
            self.xevents.add('awaited results')

        for b in self.before:
            assert b in ctx._scheduler.tasks  # The other job has started
            assert not ctx._scheduler.tasks[b].done()  # but not yet finished

        if isinstance(result, Exception):
            ctx.logger.info(f'Raising exception: {result}')
            self.xevents.add('finish', fate='failed')
            raise result
        else:
            ctx.logger.info(f'Returning result: {result!r}')
            self.xevents.add('finish', fate='success')
            return result


class TExternalWorkJob(TBasicJob):
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
            self.thread = lambda ctx: time.sleep(thread_sleep)
        else:
            self.thread = thread
        if subproc_sleep:
            if subproc is not None:
                raise ValueError('Cannot both sleep and work in subprocess')
            self.subproc = ['sleep', str(subproc_sleep)]
        else:
            self.subproc = subproc

    async def _do_thread_stuff(self, ctx):
        ctx.logger.debug(f'Await call {self.thread} in thread…')
        self.xevents.add('await worker slot')
        self.xevents.add('awaited worker slot', may_cancel=True)
        self.xevents.add('await worker thread', may_cancel=True, func=Whatever)
        try:
            ret = await ctx.call_in_thread(self.thread, ctx)
            self.xevents.add('awaited worker thread', fate='success')
        except asyncio.CancelledError:
            self.xevents.add(
                'awaited worker thread', may_cancel=True, fate='cancelled',
            )
            raise
        except Exception as e:
            ret = e
            self.xevents.add('awaited worker thread', fate='failed')
        ctx.logger.debug(f'Finished thread call: {ret!r}')
        return ret

    async def _do_subproc_stuff(self, ctx):
        ctx.logger.debug(f'Await run {self.subproc} in subprocess…')
        self.xevents.add('await worker slot')
        self.xevents.add('awaited worker slot', may_cancel=True)
        self.xevents.add(
            'await worker proc', may_cancel=True, argv=self.subproc
        )
        try:
            ret = await ctx.run_in_subprocess(self.subproc)
            self.xevents.add('awaited worker proc', returncode=0)
        except asyncio.CancelledError:
            self.xevents.add(
                'awaited worker proc', may_cancel=True, returncode=-15
            )
            raise
        except Exception as e:
            ret = e
            if isinstance(e, CalledProcessError):
                self.xevents.add(
                    'awaited worker proc', returncode=e.returncode
                )
        ctx.logger.debug(f'Finished subprocess run: {ret}')
        return ret

    async def do_work(self, ctx):
        result = await super().do_work(ctx)
        if self.thread:
            result = await self._do_thread_stuff(ctx)
        if self.subproc:
            result = await self._do_subproc_stuff(ctx)
        return result


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
def verified_events(scheduler, todo):
    """Check events generated by scheduler against expectations.

    Verify that the given scheduler generates the expected events while in
    this context. We set the scheduler's event_handler to collect all events
    that occur while in the context, and upon exiting the context, we verify
    the events emitted by the scheduler against the events expected by the
    TBasicJob instances in 'todo'.
    """
    initial_xevents = [j.xevents for j in todo]
    spawned_xevents = [
        j.xevents for j in itertools.chain(*[j.descendants() for j in todo])
    ]
    ev = EventVerifier()
    scheduler.event_handler = ev
    yield
    ev.verify_all(initial_xevents, spawned_xevents)


# Used to signal the expectation of a cancelled task
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
        for e in expect_lines_from_streams:
            if len(e):
                assert False, f'expected lines not found: {e!r}'

    def _verify_output(expect_stdout_streams, expect_stderr_streams=None):
        actual = capfd.readouterr()
        _verify_one_output(expect_stdout_streams, actual.out)
        if expect_stderr_streams is not None:
            _verify_one_output(expect_stderr_streams, actual.err)
        return True

    return _verify_output
