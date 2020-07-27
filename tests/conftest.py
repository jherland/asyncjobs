import asyncio
import contextlib
from functools import partial
import itertools
import logging
import os
from pathlib import Path
import pytest
import signal
from subprocess import CalledProcessError
import sys
import time
import traceback

from asyncjobs import logcontext
from verify_events import EventVerifier, ExpectedJobEvents, Whatever

logger = logging.getLogger(__name__)


@contextlib.asynccontextmanager
async def abort_in(when=None, assert_on_escape=True):
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
            with contextlib.suppress(asyncio.CancelledError):
                await task


@contextlib.contextmanager
def assert_elapsed_time(predicate):
    """Measure time used in context and assert that predicate holds.

    Calculate elapsed time in context, and assert predicate(elapsed).
    """
    before = time.time()
    try:
        yield
    finally:
        after = time.time()
        logger.debug(f'Passing {after - before} to predicate {predicate}')
        assert predicate(after - before)


class TBasicJob:
    """Async jobs with test instrumentation."""

    def __init__(
        self,
        name,
        deps=None,
        *,
        before=None,
        async_sleep=0,
        coro=None,
        spawn=None,
        await_spawn=False,
        result=None,
    ):
        self.name = name
        self.deps = deps
        self.before = set() if before is None else set(before)
        self.async_sleep = async_sleep
        self.coro = coro
        self.spawn = [] if spawn is None else spawn
        self.await_spawn = await_spawn
        self.result = f'{name} done' if result is None else result

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

    async def __call__(self, ctx):
        # Await results from dependencies
        dep_results = ctx.deps
        if self.deps:
            self.xevents.add('awaited results')
        logger.debug(f'Results from deps: {dep_results}')

        # Sleep, if requested
        if self.async_sleep:
            logger.info(f'Async sleep for {self.async_sleep} seconds…')
            await asyncio.sleep(self.async_sleep)
            logger.info('Finished async sleep')

        # Do the scheduled work
        if self.coro is not None:
            try:
                ctx.tjob = self  # give test code access to test job instance
                coro = self.coro(ctx)
                assert asyncio.iscoroutine(coro), f'not a coroutine: {coro}'
                self.result = await coro
            except asyncio.CancelledError:
                logger.info('Cancelled!')
                self.xevents.add('finish', fate='cancelled')
                raise
            except Exception as e:
                logger.info(f'Raising exception: {e!r}')
                traceback.print_exc()
                self.xevents.add('finish', fate='failed')
                raise

        # Spawn children jobs
        for job in self.spawn:
            ctx.add_job(job.name, job, job.deps)

        if self.await_spawn and self.spawn:
            spawn = [job.name for job in self.spawn]
            self.xevents.add('await results', jobs=spawn, pending=spawn)
            await ctx.results(*[job.name for job in self.spawn])
            self.xevents.add('awaited results')

        # Verify that our dependents are still waiting for us
        for b in self.before:
            assert b in ctx._scheduler.tasks  # The other job has started
            assert not ctx._scheduler.tasks[b].done()  # but not yet finished

        # Return result or raise exception
        if isinstance(self.result, Exception):
            logger.info(f'Raising exception: {self.result!r}')
            self.xevents.add('finish', fate='failed')
            raise self.result
        else:
            logger.info(f'Returning result: {self.result!r}')
            self.xevents.add('finish', fate='success')
            return self.result


class TExternalWorkJob(TBasicJob):
    """Test jobs with thread/subprocess capabilities."""

    def __init__(self, *args, thread=None, argv=None, **kwargs):
        super().__init__(*args, **kwargs)
        if thread:
            assert argv is None and self.coro is None
            self.coro = partial(self.run_thread, thread)
        elif argv:
            assert thread is None and self.coro is None
            self.coro = partial(self.run_subprocess, argv)

    async def run_thread(self, thread, ctx, **kwargs):
        with self.thread_xevents():
            return await ctx.call_in_thread(thread, ctx, **kwargs)

    async def run_subprocess(self, argv, ctx):
        with self.subprocess_xevents(argv, may_cancel=True):
            return await ctx.run_in_subprocess(argv, check=True)

    @contextlib.contextmanager
    def thread_xevents(self, reuse_ticket=False):
        if not reuse_ticket:
            self.xevents.add('await worker slot')
            self.xevents.add('awaited worker slot', may_cancel=True)
        self.xevents.add(
            'start work in thread', func=Whatever, may_cancel=True
        )
        try:
            yield
            self.xevents.add('finish work in thread', fate='success')
        except asyncio.CancelledError:
            self.xevents.add(
                'finish work in thread', fate='cancelled', may_cancel=True
            )
            raise
        except Exception:
            self.xevents.add('finish work in thread', fate='failed')
            raise

    @contextlib.contextmanager
    def subprocess_xevents(
        self, argv, result=None, may_cancel=False, reuse_ticket=False
    ):
        if not reuse_ticket:
            self.xevents.add('await worker slot')
            self.xevents.add('awaited worker slot', may_cancel=may_cancel)
        self.xevents.add(
            'start work in subprocess', argv=argv, may_cancel=may_cancel
        )

        try:
            yield
            if result is None:  # Normal return: assume exit code 0
                self.xevents.add('finish work in subprocess', returncode=0)
        except asyncio.CancelledError:
            if result is None:  # Assume cancellation was expected
                self.xevents.add(
                    'subprocess terminate',
                    argv=argv,
                    pid=Whatever,
                    may_cancel=may_cancel,
                )
                self.xevents.add(
                    'finish work in subprocess',
                    returncode=-15,
                    may_cancel=may_cancel,
                )
            raise
        except Exception as e:
            if result is None and isinstance(e, CalledProcessError):
                self.xevents.add(
                    'finish work in subprocess', returncode=e.returncode
                )
            raise
        finally:
            if result == 'terminate' or result == 'kill':
                self.xevents.add(
                    'subprocess terminate',
                    argv=argv,
                    pid=Whatever,
                    may_cancel=may_cancel,
                )
                if result == 'kill':
                    self.xevents.add(
                        'subprocess kill',
                        argv=argv,
                        pid=Whatever,
                        may_cancel=may_cancel,
                    )
                    result = -9
                else:
                    result = -15
            if result is not None:
                self.xevents.add(
                    'finish work in subprocess',
                    returncode=result,
                    may_cancel=may_cancel,
                )


@pytest.fixture(params=[1, 2, 4, 100])
def num_workers(request):
    return request.param


@pytest.fixture
def num_jobs(num_workers):
    """How many jobs to run in stress tests."""
    return min(100, num_workers * 10)


def verify_number_of_tasks(n):
    all_tasks = asyncio.all_tasks()
    if len(asyncio.all_tasks()) != n:
        message = f'There are {len(all_tasks)} != {n} running!'
        logger.error(message)
        for task in all_tasks:
            logger.error(f'  - {task}')
        raise RuntimeError(message)


@pytest.fixture
def scheduler_with_workers(num_workers):
    def make_scheduler_class(*bases):
        class _Scheduler(*bases):
            pass

        logger.info(f'creating scheduler with {num_workers} worker threads')
        return partial(_Scheduler, workers=num_workers)

    return make_scheduler_class


def mock_argv(*args):
    mock_path = Path(__file__).parent / 'subprocess_helper.py'
    return [sys.executable, '-u', str(mock_path)] + list(args)


@contextlib.contextmanager
def adjusted_logger_level(logger, level):
    old_level = logger.level
    logger.setLevel(level)
    try:
        yield
    finally:
        logger.setLevel(old_level)


@contextlib.contextmanager
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
    NotGiven = object()

    def fail(job_name, expect, actual=NotGiven):
        nonlocal errors
        if actual is NotGiven:
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
        elif callable(expect):  # verify by passing task to predicate
            if not expect(task):
                fail(name, f'{expect}({task}) failed')
        else:
            if task.result() != expect:
                fail(name, expect, task.result())
    return errors == 0


class ListHandler(logging.Handler):
    """A log handler implementation that simply stores log records in a list.

    Used to test that log records are handled/formatted correctly.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.records = []
        self.messages = []

    def emit(self, record):
        self.records.append(record)
        self.messages.append(self.format(record))


@pytest.fixture
def logger_with_listhandler():
    logger = logging.getLogger('test')
    handler = ListHandler()
    handler.setFormatter(logcontext.Formatter())
    logger.addHandler(handler)
    return logger, handler


@pytest.fixture
def verify_output(capfd, logger_with_listhandler):
    def _verify_one_output(expect_lines_from_streams, actual_text):
        actual_lines = actual_text.split('\n')
        logger.debug(
            f'Verifying {actual_lines} against {expect_lines_from_streams}'
        )
        assert actual_lines.pop() == ''  # last line was terminated
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

    def _verify_output(
        expect_stdout_streams,
        expect_stderr_streams=None,
        expect_log_streams=None,
    ):
        actual = capfd.readouterr()
        _verify_one_output(expect_stdout_streams, actual.out)
        if expect_stderr_streams is not None:
            _verify_one_output(expect_stderr_streams, actual.err)
        if expect_log_streams is not None:
            _, listhandler = logger_with_listhandler
            actual_log = ''.join(msg + '\n' for msg in listhandler.messages)
            _verify_one_output(expect_log_streams, actual_log)
        return True

    return _verify_output
