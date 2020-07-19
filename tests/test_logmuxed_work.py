import asyncio
import contextlib
import functools
import logging
import pytest
import random
from subprocess import PIPE
import sys
import time

from asyncjobs import logmux, logmuxed_work, signal_handling

from conftest import (
    abort_in,
    assert_elapsed_time,
    ListHandler,
    mock_argv,
    TExternalWorkJob,
    verified_events,
    verify_number_of_tasks,
    verify_tasks,
)

pytestmark = pytest.mark.asyncio

logger = logging.getLogger(__name__)


def shuffled_prints(out_f, err_f, out_strings, err_strings):
    """Print out_strings -> out_f and err_strings -> err_f in shuffled order.

    Return a stream of callables that together prints out_strings -> out_f and
    err_strings -> err_f such that each is processed in order (out_strings is
    printed in order to out_f, and err_strings is printed in order to err_f),
    but the out_f and err_f prints are interleaved in a random order. E.g.
    this call:

        shuffled_prints(sys.stdout, sys.stderr, list('foo'), list('bar'))

    might result in the following (un-called) prints being yielded:

        print('b', file=sys.stderr, end='')
        print('f', file=sys.stdout, end='')
        print('o', file=sys.stdout, end='')
        print('a', file=sys.stderr, end='')
        print('o', file=sys.stdout, end='')
        print('r', file=sys.stderr, end='')

    This helper is an attempt to provoke any issues we might have regarding
    rescheduling/ordering of tasks and internal handling of file descriptors.
    """

    def prints(f, items):
        """Yield callables that print each item of items to f, in order."""
        for item in items:
            yield functools.partial(print, item, file=f, end='')

    def random_interleave(*iterators):
        """Yield from each iterator in random order until all are exhausted."""
        iterators = list(iterators)
        while iterators:
            i = random.randrange(len(iterators))
            try:
                yield next(iterators[i])
            except StopIteration:  # iterators[i] is exhausted
                del iterators[i]

    out_prints = prints(out_f, out_strings)
    err_prints = prints(err_f, err_strings)
    yield from random_interleave(out_prints, err_prints)


def decorators(job_name):
    """Return pair of out/err decorators that prefix job_name onto lines."""
    return (
        lambda line: f'{job_name}/out: {line}',
        lambda line: f'{job_name}/ERR: {line}',
    )


class TJob(TExternalWorkJob):
    def __init__(
        self,
        *args,
        mode=None,
        out=None,
        err=None,
        log=None,
        decorate=True,
        log_handler=False,
        extras=None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        Modes = {
            'async': self.do_async,
            'thread': self.do_thread,
            'mock_argv': self.do_mock_argv,
            'custom': self.coro,
        }
        if mode is None:
            mode = 'async' if self.coro is None else 'custom'
        self.coro = Modes[mode]

        self.out = f"This is {self.name}'s stdout\n" if out is None else out
        self.err = f"This is {self.name}'s stderr\n" if err is None else err
        self.log = f"This is {self.name}'s log" if log is None else log
        self.decorate = decorate
        self.log_handler = log_handler
        self.extras = extras

        # We expect any log messages to ctx.stderr in the following cases:
        # - The default log handler (log_handler=True) streams to ctx.stderr.
        # - subprocesses log to stderr which is always == ctx.stderr.
        # - When mode == 'custom' we _assume_ the user's coroutine arranges
        #   for logging to ctx.stderr.
        self.logs_to_stderr = log_handler is True or mode in {
            'mock_argv',
            'custom',
        }

    async def __call__(self, ctx):
        # Limit log handlers to ERROR messages only, so that we can ignore
        # less critical log records in the below tests
        old_factory = ctx.log_handler_factory

        def only_errors(ctx):
            handler = old_factory(ctx)
            handler.setLevel(logging.ERROR)
            return handler

        ctx.log_handler_factory = only_errors

        if self.decorate:
            dec_out, dec_err = decorators(self.name)
        else:
            dec_out, dec_err = None, None
        async with ctx.redirect(
            decorate_out=dec_out,
            decorate_err=dec_err,
            log_handler=self.log_handler,
        ):
            return await super().__call__(ctx)

    def xout(self):
        """Return expected stdout data from this job."""
        dec = decorators(self.name)[0] if self.decorate else lambda s: s
        if isinstance(self.out, list):
            return [dec(line + '\n').rstrip() for line in self.out]
        else:
            return [dec(self.out).rstrip()]

    def xerr(self):
        """Return expected stderr data from this job."""
        dec = decorators(self.name)[1] if self.decorate else lambda s: s
        logs = [dec(self.log)] if self.log and self.logs_to_stderr else []
        if isinstance(self.err, list):
            return [dec(line + '\n').rstrip() for line in self.err] + logs
        else:
            return [dec(self.err).rstrip()] + logs

    def shuffled_out_err(self, out_f, err_f):
        if isinstance(self.out, list):  # lines from list of strings
            out = (line + '\n' for line in self.out)
        else:  # characters from string
            out = (char for char in self.out)
        if isinstance(self.err, list):  # lines from list of strings
            err = (line + '\n' for line in self.err)
        else:  # characters from string
            err = (char for char in self.err)
        return shuffled_prints(out_f, err_f, out, err)

    def mock_argv(self, extra_args=None):
        """Return a suitable mock_argv for doing this job in a subprocess."""
        assert self.out.endswith('\n')  # newline-terminated string
        assert self.err.endswith('\n')  # newline-terminated string
        args = ['out:', self.out.rstrip()]
        for arg in extra_args or []:
            if not arg.startswith('exit:'):
                args.append(arg)
        args += ['err:', self.err.rstrip()]
        if self.log:
            args.append(f'log:{self.log}')
        for arg in extra_args or []:
            if arg.startswith('exit:'):
                args.append(arg)
        return mock_argv(*args)

    async def do_async(self, ctx):
        for print_one in self.shuffled_out_err(ctx.stdout, ctx.stderr):
            print_one()
            await asyncio.sleep(0)
        if self.log:
            logger.error(self.log)

    async def do_thread(self, ctx):
        def in_thread(ctx):
            for print_one in self.shuffled_out_err(ctx.stdout, ctx.stderr):
                print_one()
                time.sleep(0.001)
            if self.log:
                logger.error(self.log)

        return await self.run_thread(
            in_thread, ctx, log_handler=self.log_handler
        )

    async def do_mock_argv(self, ctx):
        return await self.run_subprocess(self.mock_argv(self.extras), ctx)


@pytest.fixture
def Scheduler(scheduler_with_workers):
    return scheduler_with_workers(
        signal_handling.Scheduler, logmuxed_work.Scheduler
    )


@pytest.fixture
def run(Scheduler):
    async def _run(todo, abort_after=None, check_events=True, **kwargs):
        scheduler = Scheduler(**kwargs)
        if check_events:
            cm = verified_events(scheduler, todo)
        else:
            cm = contextlib.nullcontext()
        with cm:
            for job in todo:
                scheduler.add_job(job.name, job, getattr(job, 'deps', None))
            try:
                async with abort_in(abort_after):
                    return await scheduler.run()
            finally:
                verify_number_of_tasks(1)  # no other tasks than test itself

    return _run


# no output


async def test_no_output_from_no_jobs(run, verify_output):
    await run([])
    assert verify_output([], [])


async def test_no_output_from_two_jobs(run, verify_output):
    await run([TJob('foo', out='', err=''), TJob('bar', out='', err='')])
    assert verify_output([[], []], [[], []])


# undecorated output


async def test_undecorated_charwise_output_from_two_jobs(run, verify_output):
    todo = [
        TJob(
            name,
            out=f"char by char to {name}'s stdout\n",
            err=f"char by char to {name}'s stderr\n",
            decorate=False,
        )
        for name in ['foo', 'bar']
    ]
    await run(todo)
    assert verify_output(
        [job.xout() for job in todo], [job.xerr() for job in todo],
    )


async def test_undecorated_linewise_output_from_two_jobs(run, verify_output):
    todo = [
        TJob(
            name,
            out=['line', 'by', 'line', 'to', f"{name}'s", 'stdout'],
            err=['line', 'by', 'line', 'to', f"{name}'s", 'stderr'],
            decorate=False,
        )
        for name in ['foo', 'bar']
    ]
    await run(todo)
    assert verify_output(
        [job.xout() for job in todo], [job.xerr() for job in todo],
    )


async def test_undecorated_output_with_log_from_two_jobs(run, verify_output):
    todo = [
        TJob(name, decorate=False, log_handler=True) for name in ['foo', 'bar']
    ]
    await run(todo)
    assert verify_output(
        [job.xout() for job in todo], [job.xerr() for job in todo],
    )


# decorated output


async def test_decorated_charwise_output_from_two_jobs(run, verify_output):
    todo = [
        TJob(
            name,
            out=f"char by char to {name}'s stdout\n",
            err=f"char by char to {name}'s stderr\n",
        )
        for name in ['foo', 'bar']
    ]
    await run(todo)
    assert verify_output(
        [job.xout() for job in todo], [job.xerr() for job in todo],
    )


async def test_decorated_linewise_output_from_two_jobs(run, verify_output):
    todo = [
        TJob(
            name,
            out=['line', 'by', 'line', 'to', f"{name}'s", 'stdout'],
            err=['line', 'by', 'line', 'to', f"{name}'s", 'stderr'],
        )
        for name in ['foo', 'bar']
    ]
    await run(todo)
    assert verify_output(
        [job.xout() for job in todo], [job.xerr() for job in todo],
    )


async def test_decorated_output_with_log_from_two_jobs(run, verify_output):
    todo = [TJob(name, log_handler=True) for name in ['foo', 'bar']]
    await run(todo)
    assert verify_output(
        [job.xout() for job in todo], [job.xerr() for job in todo],
    )


async def test_decorated_output_via_custom_muxes(run, verify_output):
    todo = [TJob(name, log_handler=True) for name in ['foo', 'bar']]
    to_stdout = logmux.LogMux(sys.stdout)
    to_stderr = logmux.LogMux(sys.stderr)
    await run(todo, outmux=to_stderr, errmux=to_stdout)  # flip stderr/stdout
    assert verify_output(
        [job.xerr() for job in todo], [job.xout() for job in todo],  # flipped
    )


async def test_decorated_output_with_custom_log_handler(run, verify_output):
    test_handler = ListHandler(level=logging.ERROR)
    todo = [TJob(name, log_handler=test_handler) for name in ['foo', 'bar']]
    await run(todo)
    assert verify_output(
        [job.xout() for job in todo], [job.xerr() for job in todo],
    )
    assert sorted(test_handler.messages()) == sorted(job.log for job in todo)


async def test_decorated_output_from_spawned_jobs(run, verify_output):
    baz = TJob('baz', log_handler=True)
    bar = TJob('bar', log_handler=True, spawn=[baz])
    foo = TJob('foo', log_handler=True, spawn=[bar])
    await run([foo])
    assert verify_output(
        [job.xout() for job in [foo, bar, baz]],
        [job.xerr() for job in [foo, bar, baz]],
    )


async def test_decorated_output_from_spawned_w_custom_log(run, verify_output):
    foo_handler = ListHandler(level=logging.ERROR)
    bar_handler = ListHandler(level=logging.ERROR)
    baz_handler = ListHandler(level=logging.ERROR)
    baz = TJob('baz', log_handler=baz_handler)
    bar = TJob('bar', log_handler=bar_handler, spawn=[baz])
    foo = TJob('foo', log_handler=foo_handler, spawn=[bar])
    await run([foo])
    assert verify_output(
        [job.xout() for job in [foo, bar, baz]],
        [job.xerr() for job in [foo, bar, baz]],
    )
    assert list(foo_handler.messages()) == [foo.log]
    assert list(bar_handler.messages()) == [bar.log]
    assert list(baz_handler.messages()) == [baz.log]


# decorated output from threads


async def test_decorated_output_from_thread(run, verify_output):
    todo = [TJob(name, mode='thread') for name in ['foo', 'bar']]
    await run(todo)
    assert verify_output(
        [job.xout() for job in todo], [job.xerr() for job in todo],
    )


async def test_decorated_output_with_log_from_thread(run, verify_output):
    todo = [
        TJob(name, mode='thread', log_handler=True) for name in ['foo', 'bar']
    ]
    await run(todo)
    assert verify_output(
        [job.xout() for job in todo], [job.xerr() for job in todo],
    )


async def test_decorated_output_from_thread_w_custom_log(run, verify_output):
    test_handler = ListHandler(level=logging.ERROR)
    todo = [
        TJob(name, mode='thread', log_handler=test_handler)
        for name in ['foo', 'bar']
    ]
    await run(todo)
    assert verify_output(
        [job.xout() for job in todo], [job.xerr() for job in todo],
    )
    assert sorted(test_handler.messages()) == sorted(job.log for job in todo)


# decorated output from subprocesses


async def test_decorated_output_from_subprocess_worker(run, verify_output):
    todo = [TJob(name, mode='mock_argv') for name in ['foo', 'bar']]
    await run(todo)
    assert verify_output(
        [job.xout() for job in todo], [job.xerr() for job in todo],
    )


async def test_decorated_output_from_aborted_processes(
    num_workers, run, verify_output
):
    todo = [
        TJob(name, mode='mock_argv', extras=['sleep:5'])
        for name in ['foo', 'bar', 'baz']
    ]
    with assert_elapsed_time(lambda t: t < 0.75):
        await run(todo, abort_after=0.5)

    # We have 3 jobs, but can only run as many concurrently as there are
    # workers available. The rest will be cancelled before they start.
    assert verify_output(
        [job.xout() for job in todo][:num_workers],
        [],  # No stderr output as this happens _after_ the aborted sleep
    )


async def test_decorated_output_from_subprocess_context(run, verify_output):
    async def coro(ctx):
        argv = ctx.tjob.mock_argv()
        with ctx.tjob.subprocess_xevents(argv, result=0):
            async with ctx.subprocess(argv) as proc:
                await proc.wait()

    todo = [TJob(name, coro=coro) for name in ['foo', 'bar']]
    await run(todo)
    assert verify_output(
        [job.xout() for job in todo], [job.xerr() for job in todo],
    )


async def test_subproc_decorate_stderr_capture_stdout(run, verify_output):
    async def coro(ctx):
        argv = ctx.tjob.mock_argv()
        with ctx.tjob.subprocess_xevents(argv, result=0):
            async with ctx.subprocess(argv, stdout=PIPE) as proc:
                output = await proc.stdout.read()
                await proc.wait()
                return output

    todo = [TJob(name, coro=coro) for name in ['foo', 'bar']]
    done = await run(todo)
    assert verify_tasks(  # undecorated output was captured and returned
        done, {job.name: job.out.encode('ascii') for job in todo}
    )
    assert verify_output([], [job.xerr() for job in todo])  # output captured


async def test_subproc_capture_stdout_from_terminated_proc(run, verify_output):
    async def coro(ctx):
        argv = ctx.tjob.mock_argv(['sleep:30'])
        with ctx.tjob.subprocess_xevents(argv, result='terminate'):
            async with ctx.subprocess(argv, stdout=PIPE) as proc:
                return await proc.stdout.readline()
                # Skipping await proc.wait() to provoke termination

    todo = [TJob(name, coro=coro) for name in ['foo', 'bar']]
    done = await run(todo)
    assert verify_tasks(  # undecorated output was captured and returned
        done, {job.name: job.out.encode('ascii') for job in todo}
    )
    assert verify_output([], [])  # subproc terminated before print to stderr


# redirected_job() decorator


async def test_redirected_job_with_no_decoration(run, verify_output):
    @logmuxed_work.redirected_job()
    async def job(ctx):
        print('Printing to stdout', file=ctx.stdout)
        print('Printing to stderr', file=ctx.stderr)
        logger.error('Logging to stderr')

    job.name = 'foo'
    await run([job], check_events=False)
    assert verify_output(
        [['Printing to stdout']],
        [['Printing to stderr', 'Logging to stderr']],
    )


async def test_redirected_and_decorated_job_except_logger(run, verify_output):
    dec_out, dec_err = decorators('foo')

    @logmuxed_work.redirected_job(dec_out, dec_err, log_handler=False)
    async def job(ctx):
        print('Printing to stdout', file=ctx.stdout)
        print('Printing to stderr', file=ctx.stderr)
        logger.error('Logging to stderr')

    job.name = 'foo'
    await run([job], check_events=False)
    assert verify_output(
        [['foo/out: Printing to stdout']], [['foo/ERR: Printing to stderr']],
    )


async def test_redirected_and_decorated_job_include_logger(run, verify_output):
    _, dec_err = decorators('foo')

    @logmuxed_work.redirected_job(decorate_err=dec_err)
    async def job(ctx):
        print('Printing to stdout', file=ctx.stdout)
        print('Printing to stderr', file=ctx.stderr)
        logger.error('Logging to stderr')

    job.name = 'foo'
    await run([job], check_events=False)
    assert verify_output(
        [['Printing to stdout']],
        [['foo/ERR: Printing to stderr', 'foo/ERR: Logging to stderr']],
    )


async def test_redirected_job_with_decorated_subproc(run, verify_output):
    dec_out, dec_err = decorators('foo')
    argv = mock_argv(
        'Printing to stdout', 'err:', 'Printing to stderr', 'log:Logging!'
    )

    @logmuxed_work.redirected_job(dec_out, dec_err, log_handler=False)
    async def job(ctx):
        async with ctx.subprocess(argv) as proc:
            await proc.wait()

    job.name = 'foo'
    await run([job], check_events=False)
    assert verify_output(
        [['foo/out: Printing to stdout']],
        [['foo/ERR: Printing to stderr', 'foo/ERR: Logging!']],
    )


async def test_redirected_job_with_subproc_output_capture(run, verify_output):
    dec_out, dec_err = decorators('foo')
    argv = mock_argv(
        'Printing to stdout', 'err:', 'Printing to stderr', 'log:Logging!'
    )

    @logmuxed_work.redirected_job(dec_out, dec_err, log_handler=False)
    async def job(ctx):
        async with ctx.subprocess(argv, stdout=PIPE) as proc:
            output = await proc.stdout.read()
            await proc.wait()
            return output

    job.name = 'foo'
    done = await run([job], check_events=False)
    assert verify_tasks(done, {'foo': b'Printing to stdout\n'})  # undecorated
    assert verify_output(
        [[]], [['foo/ERR: Printing to stderr', 'foo/ERR: Logging!']]
    )


# stress-testing the logmux framework


async def test_decorated_output_from_many_jobs(num_jobs, run, verify_output):
    todo = [TJob(f'job #{i}') for i in range(num_jobs)]
    await run(todo)
    assert verify_output(
        [job.xout() for job in todo], [job.xerr() for job in todo],
    )


async def test_decorated_output_from_many_spawned_jobs(
    num_jobs, run, verify_output
):
    todo = TJob(
        'foo',
        out='',
        err='',
        spawn=[TJob(f'job #{i}', log_handler=True) for i in range(num_jobs)],
    )
    await run([todo])
    assert verify_output(
        [job.xout() for job in todo.spawn], [job.xerr() for job in todo.spawn],
    )


async def test_decorated_output_from_many_thread_workers(
    num_jobs, run, verify_output
):
    todo = [TJob(f'job #{i}', mode='thread') for i in range(num_jobs)]
    await run(todo)
    assert verify_output(
        [job.xout() for job in todo], [job.xerr() for job in todo],
    )


async def test_decorated_output_from_many_subprocesses(
    num_jobs, run, verify_output
):
    todo = [TJob(f'job #{i}', mode='mock_argv') for i in range(num_jobs)]
    await run(todo)
    assert verify_output(
        [job.xout() for job in todo], [job.xerr() for job in todo],
    )
