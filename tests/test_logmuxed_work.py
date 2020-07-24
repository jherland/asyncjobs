import asyncio
import contextlib
import functools
import logging
import pytest
import random
import resource
from subprocess import PIPE
import sys
import time

from asyncjobs import logcontext, logmux, logmuxed_work, signal_handling

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
testlogger = logging.getLogger('test')


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


class TJob(TExternalWorkJob):
    def __init__(
        self,
        *args,
        mode=None,
        out=None,
        err=None,
        log=None,
        decorate=True,
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
        self.extras = extras

    async def __call__(self, ctx):
        with ctx.redirect(
            decorate_out=f'{self.name}/out: ' if self.decorate else None,
            decorate_err=f'{self.name}/ERR: ' if self.decorate else None,
            decorate_log=f'{self.name}/log: ' if self.decorate else None,
        ):
            return await super().__call__(ctx)

    def xout(self):
        """Return expected stdout data from this job."""
        prefix = f'{self.name}/out: ' if self.decorate else ''
        lines = self.out if isinstance(self.out, list) else [self.out]
        return [prefix + line.rstrip() for line in lines]

    def xerr(self, include_log=False):
        """Return expected stderr data from this job."""
        prefix = f'{self.name}/ERR: ' if self.decorate else ''
        lines = self.err if isinstance(self.err, list) else [self.err]
        if include_log and self.log:
            lines += [self.log]
        return [prefix + line.rstrip() for line in lines]

    def xlog(self):
        """Return expected log messages from this job."""
        prefix = f'{self.name}/log: ' if self.decorate else ''
        lines = self.log if isinstance(self.log, list) else [self.log]
        return [prefix + line.rstrip() for line in lines]

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
        with ctx.stdout as outf, ctx.stderr as errf:
            for print_one in self.shuffled_out_err(outf, errf):
                print_one()
                await asyncio.sleep(0)
            if self.log:
                testlogger.error(self.log)

    async def do_thread(self, ctx):
        def in_thread(outf, errf, *_):
            for print_one in self.shuffled_out_err(outf, errf):
                print_one()
                time.sleep(0.001)
            if self.log:
                testlogger.error(self.log)

        with ctx.stdout as outf, ctx.stderr as errf:
            return await self.run_thread(
                functools.partial(in_thread, outf, errf), ctx
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
    assert verify_output([], [], [])


async def test_no_output_from_two_jobs(run, verify_output):
    await run([TJob(name, out='', err='', log='') for name in ['foo', 'bar']])
    assert verify_output([], [], [])


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
        [job.xout() for job in todo],
        [job.xerr() for job in todo],
        [job.xlog() for job in todo],
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
        [job.xout() for job in todo],
        [job.xerr() for job in todo],
        [job.xlog() for job in todo],
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
        [job.xout() for job in todo],
        [job.xerr() for job in todo],
        [job.xlog() for job in todo],
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
        [job.xout() for job in todo],
        [job.xerr() for job in todo],
        [job.xlog() for job in todo],
    )


async def test_decorated_output_via_custom_muxes(run, verify_output):
    todo = [TJob(name) for name in ['foo', 'bar']]
    to_stdout = logmux.LogMux(sys.stdout)
    to_stderr = logmux.LogMux(sys.stderr)
    await run(todo, outmux=to_stderr, errmux=to_stdout)  # flip stderr/stdout
    assert verify_output(
        [job.xerr() for job in todo],  # flipped
        [job.xout() for job in todo],  # flipped
        [job.xlog() for job in todo],
    )


async def test_decorated_output_with_custom_log_handler(run, verify_output):
    handler = ListHandler()
    handler.setFormatter(logging.Formatter('<%(message)s>'))
    testlogger.addHandler(handler)
    todo = [TJob(name) for name in ['foo', 'bar']]
    await run(todo)
    assert verify_output(
        [job.xout() for job in todo],
        [job.xerr() for job in todo],
        [job.xlog() for job in todo],
    )
    assert sorted(handler.messages) == sorted(f'<{job.log}>' for job in todo)


async def test_decorated_output_with_context_formatter(run, verify_output):
    handler = ListHandler()
    handler.setFormatter(logcontext.Formatter('<%(message)s>'))
    testlogger.addHandler(handler)
    todo = [TJob(name) for name in ['foo', 'bar']]
    await run(todo)
    assert verify_output(
        [job.xout() for job in todo],
        [job.xerr() for job in todo],
        [job.xlog() for job in todo],
    )
    assert sorted(handler.messages) == sorted(
        f'{job.name}/log: <{job.log}>' for job in todo
    )


async def test_decorated_output_from_spawned_jobs(run, verify_output):
    baz = TJob('baz')
    bar = TJob('bar', spawn=[baz])
    foo = TJob('foo', spawn=[bar])
    await run([foo])
    assert verify_output(
        [job.xout() for job in [foo, bar, baz]],
        [job.xerr() for job in [foo, bar, baz]],
        [job.xlog() for job in [foo, bar, baz]],
    )


# decorated output from threads


async def test_decorated_output_from_thread(run, verify_output):
    todo = [TJob(name, mode='thread') for name in ['foo', 'bar']]
    await run(todo)
    assert verify_output(
        [job.xout() for job in todo],
        [job.xerr() for job in todo],
        [job.xlog() for job in todo],
    )


# decorated output from subprocesses


async def test_decorated_output_from_subprocess_worker(run, verify_output):
    todo = [TJob(name, mode='mock_argv') for name in ['foo', 'bar']]
    await run(todo)
    assert verify_output(
        [job.xout() for job in todo],
        [job.xerr(include_log=True) for job in todo],
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
        [job.xout() for job in todo],
        [job.xerr(include_log=True) for job in todo],
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
    assert verify_output(
        [], [job.xerr(include_log=True) for job in todo],  # output captured
    )


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
        with ctx.stdout as outf, ctx.stderr as errf:
            print('Printing to stdout', file=outf)
            print('Printing to stderr', file=errf)
            testlogger.error('Logging')

    job.name = 'foo'
    await run([job], check_events=False)
    assert verify_output(
        [['Printing to stdout']], [['Printing to stderr']], [['Logging']],
    )


async def test_redirected_and_decorated_job(run, verify_output):
    @logmuxed_work.redirected_job('foo/out: ', 'foo/ERR: ', 'foo/log: ')
    async def job(ctx):
        with ctx.stdout as outf, ctx.stderr as errf:
            print('Printing to stdout', file=outf)
            print('Printing to stderr', file=errf)
            testlogger.error('Logging')

    job.name = 'foo'
    await run([job], check_events=False)
    assert verify_output(
        [['foo/out: Printing to stdout']],
        [['foo/ERR: Printing to stderr']],
        [['foo/log: Logging']],
    )


async def test_redirected_job_only_decorate_stderr_and_log(run, verify_output):
    @logmuxed_work.redirected_job(decorate_err='foo/ERR: ', decorate_log=True)
    async def job(ctx):
        with ctx.stdout as outf, ctx.stderr as errf:
            print('Printing to stdout', file=outf)
            print('Printing to stderr', file=errf)
            testlogger.error('Logging')

    job.name = 'foo'
    await run([job], check_events=False)
    assert verify_output(
        [['Printing to stdout']],
        [['foo/ERR: Printing to stderr']],
        [['foo/ERR: Logging']],
    )


async def test_redirected_job_with_decorated_subprocess(run, verify_output):
    @logmuxed_work.redirected_job('foo/out: ', 'foo/ERR: ', 'foo/log: ')
    async def job(ctx):
        argv = mock_argv(
            'Printing to stdout', 'err:', 'Printing to stderr', 'log:Logging!'
        )
        async with ctx.subprocess(argv) as proc:
            await proc.wait()

    job.name = 'foo'
    await run([job], check_events=False)
    assert verify_output(
        [['foo/out: Printing to stdout']],
        [['foo/ERR: Printing to stderr', 'foo/ERR: Logging!']],
        [],  # log output from subprocess goes to its stderr
    )


async def test_redirected_job_with_subproc_output_capture(run, verify_output):
    @logmuxed_work.redirected_job('foo/out: ', 'foo/ERR: ', 'foo/log: ')
    async def job(ctx):
        argv = mock_argv(
            'Printing to stdout', 'err:', 'Printing to stderr', 'log:Logging!'
        )
        async with ctx.subprocess(argv, stdout=PIPE) as proc:
            output = await proc.stdout.read()
            await proc.wait()
            return output

    job.name = 'foo'
    done = await run([job], check_events=False)
    assert verify_tasks(done, {'foo': b'Printing to stdout\n'})  # undecorated
    assert verify_output(
        [],  # output was captured
        [['foo/ERR: Printing to stderr', 'foo/ERR: Logging!']],
        [],  # log output from subprocess goes to its stderr
    )


# stress-testing the logmux framework


async def test_decorated_output_from_many_jobs(num_jobs, run, verify_output):
    todo = [TJob(f'job #{i}') for i in range(num_jobs)]
    await run(todo)
    assert verify_output(
        [job.xout() for job in todo],
        [job.xerr() for job in todo],
        [job.xlog() for job in todo],
    )


async def test_decorated_output_from_many_spawned_jobs(
    num_jobs, run, verify_output
):
    todo = TJob(
        'foo',
        out='',
        err='',
        log='',
        spawn=[TJob(f'job #{i}') for i in range(num_jobs)],
    )
    await run([todo])
    assert verify_output(
        [job.xout() for job in todo.spawn],
        [job.xerr() for job in todo.spawn],
        [job.xlog() for job in todo.spawn],
    )


async def test_decorated_output_from_many_thread_workers(
    num_jobs, run, verify_output
):
    todo = [TJob(f'job #{i}', mode='thread') for i in range(num_jobs)]
    await run(todo)
    assert verify_output(
        [job.xout() for job in todo],
        [job.xerr() for job in todo],
        [job.xlog() for job in todo],
    )


async def test_decorated_output_from_many_subprocesses(
    num_jobs, run, verify_output
):
    todo = [TJob(f'job #{i}', mode='mock_argv') for i in range(num_jobs)]
    await run(todo)
    assert verify_output(
        [job.xout() for job in todo],
        [job.xerr(include_log=True) for job in todo],
    )


async def test_try_to_provoke_too_many_open_files(run):
    # Each job creates 2 logmuxes. Try to exhaust available file descriptors
    num_jobs = resource.getrlimit(resource.RLIMIT_NOFILE)[0] // 2
    todo = [TJob(f'job #{i}') for i in range(num_jobs)]
    done = await run(todo)
    for task in done.values():
        assert task.done()
        if not task.cancelled():
            e = task.exception()
            assert isinstance(e, OSError), repr(e)
            assert e.args == (24, 'Too many open files'), repr(e)
