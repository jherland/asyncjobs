import asyncio
import contextlib
import functools
from pathlib import Path
import pytest
import random
import time

from asyncjobs import logmuxed_work, signal_handling

from conftest import (
    abort_in,
    assert_elapsed_time_within,
    TExternalWorkJob,
    verified_events,
)

pytestmark = pytest.mark.asyncio

sh_helper = Path(__file__).parent / 'test_5_logmuxed_work_helper.sh'


def shuffled_prints(out_f, err_f, out_str, err_str):
    """Print out_str -> out_f and err_str -> err_f in shuffled order.

    Return a stream of callables that together prints out_str -> out_f and
    err_str -> err_f such that the each char is printed in order, but the
    out_f and err_f prints are interleaved in a random order. E.g. this call:

        shuffled_prints(sys.stdout, sys.stderr, 'foo', 'bar')

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

    def prints(f, s):
        """Yield callables that print each char of s to f, in order."""
        for c in s:
            yield functools.partial(print, c, file=f, end='')

    def random_interleave(*iterators):
        """Yield from each iterator in random order until all are exhausted."""
        iterators = list(iterators)
        while iterators:
            i = random.randrange(len(iterators))
            try:
                yield next(iterators[i])
            except StopIteration:  # iterators[i] is exhausted
                del iterators[i]

    out_prints = prints(out_f, out_str)
    err_prints = prints(err_f, err_str)
    yield from random_interleave(out_prints, err_prints)


def print_out_err(out, err, *, sync):
    # out/err are strings or lists of strings. If strings, the chars in the
    # string will be printed one-by-one, if lists of strings, each string
    # will be printed with a trailing newline.
    if isinstance(out, list):  # list of strings
        out = [line + '\n' for line in out]
    if isinstance(err, list):  # list of strings
        err = [line + '\n' for line in err]

    async def print_out_err_async(ctx):
        for print_one in shuffled_prints(ctx.stdout, ctx.stderr, out, err):
            print_one()
            await asyncio.sleep(0)

    def print_out_err_sync(ctx):
        for print_one in shuffled_prints(ctx.stdout, ctx.stderr, out, err):
            print_one()
            time.sleep(0.001)

    return print_out_err_sync if sync else print_out_err_async


def decorators(job_name):
    return (
        lambda msg: f'{job_name}/out: {msg.rstrip()}\n',
        lambda msg: f'{job_name}/ERR: {msg.rstrip()}\n',
    )


class TJob(TExternalWorkJob):
    def __init__(
        self,
        *args,
        out='',
        err='',
        in_thread=False,
        decorate=False,
        redirect_logger=False,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        if in_thread:
            self.thread = print_out_err(out, err, sync=True)
        elif 'subproc' not in kwargs:
            self.do_work = print_out_err(out, err, sync=False)
        self.redirect_logger = redirect_logger
        self.decorate_out, self.decorate_err = None, None
        if decorate:
            self.decorate_out, self.decorate_err = decorators(self.name)

    async def __call__(self, ctx):
        async with ctx.setup_redirection(
            decorate_out=self.decorate_out,
            decorate_err=self.decorate_err,
            redirect_logger=self.redirect_logger,
        ):
            return await super().__call__(ctx)


@pytest.fixture
def Scheduler(scheduler_with_workers):
    return scheduler_with_workers(
        signal_handling.Scheduler, logmuxed_work.Scheduler
    )


@pytest.fixture
def run(Scheduler):
    async def _run(todo, abort_after=None, check_events=True):
        scheduler = Scheduler()
        if check_events:
            cm = verified_events(scheduler, todo)
        else:
            cm = contextlib.nullcontext()
        with cm:
            for job in todo:
                scheduler.add_job(job.name, job, getattr(job, 'deps', None))
            with abort_in(abort_after):
                return await scheduler.run()

    return _run


# no output


async def test_no_output_from_no_jobs(run, verify_output):
    await run([])
    assert verify_output([], [])


async def test_no_output_from_two_jobs(run, verify_output):
    await run([TJob('foo'), TJob('bar')])
    assert verify_output([[], []], [[], []])


# undecorated output


async def test_default_output_is_undecorated(run, verify_output):
    jobs = ['foo', 'bar']
    out = "This is {name}'s stdout"
    err = "This is {name}'s stderr"

    todo = [
        # Pass out/err as lists of strings to print entire strings at once
        TJob(name, out=[out.format(name=name)], err=[err.format(name=name)])
        for name in jobs
    ]
    await run(todo)
    assert verify_output(
        [[out.format(name=name)] for name in jobs],
        [[err.format(name=name)] for name in jobs],
    )


# decorated output


async def test_decorated_output_from_two_jobs(run, verify_output):
    jobs = ['foo', 'bar']
    out = "This is {name}'s stdout"
    err = "This is {name}'s stderr"

    todo = [
        TJob(
            name,
            out=out.format(name=name),
            err=err.format(name=name),
            decorate=True,
        )
        for name in jobs
    ]
    await run(todo)

    outprefix = '{name}/out: '
    errprefix = '{name}/ERR: '
    assert verify_output(
        [[(outprefix + out).format(name=name)] for name in jobs],
        [[(errprefix + err).format(name=name)] for name in jobs],
    )


async def test_decorated_output_from_spawned_jobs(run, verify_output):
    foo = TJob(
        'foo',
        out="This is foo's stdout",
        err="This is foo's stderr",
        decorate=True,
        spawn=[
            TJob(
                'bar',
                out="This is bar's stdout",
                err="This is bar's stderr",
                decorate=True,
                spawn=[
                    TJob(
                        'baz',
                        out="This is baz's stdout",
                        err="This is baz's stderr",
                        decorate=True,
                    )
                ],
            )
        ],
    )
    await run([foo])
    assert verify_output(
        [
            ["foo/out: This is foo's stdout"],
            ["bar/out: This is bar's stdout"],
            ["baz/out: This is baz's stdout"],
        ],
        [
            ["foo/ERR: This is foo's stderr"],
            ["bar/ERR: This is bar's stderr"],
            ["baz/ERR: This is baz's stderr"],
        ],
    )


async def test_decorated_output_from_thread_worker(run, verify_output):
    jobs = ['foo', 'bar']
    out = "This is {name}'s stdout"
    err = "This is {name}'s stderr"

    todo = [
        TJob(
            name,
            out=out.format(name=name),
            err=err.format(name=name),
            decorate=True,
            in_thread=True,
        )
        for name in jobs
    ]
    await run(todo)

    outprefix = '{name}/out: '
    errprefix = '{name}/ERR: '
    assert verify_output(
        [[(outprefix + out).format(name=name)] for name in jobs],
        [[(errprefix + err).format(name=name)] for name in jobs],
    )


async def test_decorated_output_from_subprocess_worker(run, verify_output):
    assert sh_helper.is_file()
    jobs = ['foo', 'bar']
    out = "This is {name}'s stdout"
    err = "This is {name}'s stderr"

    todo = [
        TJob(
            name,
            decorate=True,
            subproc=[
                str(sh_helper),
                out.format(name=name),
                err.format(name=name),
            ],
        )
        for name in jobs
    ]
    await run(todo)

    outprefix = '{name}/out: '
    errprefix = '{name}/ERR: '
    assert verify_output(
        [[(outprefix + out).format(name=name)] for name in jobs],
        [[(errprefix + err).format(name=name)] for name in jobs],
    )


async def test_decorated_output_from_aborted_processes(
    num_workers, run, verify_output
):
    assert sh_helper.is_file()
    jobs = ['foo', 'bar', 'baz']
    out = "This is {name}'s stdout"
    err = "This is {name}'s stderr"

    todo = [
        TJob(
            name,
            decorate=True,
            subproc=[
                str(sh_helper),
                out.format(name=name),
                err.format(name=name),
                '1',  # sleep 1 sec between stdout print and stderr print
            ],
        )
        for name in jobs
    ]
    with assert_elapsed_time_within(0.75):
        await run(todo, abort_after=0.5)

    # We have 3 jobs, but can only run as many concurrently as there are
    # workers available. The rest will be cancelled before they start their
    # subprocess
    outprefix = '{name}/out: '
    expect_out = [[(outprefix + out).format(name=name)] for name in jobs]
    assert verify_output(expect_out[:num_workers], [])


# redirected_job() decorator


async def test_redirected_job_no_decoration(run, verify_output):
    @logmuxed_work.redirected_job()
    async def job(ctx):
        print('Printing to stdout', file=ctx.stdout)
        print('Printing to stderr', file=ctx.stderr)
        ctx.logger.info('Logging to stderr')

    job.name = 'foo'
    await run([job], check_events=False)
    assert verify_output(
        [['Printing to stdout']],
        [['Printing to stderr', 'Logging to stderr']],
    )


async def test_redirected_job_decorate_without_logger(run, verify_output):
    dec_out, dec_err = decorators('foo')

    @logmuxed_work.redirected_job(dec_out, dec_err, False)
    async def job(ctx):
        print('Printing to stdout', file=ctx.stdout)
        print('Printing to stderr', file=ctx.stderr)
        ctx.logger.info('Logging to stderr')

    job.name = 'foo'
    await run([job], check_events=False)
    assert verify_output(
        [['foo/out: Printing to stdout']], [['foo/ERR: Printing to stderr']],
    )


async def test_redirected_job_decorate_err_with_logger(run, verify_output):
    _, dec_err = decorators('foo')

    @logmuxed_work.redirected_job(decorate_err=dec_err)
    async def job(ctx):
        print('Printing to stdout', file=ctx.stdout)
        print('Printing to stderr', file=ctx.stderr)
        ctx.logger.info('Logging to stderr')

    job.name = 'foo'
    await run([job], check_events=False)
    assert verify_output(
        [['Printing to stdout']],
        [['foo/ERR: Printing to stderr', 'foo/ERR: Logging to stderr']],
    )


# stress-testing the logmux framework


async def test_decorated_output_from_100_jobs(run, verify_output):
    out = "This is {i}'s stdout"
    err = "This is {i}'s stderr"

    todo = [
        TJob(
            f'job #{i}',
            out=out.format(i=i),
            err=err.format(i=i),
            decorate=True,
        )
        for i in range(100)
    ]
    await run(todo)

    outprefix = 'job #{i}/out: '
    errprefix = 'job #{i}/ERR: '
    assert verify_output(
        [[(outprefix + out).format(i=i)] for i in range(100)],
        [[(errprefix + err).format(i=i)] for i in range(100)],
    )


async def test_decorated_output_from_100_spawned_jobs(run, verify_output):
    out = "This is {i}'s stdout"
    err = "This is {i}'s stderr"

    todo = TJob(
        'foo',
        spawn=[
            TJob(
                f'job #{i}',
                out=out.format(i=i),
                err=err.format(i=i),
                decorate=True,
            )
            for i in range(100)
        ],
    )
    await run([todo])

    outprefix = 'job #{i}/out: '
    errprefix = 'job #{i}/ERR: '
    assert verify_output(
        [[(outprefix + out).format(i=i)] for i in range(100)],
        [[(errprefix + err).format(i=i)] for i in range(100)],
    )


async def test_decorated_output_from_100_thread_workers(run, verify_output):
    out = "This is {i}'s stdout"
    err = "This is {i}'s stderr"

    todo = [
        TJob(
            f'job #{i}',
            out=out.format(i=i),
            err=err.format(i=i),
            decorate=True,
            in_thread=True,
        )
        for i in range(100)
    ]
    await run(todo)

    outprefix = 'job #{i}/out: '
    errprefix = 'job #{i}/ERR: '
    assert verify_output(
        [[(outprefix + out).format(i=i)] for i in range(100)],
        [[(errprefix + err).format(i=i)] for i in range(100)],
    )


async def test_decorated_output_from_100_subprocesses(run, verify_output):
    assert sh_helper.is_file()
    out = "This is {i}'s stdout"
    err = "This is {i}'s stderr"

    todo = [
        TJob(
            f'job #{i}',
            decorate=True,
            subproc=[str(sh_helper), out.format(i=i), err.format(i=i)],
        )
        for i in range(100)
    ]
    await run(todo)

    outprefix = 'job #{i}/out: '
    errprefix = 'job #{i}/ERR: '
    assert verify_output(
        [[(outprefix + out).format(i=i)] for i in range(100)],
        [[(errprefix + err).format(i=i)] for i in range(100)],
    )
