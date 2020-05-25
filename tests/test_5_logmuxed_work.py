import asyncio
import logging
from pathlib import Path
import pytest
import random
import time

from asyncjobs import logmuxed_work, signal_handling

from conftest import (
    abort_in,
    assert_elapsed_time_within,
    setup_scheduler,
    TExternalWorkJob,
)

pytestmark = pytest.mark.asyncio

logger = logging.getLogger(__name__)

sh_helper = Path(__file__).parent / 'test_5_logmuxed_work_helper.sh'


class TJob(logmuxed_work.Job, TExternalWorkJob):
    def __init__(
        self,
        *args,
        out=None,
        err=None,
        in_thread=False,
        decorate=False,
        redirect_logger=False,
        **kwargs,
    ):
        super().__init__(*args, redirect_logger=redirect_logger, **kwargs)
        # out/err are strings or lists of strings. If strings, the chars in the
        # string will be printed one-by-one, if lists of strings, each string
        # will be printed. See ._print_out_and_err() below for details.
        self.out = out
        self.err = err
        self.decorate = decorate
        if in_thread:
            self.thread = self._print_out_and_err(True)
        elif 'subproc' not in kwargs:
            self.do_work = self._print_out_and_err(False)

    def decorate_out(self, msg):
        if self.decorate:
            return f'{self.name}/out: {msg.rstrip()}\n'
        else:
            return msg

    def decorate_err(self, msg):
        if self.decorate:
            return f'{self.name}/ERR: {msg.rstrip()}\n'
        else:
            return msg

    def _print_out_and_err(self, sync):
        # Try really hard to provoke any issues we might have regarding
        # rescheduling/ordering of tasks and whatnot: Return a sync/async
        # callable that prints one item (char or string) from either self.out
        # or self.err (to self.stdout/stderr respectively), and then yields/
        # sleeps to allow other things to run before the next item is printed.
        # Keep going until self.out and self.err are exhausted.

        if isinstance(self.out, list):  # list of strings
            out = [line + '\n' for line in self.out]
        else:  # list of chars
            out = [] if self.out is None else list(self.out)
        if isinstance(self.err, list):  # list of strings
            err = [line + '\n' for line in self.err]
        else:  # list of chars
            err = [] if self.err is None else list(self.err)

        def print_one_item():
            if not (out or err):  # Nothing left to do
                return False
            if out and err:  # Choose one at random
                src, dst = random.choice(
                    [(out, self.stdout), (err, self.stderr)]
                )
            elif out:
                src, dst = out, self.stdout
            else:
                src, dst = err, self.stderr
            assert src
            print(src.pop(0), file=dst, end='')
            return True

        if sync:

            def print_sync():
                while True:
                    if not print_one_item():
                        break
                    time.sleep(0.001)

            return print_sync
        else:

            async def print_async(*_):
                while True:
                    if not print_one_item():
                        break
                    await asyncio.sleep(0)

            return print_async


@pytest.fixture
def run(scheduler_with_workers):
    Scheduler = scheduler_with_workers(
        signal_handling.Scheduler, logmuxed_work.Scheduler
    )

    async def _run(todo, abort_after=None, **run_args):
        with setup_scheduler(Scheduler, todo) as scheduler:
            with abort_in(abort_after):
                return await scheduler.run(**run_args)

    return _run


async def test_no_output_from_no_jobs(run, verify_output):
    await run([])
    assert verify_output([], [])


async def test_no_output_from_two_jobs(run, verify_output):
    await run([TJob('foo'), TJob('bar')])
    assert verify_output([[], []], [[], []])


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
    # subprocee
    outprefix = '{name}/out: '
    expect_out = [[(outprefix + out).format(name=name)] for name in jobs]
    assert verify_output(expect_out[:num_workers], [])


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
