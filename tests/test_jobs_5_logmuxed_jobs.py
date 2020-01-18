import asyncio
from contextlib import asynccontextmanager
from functools import partial
import logging
import pytest
import random
import sys
import time

from jobs import external_work
from jobs.logmux import LogMux

from conftest import setup_scheduler, TExternalWorkJob

pytestmark = pytest.mark.asyncio

logger = logging.getLogger(__name__)


@pytest.fixture(params=[1, 2, 4, 100])
def scheduler_cls(request):
    logger.info(f'creating scheduler with {request.param} worker threads')
    yield partial(external_work.Scheduler, workers=request.param)


class TJob(TExternalWorkJob):
    outmux = None
    errmux = None

    def __init__(
        self,
        *args,
        out=None,
        err=None,
        in_thread=False,
        redirect=False,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        # out/err are strings or lists of strings. If strings, the chars in the
        # string will be printed one-by-one, if lists of strings, each string
        # will be printed. See .print_out_and_err() below for details.
        self.out = out
        self.err = err
        self.redirect = redirect
        self.stdout = None
        self.stderr = None
        if in_thread:
            self.thread = self.print_out_and_err(True)
        else:
            self.do_work = self.print_out_and_err(False)

    def print_out_and_err(self, sync):
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

    @asynccontextmanager
    async def setup_stdouterr(self, redirect_logger=False):
        if not self.redirect:  # do nothing
            self.stdout = sys.stdout
            self.stderr = sys.stderr
            try:
                yield
            finally:
                self.stdout = None
                self.stderr = None
            return

        def decorate_out(line):
            return f'{self.name}/out: {line.rstrip()}\n'

        def decorate_err(line):
            return f'{self.name}/ERR: {line.rstrip()}\n'

        async with self.outmux.new_stream(decorate_out) as outf:
            async with self.errmux.new_stream(decorate_err) as errf:
                self.stdout = outf
                self.stderr = errf
                if redirect_logger:
                    log_handler = logging.StreamHandler(self.stderr)
                    self.logger.addHandler(log_handler)
                try:
                    yield
                finally:
                    if redirect_logger:
                        self.logger.removeHandler(log_handler)
                    self.stdout = None
                    self.stderr = None

    async def run_in_subprocess(self, argv, **kwargs):
        if kwargs.get('stdout') is None:
            kwargs['stdout'] = self.stdout
        if kwargs.get('stderr') is None:
            kwargs['stderr'] = self.stderr
        return await super().run_in_subprocess(argv, **kwargs)

    async def __call__(self, scheduler):
        async with self.setup_stdouterr():
            return await super().__call__(scheduler)


@pytest.fixture
def run(scheduler_cls):
    async def _run(todo, **run_args):
        async with LogMux(sys.stdout) as outmux:
            async with LogMux(sys.stderr) as errmux:
                TJob.outmux = outmux
                TJob.errmux = errmux
                with setup_scheduler(scheduler_cls, todo) as scheduler:
                    result = await scheduler.run(**run_args)
        return result

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
            redirect=True,
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
        redirect=True,
        spawn=[
            TJob(
                'bar',
                out="This is bar's stdout",
                err="This is bar's stderr",
                redirect=True,
                spawn=[
                    TJob(
                        'baz',
                        out="This is baz's stdout",
                        err="This is baz's stderr",
                        redirect=True,
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
            redirect=True,
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
