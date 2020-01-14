from contextlib import asynccontextmanager
from logging import StreamHandler
import pytest
import sys

from jobs import LogMux, Scheduler

from conftest import setup_scheduler, TSimpleJob

pytestmark = pytest.mark.asyncio


class TJob(TSimpleJob):
    outmux = None
    errmux = None

    def __init__(self, *args, out=None, err=None, redirect=False, **kwargs):
        super().__init__(*args, **kwargs)
        self.out = out
        self.err = err
        self.redirect = redirect
        self.stdout = None
        self.stderr = None

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
                    log_handler = StreamHandler(self.stderr)
                    self.logger.addHandler(log_handler)
                try:
                    yield
                finally:
                    if redirect_logger:
                        self.logger.removeHandler(log_handler)
                    self.stdout = None
                    self.stderr = None

    def print_out_err(self):
        if self.out is not None:
            print(self.out, file=self.stdout)
        if self.err is not None:
            print(self.err, file=self.stderr)

    async def __call__(self, scheduler):
        async with self.setup_stdouterr():
            self.print_out_err()
            return await super().__call__(scheduler)


async def run(todo, **run_args):
    async with LogMux(sys.stdout) as outmux:
        async with LogMux(sys.stderr) as errmux:
            TJob.outmux = outmux
            TJob.errmux = errmux
            with setup_scheduler(Scheduler, todo) as scheduler:
                result = await scheduler.run(**run_args)
    return result


async def test_no_output_from_no_jobs(verify_output):
    await run([])
    assert verify_output([], [])


async def test_no_output_from_two_jobs(verify_output):
    await run([TJob('foo'), TJob('bar')])
    assert verify_output([[], []], [[], []])


async def test_default_output_is_undecorated(verify_output):
    jobs = ['foo', 'bar']
    out = "This is {name}'s stdout"
    err = "This is {name}'s stderr"

    todo = [
        TJob(name, out=out.format(name=name), err=err.format(name=name))
        for name in jobs
    ]
    await run(todo)
    assert verify_output(
        [[out.format(name=name)] for name in jobs],
        [[err.format(name=name)] for name in jobs],
    )


async def test_decorated_output_from_two_jobs(verify_output):
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
