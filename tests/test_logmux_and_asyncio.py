import asyncio
import sys

from asyncjobs import logmux, logmuxed_work

from conftest import verify_tasks


async def job_coro(ctx):
    async with ctx.redirect(
        decorate_out=lambda msg: f'{ctx.name}/out: {msg}',
        decorate_err=lambda msg: f'{ctx.name}/ERR: {msg}',
    ):
        print('FOO', file=ctx.stdout)
        print('BAR', file=ctx.stderr)
        return 123


async def noop():
    pass


# These tests verify that the logmuxes used by logmuxed_work are hooked up to
# the same event loop as the scheduler itself. Regressions here are not
# reproducible when everything runs inside the same asyncio test framework,
# hence these tests drive asyncio.run directly.


def test_instantiating_logmux_before_asyncio_run_is_ok():
    logmux.LogMux(sys.stdout)
    asyncio.run(noop())


def test_instantiating_logmux_after_asyncio_run_is_ok():
    asyncio.run(noop())
    logmux.LogMux(sys.stdout)


def test_one_simple_job_through_default_muxes(verify_output, num_workers):
    scheduler = logmuxed_work.Scheduler(workers=num_workers)
    scheduler.add_job('foo', job_coro)
    done = asyncio.run(scheduler.run())
    assert verify_tasks(done, {'foo': 123})
    assert verify_output([['foo/out: FOO']], [['foo/ERR: BAR']])


def test_one_simple_job_through_custom_muxes(verify_output, num_workers):
    scheduler = logmuxed_work.Scheduler(
        workers=num_workers,
        outmux=logmux.LogMux(sys.stderr),
        errmux=logmux.LogMux(sys.stdout),
    )
    scheduler.add_job('foo', job_coro)
    done = asyncio.run(scheduler.run())
    assert verify_tasks(done, {'foo': 123})
    assert verify_output([['foo/ERR: BAR']], [['foo/out: FOO']])
