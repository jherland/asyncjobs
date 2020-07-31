import asyncio
import logging
import sys

from asyncjobs import stream_mux, decorated_work

from conftest import adjusted_logger_level, verify_tasks


async def job_coro(ctx):
    with ctx.decoration(
        decorate_out=f'{ctx.name}/out: ', decorate_err=f'{ctx.name}/ERR: ',
    ):
        with ctx.stdout as outf, ctx.stderr as errf:
            print('FOO', file=outf)
            print('BAR', file=errf)
            return 123


async def noop():
    pass


# These tests verify that the StreamMuxes used by decorated_work are hooked up
# to the same event loop as the scheduler itself. Regressions here are not
# reproducible when everything runs inside the same asyncio test framework,
# hence these tests drive asyncio.run directly.


def test_instantiating_logmux_before_asyncio_run_is_ok():
    stream_mux.StreamMux(sys.stdout)
    asyncio.run(noop())


def test_instantiating_logmux_after_asyncio_run_is_ok():
    asyncio.run(noop())
    stream_mux.StreamMux(sys.stdout)


def test_one_simple_job_through_default_muxes(verify_output, num_workers):
    scheduler = decorated_work.Scheduler(workers=num_workers)
    scheduler.add_job('foo', job_coro)
    # Prevent stream_mux DEBUG messages on stderr
    with adjusted_logger_level(stream_mux.logger, logging.INFO):
        done = asyncio.run(scheduler.run())
    assert verify_tasks(done, {'foo': 123})
    assert verify_output([['foo/out: FOO']], [['foo/ERR: BAR']])


def test_one_simple_job_through_custom_muxes(verify_output, num_workers):
    scheduler = decorated_work.Scheduler(
        workers=num_workers,
        outmux=stream_mux.StreamMux(sys.stderr),
        errmux=stream_mux.StreamMux(sys.stdout),
    )
    scheduler.add_job('foo', job_coro)
    # Prevent stream_mux DEBUG messages on stderr
    with adjusted_logger_level(stream_mux.logger, logging.INFO):
        done = asyncio.run(scheduler.run())
    assert verify_tasks(done, {'foo': 123})
    assert verify_output([['foo/ERR: BAR']], [['foo/out: FOO']])
