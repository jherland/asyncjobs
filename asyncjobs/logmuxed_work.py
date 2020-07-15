import asyncio
import contextlib
import functools
import logging
import sys

from . import external_work, logcontext, logmux

logger = logging.getLogger(__name__)


def redirected_job(decorate_out=None, decorate_err=None, log_handler=True):
    """Setup stdout/stderr redirection for the given coroutine.

    This is the same as wrapping the entire coroutine in:

        async with ctx.redirect(...):
            ...

    Use as a decorator:

        @redirected_job()
        async def my_job(ctx):
            logger.info('hello')
            await asyncio.sleep(1)
            logger.info(world')

        @redirected_job(decorate_out=lambda s: f'DECORATED: {s}')
        async def my_other_job(ctx):
            print('hello', file=ctx.stdout)
            await asyncio.sleep(1)
            print(again', file=ctx.stdout)
    """

    def wrap(coro):
        @functools.wraps(coro)
        async def wrapped_coro(ctx):
            async with ctx.redirect(
                decorate_out=decorate_out,
                decorate_err=decorate_err,
                log_handler=log_handler,
            ):
                return await coro(ctx)

        return wrapped_coro

    return wrap


class Context(external_work.Context):
    """API for jobs with stdout/stderr redirected via a LogMux-aware Scheduler.

    This enables jobs to have their output multiplexed to a single (pair of)
    output stream(s) controlled by the below Scheduler.

    Redirection of the actual stdout/stderr file descriptors is automatically
    done for subprocesses, and for loggers (unless log_handler is False).
    Other output (from thread workers or directly from .__call__() must be
    redirected to self.stdout/self.stderr manually.)
    """

    @staticmethod
    def default_log_handler_factory(ctx):
        return logging.StreamHandler(ctx.stderr)

    def __init__(self, *args):
        super().__init__(*args)
        self.stdout = None
        self.stderr = None
        self.log_handler_factory = self.default_log_handler_factory

    @contextlib.contextmanager
    def _log_context(self, handler):
        if handler is True:
            handler = self.log_handler_factory(self)
        elif handler is False:
            handler = None
        assert handler is None or isinstance(handler, logging.Handler)
        with self._scheduler.log_demuxer.context_handler(handler):
            yield handler

    @contextlib.asynccontextmanager
    async def redirect(
        self, *, decorate_out=None, decorate_err=None, log_handler=True
    ):
        async with self._scheduler.outmux.new_stream(decorate_out) as outf:
            async with self._scheduler.errmux.new_stream(decorate_err) as errf:
                self.stdout = outf
                self.stderr = errf
                try:
                    with self._log_context(log_handler):
                        yield
                finally:
                    self.stdout = None
                    self.stderr = None

    async def call_in_thread(self, func, *args, log_handler=True):
        """Call func(*args) in a worker thread and await its result."""

        @functools.wraps(func)
        def func_wrapped_in_log_context(*args):
            with self._log_context(log_handler):
                return func(*args)

        return await super().call_in_thread(func_wrapped_in_log_context, *args)

    @contextlib.asynccontextmanager
    async def subprocess(self, argv, **kwargs):
        """Pass redirected out/err as stdout/stderr to the subprocess.

        Only if stdout/stderr is not already customized by the caller.
        """
        if kwargs.get('stdout') is None:
            kwargs['stdout'] = self.stdout
        if kwargs.get('stderr') is None:
            kwargs['stderr'] = self.stderr
        async with super().subprocess(argv, **kwargs) as proc:
            yield proc


class Scheduler(external_work.Scheduler):
    """Run jobs with their output multiplexed through LogMux.

    Extend Scheduler with two LogMux instances - one for stdout and one for
    stderr. Can be used by job coroutines redirect their stdout/stderr through
    to a single pair of output streams controlled by the caller.
    """

    def __init__(
        self,
        *,
        outmux=None,
        errmux=None,
        log_demuxer=None,
        context_class=Context,
        **kwargs,
    ):
        self.outmux = outmux
        self.errmux = errmux
        self.log_demuxer = log_demuxer

        assert issubclass(context_class, Context)
        super().__init__(context_class=context_class, **kwargs)

    async def _run_tasks(self, *args, **kwargs):
        if self.outmux is None:
            self.outmux = logmux.LogMux(sys.stdout)
        assert asyncio.get_running_loop() is self.outmux.q._loop
        if self.errmux is None:
            self.errmux = logmux.LogMux(sys.stderr)
        assert asyncio.get_running_loop() is self.errmux.q._loop
        if self.log_demuxer is None:
            self.log_demuxer = logcontext.LogContextDemuxer()
        assert isinstance(self.log_demuxer, logcontext.LogContextDemuxer)

        logger.debug('Starting LogMux instances…')
        async with self.outmux, self.errmux:
            with self.log_demuxer.installed():
                await super()._run_tasks(*args, **kwargs)
