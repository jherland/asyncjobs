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

        @redirected_job(decorate_out='DECORATED: ')
        async def my_other_job(ctx):
            with ctx.stdout as f:
                print('hello', file=f)
                await asyncio.sleep(1)
                print(again', file=f)
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
    done for subprocesses, and for loggers (unless a custom log_handler is
    passed, or if log handling is disabled by log_handler=False).
    Other output (from thread workers or directly from .__call__()) must be
    redirected manually into the file descriptor retrieved from
    self.stdout.open() or self.stderr.open() (or by using self.stdout or
    self.stderr as context managers).
    """

    @staticmethod
    def default_log_handler_factory(ctx):
        return logging.StreamHandler(ctx.stderr.open())

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
        assert self.stdout is None and self.stderr is None
        try:
            self.stdout = self._scheduler.outmux.new_stream(decorate_out)
            self.stderr = self._scheduler.errmux.new_stream(decorate_err)
            with self._log_context(log_handler):
                yield
        finally:
            if self.stdout is not None:
                self.stdout.close()
                self.stdout = None
            if self.stderr is not None:
                self.stderr.close()
                self.stderr = None

    async def call_in_thread(self, func, *args, log_handler=True):
        """Call func(*args) in a worker thread and await its result."""

        @functools.wraps(func)
        def call_func_in_log_context(*args):
            with self._log_context(log_handler):
                return func(*args)

        return await super().call_in_thread(call_func_in_log_context, *args)

    @contextlib.asynccontextmanager
    async def subprocess(self, argv, **kwargs):
        """Pass redirected out/err as stdout/stderr to the subprocess.

        Only if stdout/stderr is not already customized by the caller.
        """
        with contextlib.ExitStack() as stack:
            if kwargs.get('stdout') is None:
                kwargs['stdout'] = stack.enter_context(self.stdout)
            if kwargs.get('stderr') is None:
                kwargs['stderr'] = stack.enter_context(self.stderr)
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
        log_formatter=None,
        context_class=Context,
        **kwargs,
    ):
        self.outmux = outmux
        self.errmux = errmux
        self.log_demuxer = log_demuxer
        self.log_formatter = log_formatter

        assert issubclass(context_class, Context)
        super().__init__(context_class=context_class, **kwargs)

    async def _run_tasks(self, *args, **kwargs):
        if self.outmux is None:
            self.outmux = logmux.LogMux(sys.stdout)
        if self.errmux is None:
            self.errmux = logmux.LogMux(sys.stderr)
        if self.log_demuxer is None:
            self.log_demuxer = logcontext.LogContextDemuxer()
        assert isinstance(self.log_demuxer, logcontext.LogContextDemuxer)
        copy_formatter = self.log_formatter is True
        if isinstance(self.log_formatter, logging.Formatter):
            self.log_demuxer.setFormatter(self.log_formatter)

        logger.debug('Starting LogMux instances…')
        with self.outmux, self.errmux:
            with self.log_demuxer.installed(copy_formatter=copy_formatter):
                await super()._run_tasks(*args, **kwargs)
