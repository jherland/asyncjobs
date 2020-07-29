import contextlib
import functools
import logging
import sys

from . import external_work, logcontext, stream_mux

logger = logging.getLogger(__name__)


def decorated_job(decorate_out=None, decorate_err=None, decorate_log=None):
    """Setup stdout/stderr/log decoration for the given coroutine.

    This is the same as wrapping the entire coroutine in:

        with ctx.decoration(...):
            ...

    Use as a decorator:

        @decorated_job()
        async def my_job(ctx):
            logger.info('hello')
            await asyncio.sleep(1)
            logger.info(world')

        @decorated_job(decorate_out='DECORATED: ')
        async def my_other_job(ctx):
            with ctx.stdout as f:
                print('hello', file=f)
                await asyncio.sleep(1)
                print(again', file=f)

    See Context.decoration() for more details
    """

    def wrap(coro):
        @functools.wraps(coro)
        async def wrapped_coro(ctx):
            with ctx.decoration(
                decorate_out=decorate_out,
                decorate_err=decorate_err,
                decorate_log=decorate_log,
            ):
                return await coro(ctx)

        return wrapped_coro

    return wrap


class Context(external_work.Context):
    """API for jobs that want to decorate their logs/stdout/stderr.

    This enables jobs to create stdout/stderr streams that are automatically
    decorated and multiplexed onto the stdout/stderr streams controlled by the
    below Scheduler.

    Redirection of the actual stdout/stderr file descriptors is automatically
    done for subprocesses. Log statements (made in the job coroutine or its
    callees, or from threads run by the job) are not redirected, but are
    decorated nonetheless (as long as logcontext.Formatter is used as the log
    formatter).

    Other output (from the job coroutine, or from thread workers) can be
    decorated by writing into the file descriptor retrieved from
    self.stdout.open() or self.stderr.open() (or by using self.stdout or
    self.stderr as context managers).
    """

    def __init__(self, *args):
        super().__init__(*args)
        self.stdout = None
        self.stderr = None

    @contextlib.contextmanager
    def decoration(
        self, *, decorate_out=None, decorate_err=None, decorate_log=None,
    ):
        """Setup context with decorated stdout/stderr streams + decorated log.

        Inside this context, self.stdout and self.stderr are DecoratedStream
        objects (decorated by 'decorate_out' and 'decorate_err', respectively)
        that may be opened/entered in order to write decorated output.

        Likewise, 'decorate_log' is set as the log decorator for the current
        context (the current job).

        If 'decorate_log' is not given, but 'decorate_err' is given,
        'decorate_log' will default to 'decorate_err'. Otherwise, all
        decorators default to None, i.e. no decoration.

        Note that the output streams (self.stdout, self.stderr) are used as the
        default stdout/stderr stream for subprocesses started within this
        context (see .subprocess() below). Similarly, 'decorate_log' is
        automatically applied to the log context of threads started within
        this context (see .call_in_thread() below).
        """
        if decorate_log is True:
            decorate_log = decorate_err
        assert self.stdout is None and self.stderr is None
        try:
            self.stdout = self._scheduler.outmux.new_stream(decorate_out)
            self.stderr = self._scheduler.errmux.new_stream(decorate_err)
            with logcontext.Decorator.use(decorate_log):
                yield
        finally:
            if self.stdout is not None:
                self.stdout.close()
                self.stdout = None
            if self.stderr is not None:
                self.stderr.close()
                self.stderr = None

    @contextlib.contextmanager
    def follow_file(self, path, use_stderr=False, period=0.1):
        """Echo the given file to self.stdout (or self.stderr).

        This sets up a StreamMux poller to periodically check for new content
        and echo lines from the given file. By default, the self.stdout mux
        (and decorator) is used, but pass use_stderr=True to use the
        self.stderr mux (and decorator) instead.

        This will only work inside the above .decoration() context, otherwise
        a RuntimeError is raised.

        Lines read from 'path' will be passed through 'decorator' before being
        written to this StreamMux' shared output.
        """
        stream = self.stderr if use_stderr else self.stdout
        if stream is None:
            raise RuntimeError('Cannot follow file outside decoration context')
        with stream.mux.follow_file(path, stream.decorator, period=period):
            yield

    async def call_in_thread(self, func, *args, **kwargs):
        """Apply the current log context (if any) to threaded function call."""
        decorator = logcontext.Decorator.get(logcontext.current_context())
        assert decorator is None or callable(decorator)

        @functools.wraps(func)
        def wrapper(*args):
            with logcontext.Decorator.use(decorator):
                return func(*args)

        return await super().call_in_thread(wrapper, *args, **kwargs)

    @contextlib.asynccontextmanager
    async def subprocess(
        self, argv, stdout=None, stderr=None, ticket=None, **kwargs
    ):
        """Pass redirected out/err as stdout/stderr to the subprocess.

        Only if stdout/stderr is not already customized by the caller.
        """
        async with self._use_ticket(ticket, only_reserve=True) as ticket:
            with contextlib.ExitStack() as stack:
                if stdout is None and self.stdout is not None:
                    stdout = stack.enter_context(self.stdout)
                if stderr is None and self.stderr is not None:
                    stderr = stack.enter_context(self.stderr)
                async with super().subprocess(
                    argv, stdout=stdout, stderr=stderr, ticket=ticket, **kwargs
                ) as proc:
                    yield proc


class Scheduler(external_work.Scheduler):
    """Run jobs with automatic decoration of their stdout/stderr + logs.

    Extend Scheduler with two StreamMux instances - one for stdout and one for
    stderr - to be used by job coroutines for redirecting (and decorating)
    their stdout/stderr through to a single pair of output streams controlled
    by this scheduler instance.
    """

    def __init__(
        self, *, outmux=None, errmux=None, context_class=Context, **kwargs,
    ):
        self.outmux = outmux
        self.errmux = errmux

        assert issubclass(context_class, Context)
        super().__init__(context_class=context_class, **kwargs)

    async def _run_tasks(self, *args, **kwargs):
        if self.outmux is None:
            self.outmux = stream_mux.StreamMux(sys.stdout)
        if self.errmux is None:
            self.errmux = stream_mux.StreamMux(sys.stderr)

        logger.debug('Starting StreamMux instances…')
        with self.outmux, self.errmux:
            await super()._run_tasks(*args, **kwargs)
