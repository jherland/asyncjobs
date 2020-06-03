import contextlib
import logging
import sys

from . import external_work, logmux

logger = logging.getLogger(__name__)


class Job(external_work.Job):
    """Job with stdout/stderr redirected via a LogMux-enabled Scheduler.

    This enables jobs to have their output multiplexed to a single (pair of)
    output stream(s) controlled by the Scheduler.

    Redirection of the actual stdout/stderr file descriptors is automatically
    done for subprocesses, and for self.logger (unless redirect_logger is set
    to False). Other output (from thread workers or directly from .__call__()
    must be redirected to self.stdout/self.stderr manually.)
    """

    def __init__(self, *args, redirect_logger=True, **kwargs):
        super().__init__(*args, **kwargs)
        self.redirect_logger = redirect_logger

    def decorate_out(self, msg):
        """Manipulate each message sent to self.stdout."""
        return msg

    def decorate_err(self, msg):
        """Manipulate each message sent to self.stderr."""
        return msg

    async def __call__(self, ctx):
        async with ctx.setup_redirection(
            decorate_out=self.decorate_out,
            decorate_err=self.decorate_err,
            redirect_logger=self.redirect_logger,
        ):
            return await super().__call__(ctx)


class Context(external_work.Context):
    """API for jobs with stdout/stderr redirected via a LogMux-aware Scheduler.

    This enables jobs to have their output multiplexed to a single (pair of)
    output stream(s) controlled by the below Scheduler.

    Redirection of the actual stdout/stderr file descriptors is automatically
    done for subprocesses, and for self.logger (unless redirect_logger is set
    to False). Other output (from thread workers or directly from .__call__()
    must be redirected to self.stdout/self.stderr manually.)
    """

    def __init__(self, *args):
        super().__init__(*args)
        self.stdout = None
        self.stderr = None

    @contextlib.asynccontextmanager
    async def setup_redirection(
        self, *, decorate_out=None, decorate_err=None, redirect_logger=True
    ):
        async with self._scheduler.outmux.new_stream(decorate_out) as outf:
            async with self._scheduler.errmux.new_stream(decorate_err) as errf:
                self.stdout = outf
                self.stderr = errf
                log_handler = None
                if redirect_logger:
                    log_handler = logging.StreamHandler(self.stderr)
                    self.logger.addHandler(log_handler)
                try:
                    yield
                finally:
                    if log_handler is not None:
                        self.logger.removeHandler(log_handler)
                    self.stdout = None
                    self.stderr = None

    async def run_in_subprocess(self, argv, **kwargs):
        """Pass redirected out/err as stdout/stderr to the subprocess.

        Only if stdout/stderr is not already customized by the caller.
        """
        if kwargs.get('stdout') is None:
            kwargs['stdout'] = self.stdout
        if kwargs.get('stderr') is None:
            kwargs['stderr'] = self.stderr
        return await super().run_in_subprocess(argv, **kwargs)


class Scheduler(external_work.Scheduler):
    """Run jobs with their output multiplexed through LogMux.

    Extend Scheduler with two LogMux instances - one for stdout and one for
    stderr - which are used by the above Job class to setup their own stdout
    and stderr streams redirected through these instances.
    """

    def __init__(
        self, *, outmux=None, errmux=None, context_class=Context, **kwargs
    ):
        self.outmux = logmux.LogMux(sys.stdout) if outmux is None else outmux
        self.errmux = logmux.LogMux(sys.stderr) if errmux is None else errmux

        assert issubclass(context_class, Context)
        super().__init__(context_class=context_class, **kwargs)

    async def _run_tasks(self, *args, **kwargs):
        logger.debug('Starting LogMux instances…')
        async with self.outmux:
            async with self.errmux:
                await super()._run_tasks(*args, **kwargs)
