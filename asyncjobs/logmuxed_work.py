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
        self.stdout = None
        self.stderr = None
        self.redirect_logger = redirect_logger

    def decorate_out(self, msg):
        """Manipulate each message sent to self.stdout."""
        return msg

    def decorate_err(self, msg):
        """Manipulate each message sent to self.stderr."""
        return msg

    @contextlib.asynccontextmanager
    async def _setup_redirection(self, scheduler):
        async with scheduler.outmux.new_stream(self.decorate_out) as outf:
            async with scheduler.errmux.new_stream(self.decorate_err) as errf:
                self.stdout = outf
                self.stderr = errf
                log_handler = None
                if self.redirect_logger:
                    log_handler = logging.StreamHandler(self.stderr)
                    self.logger.addHandler(log_handler)
                try:
                    yield
                finally:
                    if log_handler is not None:
                        self.logger.removeHandler(log_handler)
                    self.stdout = None
                    self.stderr = None

    async def __call__(self, scheduler):
        async with self._setup_redirection(scheduler):
            return await super().__call__(scheduler)

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

    def __init__(self, *, outmux=None, errmux=None, **kwargs):
        self.outmux = logmux.LogMux(sys.stdout) if outmux is None else outmux
        self.errmux = logmux.LogMux(sys.stderr) if errmux is None else errmux
        super().__init__(**kwargs)

    async def _run_tasks(self, *args, **kwargs):
        logger.debug('Starting LogMux instances…')
        async with self.outmux:
            async with self.errmux:
                await super()._run_tasks(*args, **kwargs)
