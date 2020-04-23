import asyncio
from contextlib import contextmanager
from functools import partial
import logging
import signal

from . import basic

logger = logging.getLogger(__name__)


Job = basic.Job


class Scheduler(basic.Scheduler):
    """Teach Scheduler to cancel/abort properly on incoming signals.

    This installs appropriate signal handler to intercept SIGHUP, SIGTERM and
    SIGINT (aka. Ctrl+C or KeyboardInterrupt) and cause them to cancel and
    clean up the Scheduler (and its jobs) in an orderly manner.
    """

    handle_signals = {signal.SIGHUP, signal.SIGTERM, signal.SIGINT}

    def _caught_signal(self, signum, this_task):
        logger.warning(f'Caught signal {signum}!')
        assert self.running and not this_task.done()
        logger.warning(f'Cancelling task {this_task}…')
        this_task.cancel()

    @contextmanager
    def _handle_signals(self):
        loop = asyncio.get_running_loop()
        sched_task = asyncio.current_task()
        for signum in self.handle_signals:
            handler = partial(self._caught_signal, signum, sched_task)
            loop.add_signal_handler(
                signum, partial(loop.call_soon_threadsafe, handler)
            )
        try:
            yield
        except asyncio.CancelledError:
            logger.warning('Intercepted Cancelled on the way out')
        finally:
            for signum in self.handle_signals:
                loop.remove_signal_handler(signum)

    async def _run_tasks(self, *args, **kwargs):
        with self._handle_signals():
            return await super()._run_tasks(*args, **kwargs)
