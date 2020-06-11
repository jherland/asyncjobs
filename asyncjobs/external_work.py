import asyncio
import concurrent.futures
import contextlib
import logging
import subprocess

from . import basic

logger = logging.getLogger(__name__)


class Context(basic.Context):
    """Extend Context with helpers for doing work in threads + processes."""

    @contextlib.asynccontextmanager
    async def reserve_worker(self):
        """Acquire a worker context where this job can run its own work.

        This is the mechanism that ensures we do not schedule more concurrent
        work than allowed by Scheduler.workers. Anybody that wants to spin off
        another thread or process to perform some work should use this context
        manager to await a free "slot".
        """
        self.logger.debug('-> acquiring worker semaphore…')
        self.event('await worker slot')
        async with self._scheduler.worker_semaphore:
            self.event('awaited worker slot')
            self.logger.debug('-- acquired worker semaphore')
            try:
                yield
            finally:
                self.logger.debug('<- releasing worker semaphore')

    async def call_in_thread(self, func, *args):
        """Call func(*args) in a worker thread and await its result."""
        async with self.reserve_worker():
            try:
                self.logger.debug(f'-> starting {func} in worker thread…')
                self.event('await worker thread', func=str(func))
                future = self._scheduler._start_in_thread(func, *args)
                future.add_done_callback(
                    lambda fut: self.event(
                        'awaited worker thread',
                        fate=self._scheduler._fate(fut),
                    )
                )
                self.logger.debug('-- awaiting worker thread…')
                result = await future
                self.logger.debug(f'<- {result!r} from worker')
                return result
            except Exception as e:
                self.logger.warning(f'<- Exception {e} from worker!')
                raise

    async def run_in_subprocess(
        self, argv, stdin=None, stdout=None, stderr=None, check=True
    ):
        """Run a command line in a subprocess and await its exit code."""
        returncode = None
        async with self.reserve_worker():
            self.logger.debug(f'-> starting {argv} in subprocess…')
            self.event('await worker proc', argv=argv)
            proc = await asyncio.create_subprocess_exec(
                *argv, stdin=stdin, stdout=stdout, stderr=stderr
            )
            try:
                self.logger.debug('-- awaiting subprocess…')
                returncode = await proc.wait()
            except asyncio.CancelledError:
                self.logger.error(f'Cancelled! Terminating {proc}!…')
                proc.terminate()
                try:
                    returncode = await proc.wait()
                    self.logger.debug(f'Cancelled! {proc} terminated.')
                except asyncio.CancelledError:
                    self.logger.error(f'Cancelled again! Killing {proc}!…')
                    proc.kill()
                    returncode = await proc.wait()
                    self.logger.debug(f'Cancelled! {proc} killed.')
                raise
            finally:
                self.event('awaited worker proc', returncode=returncode)
        if check and returncode != 0:
            raise subprocess.CalledProcessError(returncode, argv)
        return returncode


class Scheduler(basic.Scheduler):
    """Manage jobs whose work is done in threads or subprocesses.

    Extend Scheduler with methods that allow job coroutines to perform work in
    other threads or subprocesses, while keeping the number of _concurrent_
    threads/processes within the given limit.
    """

    def __init__(self, *, workers=1, context_class=Context, **kwargs):
        assert workers > 0
        self.workers = workers
        self.worker_semaphore = None
        self.worker_threads = None

        assert issubclass(context_class, Context)
        super().__init__(context_class=context_class, **kwargs)

    def add_thread_job(self, name, func, *args, deps=None):
        async def coro(ctx):
            return await ctx.call_in_thread(func, *args)

        return self.add_job(name, coro, deps)

    def add_subprocess_job(self, name, argv, *, deps=None, **kwargs):
        async def coro(ctx):
            return await ctx.run_in_subprocess(argv, **kwargs)

        return self.add_job(name, coro, deps)

    def _start_in_thread(self, func, *args):
        if self.worker_threads is None:
            self.worker_threads = concurrent.futures.ThreadPoolExecutor(
                max_workers=self.workers, thread_name_prefix='Worker Thread',
            )
        return asyncio.get_running_loop().run_in_executor(
            self.worker_threads, func, *args
        )

    async def _run_tasks(self, *args, **kwargs):
        self.worker_semaphore = asyncio.BoundedSemaphore(self.workers)

        try:
            await super()._run_tasks(*args, **kwargs)
            if self.worker_threads is not None:
                logger.debug('Shutting down worker threads…')
                self.worker_threads.shutdown()  # wait=timeout is None)
                self.worker_threads = None
                logger.debug('Shut down worker threads')
        finally:
            if self.worker_threads is not None:
                logger.error('Cancelling without awaiting workers…')
                self.worker_threads.shutdown(wait=False)
                self.worker_threads = None
