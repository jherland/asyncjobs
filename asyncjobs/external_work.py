import asyncio
import concurrent.futures
import contextlib
import logging
import subprocess

from . import basic

logger = logging.getLogger(__name__)


class Context(basic.Context):
    """Extend Context with helpers for doing work in threads + processes."""

    _next_worker_ticket = 0

    @classmethod
    def next_worker_ticket(cls):
        ret = cls._next_worker_ticket
        cls._next_worker_ticket += 1
        return ret

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._workers_reserved = set()
        self._workers_in_use = set()

    @contextlib.asynccontextmanager
    async def reserve_worker(self):
        """Acquire a worker context where this job can run its own work.

        This is the mechanism that ensures we do not schedule more concurrent
        work than allowed by Scheduler.workers. Anybody that wants to spin off
        another thread or process to perform some work should use this context
        manager to await a free "slot".

        Yields a worker "ticket" into the context, which can be passed to other
        methods that would otherwise need to reserve their own worker ticket.

        If a single job tries to reserve more concurrent workers than allowed
        by Scheduler.workers, a RuntimeError will be raised.
        """
        max_reserve = self._scheduler.workers
        if len(self._workers_reserved) >= max_reserve:
            raise RuntimeError(f'Cannot reserve >={max_reserve} worker(s)!')
        logger.debug(f'{self.name} -> acquiring worker ticket…')
        self.event('await worker slot')
        async with self._scheduler.worker_semaphore:
            ticket = self.next_worker_ticket()
            self._workers_reserved.add(ticket)
            self.event('awaited worker slot')
            logger.debug(f'{self.name} -- acquired worker ticket {ticket}')
            try:
                yield ticket
            finally:
                self._workers_reserved.remove(ticket)
                logger.debug(f'{self.name} <- released worker ticket {ticket}')

    @contextlib.asynccontextmanager
    async def _use_ticket(self, ticket=None, only_reserve=False):
        """Reuse the given worker ticket or reserve a new one."""
        if ticket is None:  # issue new ticket
            async with self.reserve_worker() as ticket:
                yield ticket
        else:  # verify ticket
            if ticket not in self._workers_reserved:
                raise ValueError(f'Cannot use ticket {ticket}: Not reserved')
            elif ticket in self._workers_in_use:
                raise ValueError(f'Cannot use ticket {ticket}: Already in use')
            try:
                if not only_reserve:
                    self._workers_in_use.add(ticket)
                yield ticket
            finally:
                if not only_reserve:
                    self._workers_in_use.remove(ticket)

    async def call_in_thread(self, func, *args, ticket=None):
        """Call func(*args) in a worker thread and await its result.

        Reuse worker ticket if given, otherwise reserve a new ticket.
        """
        async with self._use_ticket(ticket):
            try:
                logger.debug(f'{self.name} -> start {func} in worker thread…')
                self.event('start work in thread', func=str(func))
                future = self._scheduler._start_in_thread(func, *args)
                future.add_done_callback(
                    lambda fut: self.event(
                        'finish work in thread',
                        fate=self._scheduler._fate(fut),
                    )
                )
                logger.debug(f'{self.name} -- awaiting worker thread…')
                result = await future
                logger.debug(f'{self.name} <- {result!r} from worker')
                return result
            except Exception as e:
                logger.warning(f'{self.name} <- Exception {e!r} from worker!')
                raise

    async def terminate_subprocess(self, proc, argv, delay, *, kill=False):
        if proc.returncode is not None:  # process has already exited
            return

        verb = 'kill' if kill else 'terminate'
        logger.warning(f'{self.name}: {proc} is still alive, {verb}…')
        self.event(f'subprocess {verb}', argv=argv, pid=proc.pid)
        with contextlib.suppress(ProcessLookupError):
            if kill:
                proc.kill()
            else:
                proc.terminate()
        try:
            out, err = await asyncio.wait_for(proc.communicate(), delay)
            if out:
                logger.warning(f'{self.name}: {proc} stdout discard: {out!r}')
            if err:
                logger.warning(f'{self.name}: {proc} stderr discard: {err!r}')
            logger.debug(f'{self.name}: {proc} {verb} done.')
        except asyncio.TimeoutError:
            logger.warning(f'{self.name}: {delay}s timeout on {verb} {proc}')
        finally:
            if proc.returncode is None:  # still running
                await self.terminate_subprocess(proc, argv, delay, kill=True)

    @contextlib.asynccontextmanager
    async def subprocess(
        self, argv, *, check=False, kill_delay=3, ticket=None, **kwargs
    ):
        """Run a command line in a subprocess and interact with it.

        This context manager yields the asyncio.subprocess.Process instance
        into the context, and allows the context to interact with the process.
        The context should await the completion of the subprocess, as the
        process (if still alive) will be automatically killed upon exiting the
        context.

        Killing the subprocess is done by sending SIGTERM to it, and if it's
        still alive 3 seconds later, send SIGKILL as well. The delay between
        these signals can be customized with the 'kill_delay' argument.

        If check=True (the default is False), and the subprocess exits with a
        non-zero exit code (this includes the case where we have to kill it),
        we will raise subprocess.CalledProcessError. Otherwise, a non-zero exit
        code is not considered exceptional.

        Reuse worker ticket if given, otherwise reserve a new ticket.
        """
        async with self._use_ticket(ticket):
            logger.debug(f'{self.name} -> start {argv} in subprocess…')
            self.event('start work in subprocess', argv=argv)
            proc = await asyncio.create_subprocess_exec(*argv, **kwargs)
            try:
                logger.debug(f'{self.name} -- enter subprocess context…')
                yield proc
            finally:
                logger.debug(f'{self.name} -- exited subprocess context…')
                try:
                    if proc.returncode is None:  # still running
                        await self.terminate_subprocess(proc, argv, kill_delay)
                finally:
                    assert proc.returncode is not None
                    self.event(
                        'finish work in subprocess', returncode=proc.returncode
                    )
            if check and proc.returncode != 0:
                raise subprocess.CalledProcessError(proc.returncode, argv)

    async def run_in_subprocess(self, argv, **kwargs):
        """Run a command line in a subprocess and await its exit code."""
        async with self.subprocess(argv, **kwargs) as proc:
            logger.debug(f'{self.name} -- awaiting subprocess…')
            await proc.wait()
            logger.debug(f'{self.name} -- awaited subprocess')
            return proc.returncode


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

    def add_thread_job(self, name, func, *args, deps=None, **kwargs):
        async def coro(ctx):
            return await ctx.call_in_thread(func, *args, **kwargs)

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
