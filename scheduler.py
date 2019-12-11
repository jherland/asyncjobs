import asyncio
import concurrent.futures
from contextlib import asynccontextmanager, contextmanager
from functools import partial
import logging
import signal
import subprocess


logger = logging.getLogger('scheduler')


class Job:
    """ABC for jobs. Jobs have a name, and are callable."""

    def __init__(self, name):
        self.name = name
        self.logger = logging.getLogger(name)
        self.logger.debug(f'Created {self.__class__.__name__}: {self}')

    def __str__(self):
        return self.name

    async def __call__(self, scheduler):
        """Perform the job.

        The given scheduler object can be used to interact with the
        surrounding job running machinery.
        """
        pass


class JobInWorker(Job):
    """A Job where the actual work is done in a worker."""

    async def __call__(self, scheduler):
        """Call self.do_work() in a worker and wait for its result."""
        await super().__call__(scheduler)
        self.logger.debug(f'Awaiting own work…')
        result = await scheduler.call_in_thread(self.do_work)
        self.logger.debug(f'Awaited own work: {result}')
        return result

    def do_work(self):
        """Actual work to be performed here."""
        pass


class JobWithDeps(Job):
    """A Job that waits for dependencies to finish before work is started."""

    def __init__(self, *, deps, **kwargs):
        self.deps = set(deps)
        self.dep_results = {}  # Filled by __call__()
        super().__init__(**kwargs)

    def __str__(self):
        deps = ' '.join(sorted(self.deps))
        return f'{super().__str__()} [{deps}]'

    async def __call__(self, scheduler):
        if self.deps:
            self.logger.debug(f'Awaiting dependencies {self.deps}…')
            self.dep_results = await scheduler.results(*self.deps)
        return await super().__call__(scheduler)


def current_task():
    if hasattr(asyncio, 'current_task'):  # Added in Python v3.7
        return asyncio.current_task()
    else:
        return asyncio.Task.current_task()


def current_task_name():
    task = current_task()
    if hasattr(asyncio.Task, 'get_name'):  # Added in Python v3.8
        return task.get_name()
    elif hasattr(task, 'job_name'):  # Added by Scheduler._start_job
        return task.job_name
    else:
        return None


class Scheduler:
    """Async job scheduler. Run Job instances as concurrent tasks."""

    def __init__(self, workers=1):
        self.jobs = {}  # name -> Job
        self.tasks = {}  # name -> Task object, aka. (future) result from job()
        self.running = False

    def _start_job(self, job):
        if hasattr(asyncio.Task, 'get_name'):  # Added in Python v3.8
            self.tasks[job.name] = asyncio.create_task(
                job(self), name=job.name
            )
        else:
            task = asyncio.ensure_future(job(self))
            task.job_name = job.name
            self.tasks[job.name] = task
        #  logger.info(f'{job} started')

    def add(self, job):
        """Add a job to be run.

        If we're already running (i.e. inside .run()) schedule the job
        immediately, otherwise the job will be scheduled by .run().
        Raise ValueError if a job with the same name has already been added.
        """
        if job.name in self.jobs:
            exist = self.jobs[job.name]
            raise ValueError(f'Cannot add {job} with same name as {exist}')
        self.jobs[job.name] = job
        #  logger.info(f'{job} added')
        if self.running:
            self._start_job(job)

    async def results(self, *job_names):
        """Wait until the given jobs are finished, and return their results.

        Returns a dictionary mapping job names to their results.

        Raise KeyError if any of the given jobs have not been added to the
        scheduler. If any of the given jobs raise an exception, then raise
        CancelledError here to cancel the calling job.
        """
        assert self.running
        caller = current_task_name()
        tasks = [self.tasks[n] for n in job_names]
        pending = [n for n, t in zip(job_names, tasks) if not t.done()]
        if pending:
            logger.debug(f'{caller} is waiting for {", ".join(pending)}…')
        try:
            results = dict(zip(job_names, await asyncio.gather(*tasks)))
        except Exception:
            logger.info(f'{caller} cancelled due to failure(s) in {job_names}')
            raise concurrent.futures.CancelledError
        logger.debug(f'{caller} <- {results} from .results()')
        return results

    async def _run_tasks(self, return_when):
        logger.info(f'Waiting for all jobs to complete…')
        await asyncio.wait(self.tasks.values(), return_when=return_when)

    async def run(self, keep_going=False):
        """Run until all jobs are finished.

        If keep_going is disabled (the default), the first failing job (i.e.
        a job that raises an exception) will cause us to cancel all other
        concurrent and remaining jobs and return as soon as possible.

        If keep_going is enabled, we will keep running as long as there are
        jobs to do. Only the jobs that depend (directly or indirectly) on the
        failing job(s) will be cancelled.

        Return a dictionary mapping job names to the corresponding
        asyncio.Future objects that encapsulate their fate (result, exception,
        or cancelled state).
        """
        logger.debug('Running…')
        if keep_going:
            return_when = concurrent.futures.ALL_COMPLETED
        else:
            return_when = concurrent.futures.FIRST_EXCEPTION

        to_start = list(self.jobs.values())
        if not to_start:
            logger.warning('Nothing to do!')
            return self.tasks
        for job in to_start:
            self._start_job(job)

        self.running = True
        await self._run_tasks(return_when)
        self.running = False

        return self.tasks


class ExternalWorkScheduler(Scheduler):
    """Manage jobs whose work is done in threads or subprocesses.

    Extend Scheduler with methods that allow Job instances to perform work in
    other threads or subprocess, while keeping the number of _concurrent_
    threads/processes within the given limit.
    """

    def __init__(self, workers=1):
        assert workers > 0
        self.workers = workers
        self.worker_sem = None
        self.worker_threads = None
        super().__init__()

    @asynccontextmanager
    async def reserve_worker(self):
        """Acquire a worker context where the caller can run its own work."""
        assert self.running
        logger.debug('acquiring worker semaphore…')
        async with self.worker_sem:
            logger.debug('acquired worker semaphore')
            yield
            logger.debug('releasing worker semaphore')

    async def call_in_thread(self, func, *args):
        """Call func(*args) in a worker thread and await its result."""
        caller = current_task_name()
        async with self.reserve_worker():
            if self.worker_threads is None:
                self.worker_threads = concurrent.futures.ThreadPoolExecutor(
                    max_workers=self.workers,
                    thread_name_prefix='Worker Thread',
                )
            try:
                logger.debug(f'{caller} -> awaiting worker thread…')
                result = await asyncio.get_running_loop().run_in_executor(
                    self.worker_threads, func, *args
                )
                logger.debug(f'{caller} <- {result} from worker')
                return result
            except Exception as e:
                logger.warning(f'{caller} <- Exception {e} from worker!')
                raise

    async def run_in_subprocess(self, argv, check=True):
        """Run a command line in a subprocess and await its exit code."""
        caller = current_task_name()
        async with self.reserve_worker():
            logger.debug(f'{caller} -> starting {argv} in subprocess…')
            proc = await asyncio.create_subprocess_exec(*argv)
            try:
                logger.debug(f'{caller} -- awaiting subprocess…')
                retcode = await proc.wait()
            except asyncio.CancelledError:
                logger.error(f'{caller} cancelled! Terminating {proc}!…')
                proc.terminate()
                try:
                    retcode = await proc.wait()
                    logger.debug(f'{caller} cancelled! {proc} terminated.')
                except asyncio.CancelledError:
                    logger.error(f'{caller} cancelled again! Killing {proc}!…')
                    proc.kill()
                    retcode = await proc.wait()
                    logger.debug(f'{caller} cancelled! {proc} killed.')
                raise
        if check and retcode:
            raise subprocess.CalledProcessError(retcode, argv)
        return retcode

    async def _run_tasks(self, *args, **kwargs):
        self.worker_sem = asyncio.BoundedSemaphore(self.workers)

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


class SignalHandlingScheduler(Scheduler):
    handle_signals = {signal.SIGHUP, signal.SIGTERM, signal.SIGINT}

    def _caught_signal(self, signum, cancel_task, cancel_jobs=False):
        logger.warning(f'Caught signal {signum}!')
        assert self.running
        if cancel_jobs:  # TODO: REMOVE?
            cancelled, total = 0, 0
            for job_name, task in self.tasks.items():
                total += 1
                if not task.done():
                    logger.warning(f'Cancelling job {job_name}…')
                    task.cancel()
                    cancelled += 1
            logger.warning(f'Cancelled {cancelled}/{total} jobs')
        if cancel_task is not None and not cancel_task.done():
            logger.warning(f'Cancelling task {cancel_task}…')
            cancel_task.cancel()

    @contextmanager
    def _handle_signals(self):
        loop = asyncio.get_running_loop()
        for signum in self.handle_signals:
            handler = partial(self._caught_signal, signum, current_task())
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
