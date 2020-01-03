import asyncio
import concurrent.futures
from contextlib import asynccontextmanager, contextmanager
from functools import partial
import logging
import signal
import subprocess
import time


logger = logging.getLogger('scheduler')


class Job:
    """Base class for jobs. Jobs have a name, dependencies, and are callable.

    A Job instance represents a unit of work to be performed. The job is
    identified by a name (.name), and its work is performed by calling it
    (i.e. invoking the .__call__() async method, aka. coroutine).

    Additionally, the job has a set of zero of more dependencies (.deps) which
    are the names of other jobs that should complete successfully before this
    job can proceed.

    A job instance is submitted for execution by passing it to the .add()
    method of a Scheduler instance. The Scheduler will then start the job
    (during its .run()) by starting the job's .__call__() coroutine.
    The scheduler passes itself to the job's .__call__(), to allow the job
    implementation to communicate with the scheduler while it's working.

    The return value from .__call__() is known as the job result. Any return
    value from .__call__() is taken to mean that the job finished successfully.
    This return value is accessible to other jobs (via Scheduler.results())
    and ultimately included in the result from Scheduler.run().

    If .__call__() raises an exception, the job is considered to have failed.
    This may or may not cause the Scheduler to cancel/abort all other jobs,
    but it will at least cause dependent jobs to be cancelled (when they call
    Scheduler.results()).

    The ordering of jobs according to their dependencies is implemented by the
    default .__call__() implementation below, by asking (and waiting for) the
    scheduler to return the results of its dependencies. Subclasses must make
    sure to call this base class implementation before assuming anything about
    the state of its dependencies.
    """

    def __init__(self, name, *, deps=None):
        self.name = name
        self.deps = set() if deps is None else set(deps)
        self.logger = logging.getLogger(name)
        self.logger.debug(f'Created {self.__class__.__name__}: {self}')

    def __str__(self):
        return self.name

    def __repr__(self):
        return f'{self.__class__.__name__}({self.name!r}, deps={self.deps!r})'

    async def __call__(self, scheduler):
        """Perform the job.

        The given scheduler object can be used to interact with the
        surrounding job running machinery.

        This superclass implementation awaits the results from our dependencies
        and returns their results as a dict mapping job names to results.
        Until this returns, we cannot assume that our dependencies have been
        run at all.

        Raises KeyError if any dependency has not been added to the scheduler.
        Raises CancelledError if any dependency has failed.
        """
        if self.deps:
            self.logger.debug(f'Awaiting dependencies {self.deps}…')
            return await scheduler.results(*self.deps)
        return {}


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


def fate(future):
    """Return a word describing the state of the given future."""
    if not future.done():
        return 'unfinished'
    elif future.cancelled():
        return 'cancelled'
    elif future.exception() is not None:
        return 'failed'
    else:
        return 'success'


class Scheduler:
    """Async job scheduler. Run Job instances as concurrent tasks.

    Jobs are added by passing Job instances to .add(). These will be scheduled
    for execution when .run() is called. While running, jobs may await each
    other's results (by calling .results()). Further jobs may be added (with
    .add()). When all jobs are completed (with or without success), .run() will
    return dictionary with all the job results.
    """

    def __init__(self, *, event_handler=None):
        self.jobs = {}  # name -> Job
        self.tasks = {}  # name -> Task object, aka. (future) result from job()
        self.running = False
        self.event_handler = event_handler

    def event(self, event, job_name=None, info=None):
        if self.event_handler is None:
            return
        d = {'timestamp': time.time(), 'event': event}
        if job_name is not None:
            d['job'] = job_name
        if info is not None:
            d.update(info)
        logger.debug(f'Posting event: {d}')
        try:
            self.event_handler(d)
        except asyncio.QueueFull:
            logger.error('Failed to post event: {d}')

    def _start_job(self, job):
        if hasattr(asyncio.Task, 'get_name'):  # Added in Python v3.8
            self.tasks[job.name] = asyncio.create_task(
                job(self), name=job.name
            )
        else:
            task = asyncio.ensure_future(job(self))
            task.job_name = job.name
            self.tasks[job.name] = task
        self.event('start', job.name)

        self.tasks[job.name].add_done_callback(
            lambda task: self.event('finish', job.name, {'fate': fate(task)})
        )

    def add(self, job):
        """Add a job to be run.

        If we're already running (i.e. inside .run()) schedule the job
        immediately, otherwise the job will be scheduled when .run() is called.
        Raise ValueError if a job with the same name has already been added.
        """
        if job.name in self.jobs:
            exist = self.jobs[job.name]
            raise ValueError(f'Cannot add {job} with same name as {exist}')
        self.jobs[job.name] = job
        self.event('add', job.name)
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
        self.event(
            'await results',
            caller,
            {'jobs': list(job_names), 'pending': pending},
        )
        if pending:
            logger.debug(f'{caller} is waiting for {", ".join(pending)}…')
        try:
            results = dict(zip(job_names, await asyncio.gather(*tasks)))
        except Exception:
            logger.info(f'{caller} cancelled due to failure(s) in {job_names}')
            raise concurrent.futures.CancelledError
        self.event('awaited results', caller)
        logger.debug(f'{caller} <- {results} from .results()')
        return results

    async def _run_tasks(self, return_when):
        logger.info(f'Waiting for all jobs to complete…')
        self.event('await tasks', info={'jobs': list(self.tasks.keys())})
        try:
            await asyncio.wait(self.tasks.values(), return_when=return_when)
        finally:
            # Any tasks left running at this point should be cancelled and
            # reaped _before_ we return from here
            for job_name, task in self.tasks.items():
                if not task.done():
                    logger.warning(f'Cancelling {job_name}...')
                    task.cancel()
            try:
                await asyncio.wait(self.tasks.values())
            finally:
                self.event('awaited tasks')

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
        self.event(
            'start',
            info={'keep_going': keep_going, 'num_jobs': len(self.jobs)},
        )
        if keep_going:
            return_when = concurrent.futures.ALL_COMPLETED
        else:
            return_when = concurrent.futures.FIRST_EXCEPTION

        to_start = list(self.jobs.values())
        if to_start:
            for job in to_start:
                self._start_job(job)

            self.running = True
            await self._run_tasks(return_when)
            self.running = False
        else:
            logger.warning('Nothing to do!')
        self.event('finish', info={'num_tasks': len(self.tasks)})
        return self.tasks


class ExternalWorkScheduler(Scheduler):
    """Manage jobs whose work is done in threads or subprocesses.

    Extend Scheduler with methods that allow Job instances to perform work in
    other threads or subprocess, while keeping the number of _concurrent_
    threads/processes within the given limit.
    """

    def __init__(self, *, workers=1, **kwargs):
        assert workers > 0
        self.workers = workers
        self.worker_sem = None
        self.worker_threads = None
        super().__init__(**kwargs)

    @asynccontextmanager
    async def reserve_worker(self, caller=None):
        """Acquire a worker context where the caller can run its own work.

        This is the mechanism that ensures we do not schedule more concurrent
        work than allowed by .workers. Anybody that wants to spin off another
        thread or process to perform some work should use this context manager
        to await a free "slot".
        """
        assert self.running
        logger.debug('acquiring worker semaphore…')
        self.event('await worker slot', caller)
        async with self.worker_sem:
            self.event('awaited worker slot', caller)
            logger.debug('acquired worker semaphore')
            yield
            logger.debug('releasing worker semaphore')

    async def call_in_thread(self, func, *args):
        """Call func(*args) in a worker thread and await its result."""
        caller = current_task_name()
        async with self.reserve_worker(caller):
            if self.worker_threads is None:
                self.worker_threads = concurrent.futures.ThreadPoolExecutor(
                    max_workers=self.workers,
                    thread_name_prefix='Worker Thread',
                )
            try:
                logger.debug(f'{caller} -> awaiting worker thread…')
                self.event('await worker thread', caller, {'func': str(func)})
                future = asyncio.get_running_loop().run_in_executor(
                    self.worker_threads, func, *args
                )
                future.add_done_callback(
                    lambda f: self.event(
                        'awaited worker thread', caller, {'fate': fate(f)}
                    )
                )
                result = await future
                logger.debug(f'{caller} <- {result} from worker')
                return result
            except Exception as e:
                logger.warning(f'{caller} <- Exception {e} from worker!')
                raise

    async def run_in_subprocess(self, argv, check=True):
        """Run a command line in a subprocess and await its exit code."""
        caller = current_task_name()
        retcode = None
        async with self.reserve_worker(caller):
            logger.debug(f'{caller} -> starting {argv} in subprocess…')
            self.event('await worker proc', caller, {'argv': argv})
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
            finally:
                self.event('awaited worker proc', caller, {'exit': retcode})
        if check and retcode != 0:
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
    """Teach Scheduler to cancel/abort properly on incoming signals.

    This installs appropriate signal handler to intercept SIGHUP, SIGTERM and
    SIGINT (aka. Ctrl+C or KeyboardInterrupt) and cause them to cancel and
    clean up the Scheduler and its jobs in an orderly manner.
    """

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
