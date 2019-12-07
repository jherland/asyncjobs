import asyncio
import concurrent.futures
import logging


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
        self.logger.debug(f'Scheduling own work')
        future = scheduler.do_in_worker(self.do_work)
        self.logger.debug(f'Awaiting own work')
        result = await future
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


class Scheduler:
    def __init__(self, workers=None):
        self.workers = workers
        self.jobs = {}  # name -> Job
        self.tasks = {}  # name -> Task object, aka. (future) result from job()
        self.running = False

    def _caller(self):
        if hasattr(asyncio, 'current_task'):  # Added in Python v3.7
            task = asyncio.current_task()
        else:
            task = asyncio.Task.current_task()
        if hasattr(asyncio.Task, 'get_name'):  # Added in Python v3.8
            return task.get_name()
        else:
            return task.job_name

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
        caller = self._caller()
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

    def _do_sync_work(self, loop, func, *args):
        future = loop.create_future()
        try:
            future.set_result(func(*args))
        except Exception as e:
            future.set_exception(e)
        return future

    def do_in_worker(self, func, *args):
        """Call 'func(*args)' in a worker and return its future result."""
        assert self.running
        caller = self._caller()
        loop = asyncio.get_running_loop()
        if self.workers is None:  # single-threaded, run synchronously
            logger.debug(f'{caller} -> doing work synchronously…')
            future = self._do_sync_work(loop, func, *args)
        else:
            logger.debug(f'{caller} -> scheduling work')
            future = loop.run_in_executor(self.workers, func, *args)
        result = future if future.done() else '<pending result>'
        logger.debug(f'{caller} <- {result} from worker')
        return future

    async def _run_tasks(self, return_when):
        logger.info(f'Waiting for all jobs to complete…')
        self.running = True
        await asyncio.wait(self.tasks.values(), return_when=return_when)
        self.running = False

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

        await self._run_tasks(return_when)

        if self.workers is not None:
            self.workers.shutdown()
        return self.tasks
