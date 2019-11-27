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
        """Schedule self.do_work() to be called in a worker."""
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
        self.dep_results = {
            dep: await scheduler.result(dep) for dep in self.deps
        }
        return await super().__call__(scheduler)


class Scheduler:
    def __init__(self, workers=None):
        self.workers = workers
        self.jobs = {}  # name -> Job
        self.results = {}  # name -> (future) result from job()
        self.running = False

    def _caller(self):
        if hasattr(asyncio, 'current_task'):
            return asyncio.current_task().job_name  # New in Python v3.7
        else:
            return asyncio.Task.current_task().job_name

    def _start_job(self, job):
        task = asyncio.ensure_future(job(self))
        task.job_name = job.name
        self.results[job.name] = task
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

    async def result(self, job_name):
        """Wait until the given job is finished, and return its result.

        Raise KeyError if the given job has not been added to the scheduler.
        If the given job raises an exception, then raise CancelledError to
        cancel the calling job.
        """
        assert self.running
        caller = self._caller()
        if job_name not in self.results:
            raise KeyError(job_name)
        if not self.results[job_name].done():
            logger.debug(f'{caller} is waiting on {job_name}…')
        try:
            result = await self.results[job_name]
        except Exception:
            logger.info(f'{caller} cancelled due to failing {job_name}')
            raise concurrent.futures.CancelledError
        logger.debug(f'{caller} <- {result} from {job_name}')
        return result

    def do_in_worker(self, func, *args):
        """Call 'func(*args)' in a worker and return its future result."""
        assert self.running
        caller = self._caller()
        loop = asyncio.get_running_loop()
        if self.workers is None:  # single-threaded, run synchronously
            logger.debug(f'{caller} -> doing work…')
            future = loop.create_future()
            try:
                future.set_result(func(*args))
            except Exception as e:
                future.set_exception(e)
        else:
            logger.debug(f'{caller} -> scheduling work')
            future = loop.run_in_executor(self.workers, func, *args)
        result = future if future.done() else '<pending result>'
        logger.debug(f'{caller} <- {result} from work')
        return future

    async def run(self, max_runtime=10, keep_going=False):
        """Run until all jobs are finished.

        if keep_going is disabled (the default), the first failing job (i.e.
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
        to_start = reversed(list(self.jobs.values()))
        self.running = True
        for job in to_start:
            self._start_job(job)
        if keep_going:
            return_when = concurrent.futures.ALL_COMPLETED
        else:
            return_when = concurrent.futures.FIRST_EXCEPTION
        await asyncio.wait(
            self.results.values(),
            timeout=max_runtime,
            return_when=return_when,
        )
        if self.workers is not None:
            self.workers.shutdown()
        return self.results
