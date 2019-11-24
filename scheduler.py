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
        raise NotImplementedError


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
        raise NotImplementedError


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
        self.logger.debug('Awaiting dependencies {self.deps}...')
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
        return asyncio.Task.current_task().job_name

    def _start_job(self, job):
        task = asyncio.ensure_future(job(self))
        task.job_name = job.name
        self.results[job.name] = task
        #  logger.info(f'Started job {job}')

    def add(self, job):
        """Add a job to be run.

        If we're already running (i.e. inside .run()) schedule the job
        immediately, otherwise the job will be scheduled by .run().
        """
        assert job.name not in self.jobs
        self.jobs[job.name] = job
        #  logger.info(f'Added job {job}')
        if self.running:
            self._start_job(job)

    async def result(self, job_name):
        """Wait until the given job is finished, and return its result."""
        assert self.running
        caller = self._caller()
        assert job_name in self.results
        if not self.results[job_name].done():
            logger.info(f'  {caller} is waiting on {job_name}...')
        result = await (self.results[job_name])
        logger.info(f'  {job_name} done, returning {result} to {caller}')
        return result

    def do_in_worker(self, func, *args):
        """Call 'func(*args)' in a worker and return its future result."""
        assert self.running
        logger.info(f'Submit to worker: {func}(*{args})')
        loop = asyncio.get_running_loop()
        if self.workers is None:  # single-threaded, run synchronously
            future = loop.create_future()
            try:
                future.set_result(func(*args))
            except Exception as e:
                future.set_exception(e)
        else:
            future = loop.run_in_executor(self.workers, func, *args)
        logger.info(f'Returned {future} from worker')
        return future

    async def run(self, max_runtime=10, stop_on_first_error=False):
        """Run until all jobs are finished."""
        logger.info('Running')
        to_start = reversed(list(self.jobs.values()))
        self.running = True
        for job in to_start:
            self._start_job(job)
        if stop_on_first_error:
            return_when = concurrent.futures.FIRST_EXCEPTION
        else:
            return_when = concurrent.futures.ALL_COMPLETED
        await asyncio.wait(
            self.results.values(),
            timeout=max_runtime,
            return_when=return_when,
        )
        if self.workers is not None:
            self.workers.shutdown()
        return self.results
