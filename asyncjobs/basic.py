import asyncio
import concurrent.futures
import logging
import time

logger = logging.getLogger(__name__)


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

    def __contains__(self, job_name):
        return job_name in self.jobs

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

    @staticmethod
    def _fate(future):
        """Return a word describing the state of the given future."""
        if not future.done():
            return 'unfinished'
        elif future.cancelled():
            return 'cancelled'
        elif future.exception() is not None:
            return 'failed'
        else:
            return 'success'

    def _start_job(self, job):
        self.tasks[job.name] = asyncio.create_task(job(self), name=job.name)
        self.event('start', job.name)
        self.tasks[job.name].add_done_callback(
            lambda task: self.event(
                'finish', job.name, {'fate': self._fate(task)}
            )
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
        caller = asyncio.current_task().get_name()
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
            raise asyncio.CancelledError
        self.event('awaited results', caller)
        logger.debug(f'{caller} <- {results} from .results()')
        return results

    async def _run_tasks(self, return_when):
        logger.info('Waiting for all jobs to complete…')
        self.event('await tasks', info={'jobs': list(self.tasks.keys())})
        try:
            while True:
                # Await the tasks that are currently running. More tasks may be
                # spawned while we're waiting, and those are not awaited here.
                await asyncio.wait(
                    self.tasks.values(), return_when=return_when
                )
                # We know _some_ task completed, successfully or not
                if return_when == concurrent.futures.FIRST_COMPLETED:
                    break
                elif return_when == concurrent.futures.FIRST_EXCEPTION:
                    # Figure out if there are failed tasks and we should return
                    if any(
                        t.done() and (t.cancelled() or t.exception())
                        for t in self.tasks.values()
                    ):
                        break
                # Otherwise return when all tasks are done
                if all(t.done() for t in self.tasks.values()):
                    break
        finally:
            # Any tasks left running at this point should be cancelled and
            # reaped/awaited _before_ we return from here
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
