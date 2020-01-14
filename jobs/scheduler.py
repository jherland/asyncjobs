import asyncio
import concurrent.futures
import logging
import time

from .util import current_task_name, fate

logger = logging.getLogger(__name__)


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
            task = asyncio.ensure_future(job(self))  # .create_task() in >=v3.7
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
