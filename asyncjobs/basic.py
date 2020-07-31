"""Basic job scheduler.

A 'job' is a _coroutine_ with a _name_, and a set of _dependencies_:

Coroutine

Jobs are executed in an asynchronous context. As usual in async programming,
a coroutine should never directly invoke any long, blocking operations, as
this affects the entire async framework and prevents other jobs from
progressing. Such work must instead be 'await'ed. See the external_work module
for an extension to this module which provides helpers for starting and
awaiting synchronous work in worker threads and/or subprocesses.

A job coroutine may finish in one of three ways:
  - complete successfully, by returning a value.
  - fail, by raising an exception which aborts the coroutine.
  - get cancelled, in which case this framework raises a CancelledError inside
    the coroutine, causing it to abort.

Name

Jobs have a name that is unique to the Scheduler instance.
The name is used in several contexts:
  - When jobs specify dependency relationships between eachother.
  - In events emitted while Scheduler.run() is executing
  - As a key in the job results dict returned from Scheduler.run()

Dependencies

Jobs may specify a set of dependencies which are names of other jobs that must
finish executing successfully before this job can proceed. The results of
those other jobs are made available in the Context object passed to the job
coroutine. If any of the dependencies fail (either by exception, or by getting
cancelled), then this job will also be cancelled.

Scheduler

Jobs are created by calling Scheduler.add_job(). When the Scheduler.run() is
called, each of the added jobs/coroutines will be invoked once with a Context
object (see below) and wrapped in an asyncio.Task
object which starts it running concurrently with other coroutines.

The Scheduler.run() ends when all jobs have finished (one way or another) and
returns a dict mapping job names to their corresponding asyncio.Future objects
that encapsulate their result (return value, exception, or cancellation).

This module implements the most basic Scheduler. Other modules in this package
extend the Scheduler class (and accompanying Context class) with more features.

Context

Each job's coroutine is called with a single argument: ctx, which is a Context
object the job may use to interact with the Scheduler or other parts of the
surrounding framework. For example:

  - While running, a job may await the result of one or more other jobs by
    passing their names to ctx.results() (this is also how dependencies between
    jobs are implemented): A job 'foo' may depend on jobs 'bar' and 'baz' by
    awaiting their results:

        results = ctx.results('bar', 'baz')

  - A job may spawn another job by calling ctx.add_job().
"""
import asyncio
import concurrent.futures
import logging
import time

logger = logging.getLogger(__name__)


class Context:
    """Helper class instantiated per job and passed to the job coroutine.

    This is the interface available to the job coroutine for communicating
    with the Scheduler, and other parts of the surrounding scheduler
    environment.
    """

    def __init__(self, name, scheduler):
        self.name = name
        self.deps = None  # set by the coroutine wrapper in Scheduler.add_job()
        self._scheduler = scheduler

    def event(self, event, **kwargs):
        """Emit a scheduling event from this job."""
        self._scheduler.event(event, job=self.name, **kwargs)

    async def results(self, *job_names):
        """Wait until the given jobs are finished, and return their results.

        Returns a dictionary mapping job names to their results.

        Raise KeyError if any of the given jobs have not been added to the
        scheduler. If any of the given jobs fail (either by raising an
        exception or by being cancelled) then raise CancelledError here to
        cancel this job as a consequence.
        """
        assert self._scheduler.running
        tasks = [self._scheduler.tasks[n] for n in job_names]
        pending = [n for n, t in zip(job_names, tasks) if not t.done()]
        self.event('await results', jobs=list(job_names), pending=pending)
        if pending:
            logger.debug(f'{self.name} waiting for {", ".join(pending)}…')
        try:
            results = dict(zip(job_names, await asyncio.gather(*tasks)))
        except Exception:
            logger.info(f'{self.name} cancelled due to failed dependency')
            raise asyncio.CancelledError
        self.event('awaited results')
        logger.debug(f'{self.name} returning {results} from .results()')
        return results

    def add_job(self, name, coro, deps=None):
        """Add a job to be run.

        Schedule the job to start immediately. Raise ValueError if a job with
        the same name has already been added.
        """
        self._scheduler.add_job(name, coro, deps)


class Scheduler:
    """Async job scheduler. Run job coroutines as concurrent tasks.

    Jobs are added by passing their names and coroutines to .add_job(). The
    coroutines will be scheduled for execution when .run() is called. While
    running, jobs may use the Context object passed to the coroutine to await
    each other's results or spawn new jobs. When all jobs are completed
    (successfully or otherwise), .run() will return a dictionary with all the
    job results.
    """

    def __init__(self, *, event_handler=None, context_class=Context):
        self.jobs = {}  # job name -> coroutine
        self.tasks = {}  # job name -> Task object, aka. (future) job result
        self.running = False
        self.event_handler = event_handler

        assert issubclass(context_class, Context)
        self.context_class = context_class

    def __contains__(self, job_name):
        return job_name in self.jobs

    def event(self, event, **kwargs):
        if self.event_handler is None:
            return
        d = {'timestamp': time.time(), 'event': event}
        d.update(kwargs)
        logger.debug(f'Posting event: {d}')
        self.event_handler(d)

    @staticmethod
    def _fate(future):
        """Return a word describing the state of the given future."""
        if not future.done():
            return 'unfinished'
        elif future.cancelled():
            return 'cancelled'
        elif future.exception() is not None:
            logger.warning(f'FAILED future: {future}:')
            import traceback

            e = future.exception()
            lines = traceback.format_exception(type(e), e, None)
            logger.warning(''.join(lines).rstrip())
            return 'failed'
        else:
            return 'success'

    def _start_job(self, name):
        assert name in self.jobs
        assert name not in self.tasks

        ctx = self.context_class(name, self)
        task = asyncio.create_task(self.jobs[name](ctx), name=name)
        self.tasks[name] = task
        self.event('start', job=name)
        task.add_done_callback(
            lambda task: self.event('finish', job=name, fate=self._fate(task))
        )

    def add_job(self, name, coro, deps=None):
        """Add a job (aka. named coroutine) to be run.

        If 'deps' is given, it must be a set of names of other jobs that will
        be awaited before coro is started. The results of those jobs is made
        available in the ctx.deps dict.

        If we're already running (i.e. inside .run()) schedule the job
        immediately, otherwise the job will be scheduled when .run() is called.
        Raise ValueError if a job with the same name has already been added.
        """
        if name in self.jobs:
            raise ValueError(f'Cannot add job named {name}. Already added!')

        logger.debug(f'Adding job named {name} with deps={deps!r}')
        if deps is not None:

            async def deps_before_coro(ctx, wrapped_coro=coro):
                assert ctx.deps is None
                logger.debug(f'Awaiting dependencies: {deps}…')
                ctx.deps = await ctx.results(*deps)
                return await wrapped_coro(ctx)

            coro = deps_before_coro

        self.jobs[name] = coro
        self.event('add', job=name)
        if self.running:
            self._start_job(name)

    async def _run_tasks(self, return_when):
        logger.info('Waiting for all jobs to complete…')
        self.event('await tasks', jobs=list(self.tasks.keys()))
        shutting_down = False
        while any(not t.done() for t in self.tasks.values()):
            # NEVER exit this loop while there are tasks still running.
            try:
                # Await the tasks that are currently running. More tasks may be
                # spawned while we're waiting, and those are not awaited here.
                await asyncio.wait(
                    self.tasks.values(), return_when=return_when
                )
                # We know _some_ task completed, successfully or not
                assert any(t.done() for t in self.tasks.values())

                if not shutting_down:
                    # It is time to shut down, yet?
                    if return_when == concurrent.futures.FIRST_COMPLETED:
                        shutting_down = True
                    elif return_when == concurrent.futures.FIRST_EXCEPTION:
                        shutting_down = any(
                            t.done() and (t.cancelled() or t.exception())
                            for t in self.tasks.values()
                        )
                    else:
                        assert return_when == concurrent.futures.ALL_COMPLETED
                        shutting_down = all(
                            t.done() for t in self.tasks.values()
                        )
            except BaseException as e:
                logger.warning(f'{self.__class__.__name__} aborted by {e!r}')
                shutting_down = True
            finally:
                if shutting_down:
                    # Keep cancelling tasks until all are finished
                    logger.info('Shutting down…')
                    self.event('cancelling tasks')
                    return_when = concurrent.futures.ALL_COMPLETED
                    for name, task in self.tasks.items():
                        if not task.done():
                            logger.info(f'Cancelling {name}…')
                            task.cancel()
        self.event('awaited tasks')

    async def run(self, *, keep_going=False):
        """Run until all jobs are finished.

        If keep_going is disabled (the default), the first failing job (i.e.
        a job that raises an exception) will cause us to cancel all other
        concurrent and remaining jobs and return as soon as possible.

        If keep_going is enabled, we will keep running as long as there are
        jobs to do. Only the jobs that depend (directly or indirectly) on the
        failing job(s) will be cancelled.

        Return a dictionary mapping job names to the corresponding
        asyncio.Future objects that encapsulate their result (return value,
        exception, or cancellation).
        """
        logger.debug('Running…')
        self.event('start', keep_going=keep_going, num_jobs=len(self.jobs))
        if keep_going:
            return_when = concurrent.futures.ALL_COMPLETED
        else:
            return_when = concurrent.futures.FIRST_EXCEPTION

        if self.jobs:
            for name in self.jobs.keys():
                self._start_job(name)

            self.running = True
            await self._run_tasks(return_when)
            self.running = False
        else:
            logger.warning('Nothing to do!')
        self.event('finish', num_tasks=len(self.tasks))
        return self.tasks
