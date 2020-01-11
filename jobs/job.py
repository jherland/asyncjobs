import logging


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
