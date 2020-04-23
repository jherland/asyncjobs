from . import polyfill  # noqa: F401
from . import external_work, signal_handling


class Job(external_work.Job, signal_handling.Job):
    pass


class Scheduler(external_work.Scheduler, signal_handling.Scheduler):
    pass
