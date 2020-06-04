from . import polyfill  # noqa: F401
from . import basic, external_work, signal_handling


class Job(basic.Job):
    pass


class Scheduler(external_work.Scheduler, signal_handling.Scheduler):
    pass
