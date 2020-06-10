from . import polyfill  # noqa: F401
from . import external_work, signal_handling


class Scheduler(external_work.Scheduler, signal_handling.Scheduler):
    pass
