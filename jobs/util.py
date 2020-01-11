import asyncio


def current_task():
    if hasattr(asyncio, 'current_task'):  # Added in Python v3.7
        return asyncio.current_task()
    else:
        return asyncio.Task.current_task()


def current_task_name():
    task = current_task()
    if hasattr(asyncio.Task, 'get_name'):  # Added in Python v3.8
        return task.get_name()
    elif hasattr(task, 'job_name'):  # Added by Scheduler._start_job
        return task.job_name
    else:
        return None


def fate(future):
    """Return a word describing the state of the given future."""
    if not future.done():
        return 'unfinished'
    elif future.cancelled():
        return 'cancelled'
    elif future.exception() is not None:
        return 'failed'
    else:
        return 'success'
