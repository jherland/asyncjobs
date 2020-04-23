"""Add missing async functionality to Python v3.6 and v3.7."""
import asyncio
import contextlib


# asyncio.Task names added in Python v3.8
if not hasattr(asyncio.Task, 'get_name'):

    class Task(asyncio.Task):
        num_tasks = 0

        def __init__(self, coro, *, loop=None, name=None):
            super().__init__(coro, loop=loop)

            self.__class__.num_tasks += 1
            if name is None:
                self._name = f'Task-{self.__class__.num_tasks}'
            else:
                self._name = str(name)

        def get_name(self):
            return self._name

        def set_name(self, value):
            self._name = str(value)

    def create_task(coro, *, name=None):
        return asyncio.ensure_future(Task(coro, name=name))

    setattr(asyncio, 'Task', Task)
    setattr(asyncio, 'create_task', create_task)


# asyncio.current_task() added in Python v3.7
if not hasattr(asyncio, 'current_task'):

    def current_task():
        return asyncio.Task.current_task()

    setattr(asyncio, 'current_task', current_task)


# asyncio.get_running_loop() added in Python v3.7
if not hasattr(asyncio, 'get_running_loop'):

    def get_running_loop():
        return asyncio.get_event_loop()

    setattr(asyncio, 'get_running_loop', get_running_loop)


# asyncio.run() added in Python v3.7
if not hasattr(asyncio, 'run'):

    def run(main, *, debug=False):
        loop = asyncio.get_event_loop()
        ret = loop.run_until_complete(main)
        loop.close()
        return ret

    setattr(asyncio, 'run', run)


# contextlib.asynccontextmanager added in Python v3.7
if not hasattr(contextlib, 'asynccontextmanager'):
    # The following was pilfered and simplified from Python v3.8's contextlib:
    # https://github.com/python/cpython/blob/3.8/Lib/contextlib.py
    from functools import wraps

    class _AsyncGeneratorContextManager:
        def __init__(self, func, args, kwds):
            self.gen = func(*args, **kwds)
            self.func, self.args, self.kwds = func, args, kwds

        async def __aenter__(self):
            try:
                return await self.gen.__anext__()
            except StopAsyncIteration:
                raise RuntimeError("generator didn't yield") from None

        async def __aexit__(self, typ, value, traceback):
            if typ is None:
                try:
                    await self.gen.__anext__()
                except StopAsyncIteration:
                    return
                else:
                    raise RuntimeError("generator didn't stop")
            else:
                if value is None:
                    value = typ()
                # See _GeneratorContextManager.__exit__ for comments on
                # subtleties in this implementation
                try:
                    await self.gen.athrow(typ, value, traceback)
                    raise RuntimeError("generator didn't stop after athrow()")
                except StopAsyncIteration as exc:
                    return exc is not value
                except RuntimeError as exc:
                    if exc is value:
                        return False
                    # Avoid suppressing if a StopIteration exception
                    # was passed to throw() and later wrapped into a
                    #  RuntimeError (see PEP 479 for sync generators; async
                    # generators also have this behavior). But do this only if
                    # the exception wrapped by the RuntimeError is actully
                    # Stop(Async)Iteration (see issue29692).
                    if isinstance(value, (StopIteration, StopAsyncIteration)):
                        if exc.__cause__ is value:
                            return False
                    raise
                except BaseException as exc:
                    if exc is not value:
                        raise

    def asynccontextmanager(func):
        @wraps(func)
        def helper(*args, **kwds):
            return _AsyncGeneratorContextManager(func, args, kwds)

        return helper

    setattr(contextlib, 'asynccontextmanager', asynccontextmanager)
