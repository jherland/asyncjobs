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


# asyncio.get_running_loop() added in Python v3.7
if not hasattr(asyncio, 'get_running_loop'):
    # The following was pilfered from Python v3.8's asyncio.events:
    # https://github.com/python/cpython/blob/3.8/Lib/asyncio/events.py

    def get_running_loop():
        loop = asyncio.events._get_running_loop()
        if loop is None:
            raise RuntimeError('no running event loop')
        return loop

    setattr(asyncio, 'get_running_loop', get_running_loop)


# asyncio.current_task() added in Python v3.7
if not hasattr(asyncio, 'current_task'):
    # The following was pilfered from Python v3.8's asyncio.tasks:
    # https://github.com/python/cpython/blob/3.8/Lib/asyncio/tasks.py

    def current_task(loop=None):
        if loop is None:
            loop = asyncio.get_running_loop()
        return asyncio.Task.current_task(loop)

    setattr(asyncio, 'current_task', current_task)


# asyncio.all_tasks() added in Python v3.7
if not hasattr(asyncio, 'all_tasks'):

    def all_tasks(loop=None):
        if loop is None:
            loop = asyncio.get_running_loop()
        return {t for t in asyncio.Task.all_tasks(loop) if not t.done()}

    setattr(asyncio, 'all_tasks', all_tasks)


# asyncio.run() added in Python v3.7
if not hasattr(asyncio, 'run'):
    # The following was pilfered from Python v3.8's asyncio.runners:
    # https://github.com/python/cpython/blob/3.8/Lib/asyncio/runners.py

    def _cancel_all_tasks(loop):
        to_cancel = {t for t in asyncio.Task.all_tasks(loop) if not t.done()}
        if not to_cancel:
            return

        for task in to_cancel:
            task.cancel()

        loop.run_until_complete(
            asyncio.tasks.gather(*to_cancel, loop=loop, return_exceptions=True)
        )

        for task in to_cancel:
            if task.cancelled():
                continue
            if task.exception() is not None:
                message = 'unhandled exception during asyncio.run() shutdown'
                loop.call_exception_handler(
                    {
                        'message': message,
                        'exception': task.exception(),
                        'task': task,
                    }
                )

    def run(main, *, debug=False):
        if asyncio.events._get_running_loop() is not None:
            raise RuntimeError(
                "asyncio.run() cannot be called from a running event loop"
            )

        if not asyncio.coroutines.iscoroutine(main):
            raise ValueError("a coroutine was expected, got {!r}".format(main))

        loop = asyncio.events.new_event_loop()
        # We want all tasks created/returned to have name support (see above)
        loop.set_task_factory(lambda loop, coro: asyncio.Task(coro, loop=loop))
        try:
            asyncio.events.set_event_loop(loop)
            loop.set_debug(debug)
            return loop.run_until_complete(main)
        finally:
            try:
                _cancel_all_tasks(loop)
                loop.run_until_complete(loop.shutdown_asyncgens())
            finally:
                asyncio.events.set_event_loop(None)
                loop.close()

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


# contextlib.nullcontext added in Python v3.7
if not hasattr(contextlib, 'nullcontext'):

    @contextlib.contextmanager
    def nullcontext(enter_result=None):
        yield enter_result

    setattr(contextlib, 'nullcontext', nullcontext)
