import asyncio
import contextlib
import logging
import threading

NotSet = object()


def current_context():
    """Return a hashable identifier for the current log context.

    Use the id of the current task in async contexts, or the id of the
    current thread in threaded contexts. Otherwise return None.
    """
    obj = None
    if threading.current_thread() is not threading.main_thread():
        # we're in a (worker) thread
        obj = threading.current_thread()
    else:  # log context is determined by current task
        with contextlib.suppress(RuntimeError):
            obj = asyncio.current_task()
    return None if obj is None else id(obj)


class Decorator:
    registry = {}  # context -> decorator function

    @classmethod
    def from_string(cls, pattern):
        try:
            assert '\0' not in pattern
            prefix, suffix = pattern.format('\0').split('\0')
        except (AssertionError, ValueError, IndexError):
            prefix, suffix = pattern, ''

        return lambda line: prefix + line.rstrip() + suffix.rstrip()

    @classmethod
    def from_bytes(cls, pattern):
        return cls.from_string(
            pattern.decode('utf-8', errors='surrogateescape')
        )

    @classmethod
    def add(cls, decorator, context=NotSet):
        context = current_context() if context is NotSet else context
        if decorator is None:  # no decoration
            return cls.remove(context)
        elif isinstance(decorator, str):  # 'prefix{}suffix'
            decorator = cls.from_string(decorator)
        elif isinstance(decorator, (bytes, bytearray)):  # b'prefix{}suffix'
            decorator = cls.from_bytes(decorator)
        else:
            assert callable(decorator)
        cls.registry[context] = decorator

    @classmethod
    def get(cls, context=NotSet, *, default=None):
        context = current_context() if context is NotSet else context
        return cls.registry.get(context, default)

    @classmethod
    def remove(cls, context=NotSet):
        context = current_context() if context is NotSet else context
        return cls.registry.pop(context, None)

    @classmethod
    @contextlib.contextmanager
    def use(cls, decorator, context=NotSet):
        context = current_context() if context is NotSet else context
        cls.add(decorator, context)
        try:
            yield
        finally:
            cls.remove(context)

    @classmethod
    def apply(cls, line, context=NotSet):
        return cls.get(context, default=lambda line: line)(line)


class Formatter(logging.Formatter):
    """Decorate log records with the registered context decorators."""

    def format(self, record):
        context = getattr(record, 'context', current_context())
        return Decorator.apply(super().format(record), context)
