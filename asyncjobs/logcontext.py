import asyncio
import contextlib
import logging
import threading


class LogContextDemuxer(logging.Handler):
    """A demultiplexer for log records based on async/thread context.

    This log handler is "installed" into a Logger object (typically the root
    logger), and redirects incoming log records to registered handlers (other
    logging.Handler instances) based on the current log context (i.e. which
    async task or worker thread made the log call).

    Tasks and threads register/deregister their log handlers with the
    .reset_context_handler() method (or use the .context_handler() context
    manager).

    Log records made from any other task/thread are sent to the fallback log
    handlers, which are the log handlers (if any) that were associated with
    the logger instance at install time. If None, we fall back to the
    logging.lastResort handler.
    """

    @staticmethod
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

    def __init__(self, *args, **kwargs):
        self.fallback_handlers = []
        self.context_handlers = {}  # context -> Handler object
        self._installed = False
        super().__init__(*args, **kwargs)

    def install(self, logger=None):
        """Install this demuxer into the given logger.

        If no logger is given, use the root logger.
        """
        assert not self._installed
        if logger is None:  # install into root logger
            logger = logging.getLogger()

        for handler in list(logger.handlers):
            assert handler is not self
            self.fallback_handlers.append(handler)
            logger.removeHandler(handler)
        logger.addHandler(self)
        self._installed = True

    def uninstall(self, logger=None):
        """Uninstall this demuxer from the given logger.

        If no logger is given, use the root logger.
        """
        assert self._installed
        if logger is None:  # uninstall from root logger
            logger = logging.getLogger()

        assert self in logger.handlers
        logger.removeHandler(self)
        for handler in self.fallback_handlers:
            logger.addHandler(handler)
        self.fallback_handlers = []
        self._installed = False

    @contextlib.contextmanager
    def installed(self, logger=None):
        """Provide a context where this demuxer is installed into 'logger'.

        If no logger is given, use the root logger.
        """
        assert not self.context_handlers
        self.install(logger)
        try:
            yield
        finally:
            self.uninstall(logger)
            assert not self.context_handlers

    def reset_context_handler(self, handler=None):
        """(Re)set the log handler for the current context."""
        context = self.current_context()
        if context is None:
            raise RuntimeError('Failed to determine current log context')

        if handler is None:  # remove existing handler
            self.context_handlers.pop(context, None)
        else:
            self.context_handlers[context] = handler

    @contextlib.contextmanager
    def context_handler(self, handler):
        """Provide a context in which the given log handler is set/active."""
        self.reset_context_handler(handler)
        try:
            yield
        finally:
            self.reset_context_handler()

    def _get_handlers(self):
        """Yield appropriate handlers for the current context.

        Yields the current context handler is set, otherwise yields each of
        the fallback handler. If no fallback handlers are set, yield the
        logging.lastResort handler.
        """
        try:
            yield self.context_handlers[self.current_context()]
        except KeyError:
            if self.fallback_handlers:
                yield from self.fallback_handlers
            else:
                yield logging.lastResort

    def _all_handlers(self):
        """Yield all handlers associated with this demuxer."""
        yield from self.context_handlers.values()
        yield from self.fallback_handlers

    def flush(self):
        for handler in self._all_handlers():
            handler.flush()
        super().flush()

    def close(self):
        for handler in self._all_handlers():
            handler.close()
        super().close()

    def emit(self, record):
        """Dispatch the log record to the appropriate handler."""
        for handler in self._get_handlers():
            if record.levelno >= handler.level:
                handler.handle(record)
