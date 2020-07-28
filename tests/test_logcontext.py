import asyncio
import concurrent.futures
import pytest

from asyncjobs import logcontext


def run_in_no_context(func, *args, **kwargs):
    return func(*args, **kwargs)


def run_in_async_context(func, *args, **kwargs):
    async def coro():
        return func(*args, **kwargs)

    return asyncio.run(coro())


def run_in_thread_context(func, *args, **kwargs):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(func, *args, **kwargs)
        return future.result()


@pytest.fixture(
    params=[run_in_no_context, run_in_async_context, run_in_thread_context]
)
def run_in_any_context(request):
    return request.param


class TestFormatterInAnyContext:
    @pytest.fixture(autouse=True)
    def setup(self, run_in_any_context, logger_with_listhandler):
        self.logger, self.handler = logger_with_listhandler
        self.handler.setFormatter(logcontext.Formatter('[%(message)s]'))
        self.run_in_context = run_in_any_context

    def test_no_decorators_does_not_decorate(self):
        self.run_in_context(self.logger.error, 'FOO')
        assert self.handler.messages == ['[FOO]']

    def test_unrelated_decorator_does_not_decorate(self):
        with logcontext.Decorator.use(lambda line: f'>>>{line}<<<', 12345):
            self.run_in_context(self.logger.error, 'FOO')
        assert self.handler.messages == ['[FOO]']

    def test_default_decorator_only_in_no_context(self):
        def in_context():
            with logcontext.Decorator.use(lambda line: f'>>>{line}<<<', None):
                self.logger.error('FOO')

        self.run_in_context(in_context)
        if self.run_in_context is run_in_no_context:
            assert self.handler.messages == ['>>>[FOO]<<<']
        else:
            assert self.handler.messages == ['[FOO]']

    def test_context_decorator_decorates_in_context(self):
        def in_context():
            with logcontext.Decorator.use(lambda line: f'>>>{line}<<<'):
                self.logger.error('FOO')

        self.run_in_context(in_context)
        assert self.handler.messages == ['>>>[FOO]<<<']

    def test_use_None_as_context_decorator(self):
        def in_context():
            with logcontext.Decorator.use(None):
                self.logger.error('FOO')

        self.run_in_context(in_context)
        assert self.handler.messages == ['[FOO]']

    def test_use_string_as_context_decorator(self):
        def in_context():
            with logcontext.Decorator.use('❰❰❰{}❱❱❱'):
                self.logger.error('FOO')

        self.run_in_context(in_context)
        assert self.handler.messages == ['❰❰❰[FOO]❱❱❱']

    def test_use_bytes_as_context_decorator(self):
        def in_context():
            with logcontext.Decorator.use(b'>>>'):
                self.logger.error('FOO')

        self.run_in_context(in_context)
        assert self.handler.messages == ['>>>[FOO]']
