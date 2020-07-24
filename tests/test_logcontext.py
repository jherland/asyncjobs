import asyncio
import concurrent.futures
import logging
import pytest
import threading

from asyncjobs import logcontext

from conftest import ListHandler


def setup_test_logger(fmt=None):
    logger = logging.getLogger('test')
    handler = ListHandler()
    handler.setFormatter(logcontext.Formatter(fmt))
    logger.addHandler(handler)
    return logger, handler


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


@pytest.fixture
def no_context():
    return run_in_no_context


@pytest.fixture
def async_context():
    return run_in_async_context


@pytest.fixture
def thread_context():
    return run_in_thread_context


@pytest.fixture(
    params=[run_in_no_context, run_in_async_context, run_in_thread_context]
)
def any_context(request):
    return request.param


# Formatter


def test_formatter_with_no_decorators_does_not_decorate(any_context):
    logger, handler = setup_test_logger()
    any_context(logger.error, 'FOO')
    assert handler.messages == ['FOO']


def test_formatter_with_unrelated_decorator_does_not_decorate(any_context):
    logger, handler = setup_test_logger()
    with logcontext.Decorator.use(lambda line: f'>>>{line}<<<', 12345):
        any_context(logger.error, 'FOO')
    assert handler.messages == ['FOO']


def test_formatter_with_default_decorator_only_in_no_context(any_context):
    logger, handler = setup_test_logger()

    def in_context():
        with logcontext.Decorator.use(lambda line: f'>>>{line}<<<', None):
            logger.error('FOO')

    any_context(in_context)
    expect = ['>>>FOO<<<'] if any_context is run_in_no_context else ['FOO']
    assert handler.messages == expect


def test_formatter_with_context_decorator_decorates_in_context(any_context):
    logger, handler = setup_test_logger()

    def in_context():
        with logcontext.Decorator.use(lambda line: f'>>>{line}<<<'):
            logger.error('FOO')

    any_context(in_context)
    assert handler.messages == ['>>>FOO<<<']

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


# LogContextDemuxer forwards log records to appropriate handler


def test_LogContextDemuxer_no_context_uses_fallback_handler():
    demux = logcontext.LogContextDemuxer()
    root_handler = ListHandler(level=logging.ERROR)
    logging.getLogger().addHandler(root_handler)

    test_logger = logging.getLogger('test')
    test_logger.error('BEFORE')
    with demux.installed():
        test_logger.error('DURING')
    test_logger.error('AFTER')

    assert root_handler.messages == ['BEFORE', 'DURING', 'AFTER']


def test_LogContextDemuxer_handler_with_no_context_raises_RuntimeError():
    demux = logcontext.LogContextDemuxer()
    root_handler = ListHandler(level=logging.ERROR)
    logging.getLogger().addHandler(root_handler)

    test_handler = ListHandler(level=logging.ERROR)
    test_logger = logging.getLogger('test')

    test_logger.error('BEFORE')
    with demux.installed():
        with pytest.raises(RuntimeError):
            with demux.context_handler(test_handler):
                test_logger.error('DURING')
    test_logger.error('AFTER')

    assert root_handler.messages == ['BEFORE', 'AFTER']
    assert test_handler.messages == []


def test_LogContextDemuxer_async_context_without_handler_uses_fallback():
    demux = logcontext.LogContextDemuxer()
    root_handler = ListHandler(level=logging.ERROR)
    logging.getLogger().addHandler(root_handler)

    test_logger = logging.getLogger('test')

    async def coro():
        test_logger.error('DURING')

    with demux.installed():
        test_logger.error('BEFORE')
        asyncio.run(coro())
        test_logger.error('AFTER')

    assert root_handler.messages == ['BEFORE', 'DURING', 'AFTER']


def test_LogContextDemuxer_async_context_uses_custom_handler():
    demux = logcontext.LogContextDemuxer()
    root_handler = ListHandler(level=logging.ERROR)
    logging.getLogger().addHandler(root_handler)

    test_handler = ListHandler(level=logging.ERROR)
    test_logger = logging.getLogger('test')

    async def coro():
        with demux.context_handler(test_handler):
            test_logger.error('DURING')

    with demux.installed():
        test_logger.error('BEFORE')
        asyncio.run(coro())
        test_logger.error('AFTER')

    assert root_handler.messages == ['BEFORE', 'AFTER']
    assert test_handler.messages == ['DURING']


def test_LogContextDemuxer_thread_context_without_handler_uses_fallback():
    demux = logcontext.LogContextDemuxer()
    root_handler = ListHandler(level=logging.ERROR)
    logging.getLogger().addHandler(root_handler)

    test_logger = logging.getLogger('test')

    def in_thread():
        test_logger.error('DURING')

    with demux.installed():
        test_logger.error('BEFORE')
        thread = threading.Thread(target=in_thread)
        thread.start()
        thread.join()
        test_logger.error('AFTER')

    assert root_handler.messages == ['BEFORE', 'DURING', 'AFTER']


def test_LogContextDemuxer_thread_context_uses_custom_handler():
    demux = logcontext.LogContextDemuxer()
    root_handler = ListHandler(level=logging.ERROR)
    logging.getLogger().addHandler(root_handler)

    test_handler = ListHandler(level=logging.ERROR)
    test_logger = logging.getLogger('test')

    def in_thread():
        with demux.context_handler(test_handler):
            test_logger.error('DURING')

    with demux.installed():
        test_logger.error('BEFORE')
        thread = threading.Thread(target=in_thread)
        thread.start()
        thread.join()
        test_logger.error('AFTER')

    assert root_handler.messages == ['BEFORE', 'AFTER']
    assert test_handler.messages == ['DURING']


# setting handler levels filters log records appropriately


def test_LogContextDemuxer_level_affects_fallback_handler():
    demux = logcontext.LogContextDemuxer(level=logging.ERROR)
    root_handler = ListHandler(level=logging.WARNING)
    logging.getLogger().addHandler(root_handler)

    test_logger = logging.getLogger('test')
    # only root_handler level in effect
    test_logger.error('BEFORE')
    test_logger.warning('before')
    with demux.installed():  # introduce demux level
        test_logger.error('DURING')
        test_logger.warning('during')  # stopped by demux level
    # only root_handler level in effect
    test_logger.error('AFTER')
    test_logger.warning('after')

    assert root_handler.messages == [
        'BEFORE',
        'before',
        'DURING',
        'AFTER',
        'after',
    ]


def test_LogContextDemuxer_obeys_fallback_handler_level():
    demux = logcontext.LogContextDemuxer(level=logging.WARNING)
    root_handler = ListHandler(level=logging.ERROR)
    logging.getLogger().addHandler(root_handler)

    test_logger = logging.getLogger('test')
    test_logger.error('BEFORE')
    test_logger.warning('before')  # stopped by root_handler level
    with demux.installed():
        test_logger.error('DURING')
        test_logger.warning('during')  # stopped by root_handler level
    test_logger.error('AFTER')
    test_logger.warning('after')  # stopped by root_handler level

    assert root_handler.messages == ['BEFORE', 'DURING', 'AFTER']


def test_LogContextDemuxer_level_affects_custom_handler():
    demux = logcontext.LogContextDemuxer(level=logging.ERROR)
    root_handler = ListHandler(level=logging.WARNING)
    logging.getLogger().addHandler(root_handler)

    test_handler = ListHandler(level=logging.WARNING)
    test_logger = logging.getLogger('test')

    async def coro():
        with demux.context_handler(test_handler):
            test_logger.error('DURING')
            test_logger.warning('during')  # stopped by demux level

    with demux.installed():
        test_logger.error('BEFORE')
        test_logger.warning('before')  # stopped by demux level
        asyncio.run(coro())
        test_logger.error('AFTER')
        test_logger.warning('after')  # stopped by demux level

    assert root_handler.messages == ['BEFORE', 'AFTER']
    assert test_handler.messages == ['DURING']


def test_LogContextDemuxer_obeys_custom_handler_level():
    demux = logcontext.LogContextDemuxer(level=logging.WARNING)
    root_handler = ListHandler(level=logging.WARNING)
    logging.getLogger().addHandler(root_handler)

    test_handler = ListHandler(level=logging.ERROR)
    test_logger = logging.getLogger('test')

    async def coro():
        with demux.context_handler(test_handler):
            test_logger.error('DURING')
            test_logger.warning('during')  # stopped by test_handler level

    with demux.installed():
        test_logger.error('BEFORE')
        test_logger.warning('before')
        asyncio.run(coro())
        test_logger.error('AFTER')
        test_logger.warning('after')

    assert root_handler.messages == [
        'BEFORE',
        'before',
        'AFTER',
        'after',
    ]
    assert test_handler.messages == ['DURING']


# setting formatters formats log records appropriately


def test_LogContextDemuxer_no_context_uses_fallback_formatter():
    demux = logcontext.LogContextDemuxer(level=logging.ERROR)
    root_handler = ListHandler()
    root_handler.setFormatter(logging.Formatter('root<%(message)s>'))
    logging.getLogger().addHandler(root_handler)

    test_logger = logging.getLogger('test')
    test_logger.error('BEFORE')
    with demux.installed():
        test_logger.error('DURING')
    test_logger.error('AFTER')

    assert root_handler.messages == [
        'root<BEFORE>',
        'root<DURING>',
        'root<AFTER>',
    ]


def test_LogContextDemuxer_async_context_handler_uses_no_formatter():
    demux = logcontext.LogContextDemuxer(level=logging.ERROR)
    root_handler = ListHandler()
    root_handler.setFormatter(logging.Formatter('root<%(message)s>'))
    logging.getLogger().addHandler(root_handler)

    test_handler = ListHandler()
    test_logger = logging.getLogger('test')

    async def coro():
        with demux.context_handler(test_handler):
            test_logger.error('DURING')

    with demux.installed():
        test_logger.error('BEFORE')
        asyncio.run(coro())
        test_logger.error('AFTER')

    assert root_handler.messages == ['root<BEFORE>', 'root<AFTER>']
    assert test_handler.messages == ['DURING']


def test_LogContextDemuxer_async_context_handlers_uses_own_formatters():
    demux = logcontext.LogContextDemuxer(level=logging.ERROR)
    root_handler = ListHandler()
    root_handler.setFormatter(logging.Formatter('root<%(message)s>'))
    logging.getLogger().addHandler(root_handler)

    test1_handler = ListHandler()
    test1_handler.setFormatter(logging.Formatter('test1<%(message)s>'))
    test2_handler = ListHandler()
    test2_handler.setFormatter(logging.Formatter('test2<%(message)s>'))
    test_logger = logging.getLogger('test')

    async def coro1():
        with demux.context_handler(test1_handler):
            test_logger.error('BEFORE')
            await asyncio.create_task(coro2())
            test_logger.error('AFTER')

    async def coro2():
        with demux.context_handler(test2_handler):
            test_logger.error('DURING')

    with demux.installed():
        test_logger.error('START')
        asyncio.run(coro1())
        test_logger.error('END')

    assert root_handler.messages == ['root<START>', 'root<END>']
    assert test1_handler.messages == ['test1<BEFORE>', 'test1<AFTER>']
    assert test2_handler.messages == ['test2<DURING>']


def test_LogContextDemuxer_thread_context_handlers_uses_own_formatters():
    demux = logcontext.LogContextDemuxer(level=logging.ERROR)
    root_handler = ListHandler()
    root_handler.setFormatter(logging.Formatter('root<%(message)s>'))
    logging.getLogger().addHandler(root_handler)

    test1_handler = ListHandler()
    test1_handler.setFormatter(logging.Formatter('test1<%(message)s>'))
    test2_handler = ListHandler()
    test2_handler.setFormatter(logging.Formatter('test2<%(message)s>'))
    test_logger = logging.getLogger('test')

    def thread1():
        with demux.context_handler(test1_handler):
            test_logger.error('BEFORE')
            thread = threading.Thread(target=thread2)
            thread.start()
            thread.join()
            test_logger.error('AFTER')

    def thread2():
        with demux.context_handler(test2_handler):
            test_logger.error('DURING')

    with demux.installed():
        test_logger.error('START')
        thread = threading.Thread(target=thread1)
        thread.start()
        thread.join()
        test_logger.error('END')

    assert root_handler.messages == ['root<START>', 'root<END>']
    assert test1_handler.messages == ['test1<BEFORE>', 'test1<AFTER>']
    assert test2_handler.messages == ['test2<DURING>']


def test_LogContextDemuxer_async_context_handlers_inherit_demux_formatter():
    demux = logcontext.LogContextDemuxer(level=logging.ERROR)
    demux.setFormatter(logging.Formatter('demux<%(message)s>'))
    root_handler = ListHandler()
    root_handler.setFormatter(logging.Formatter('root<%(message)s>'))
    logging.getLogger().addHandler(root_handler)

    test1_handler = ListHandler()
    test1_handler.setFormatter(logging.Formatter('test1<%(message)s>'))
    test2_handler = ListHandler()  # inherits demux.formatter
    test_logger = logging.getLogger('test')

    async def coro1():
        with demux.context_handler(test1_handler):
            test_logger.error('BEFORE')
            await asyncio.create_task(coro2())
            test_logger.error('AFTER')

    async def coro2():
        with demux.context_handler(test2_handler):
            test_logger.error('DURING')

    with demux.installed():
        test_logger.error('START')
        asyncio.run(coro1())
        test_logger.error('END')

    assert root_handler.messages == ['root<START>', 'root<END>']
    assert test1_handler.messages == ['test1<BEFORE>', 'test1<AFTER>']
    assert test2_handler.messages == ['demux<DURING>']


def test_LogContextDemuxer_can_copy_formatter_from_fallback_and_propagate():
    demux = logcontext.LogContextDemuxer(level=logging.ERROR)
    demux.setFormatter(logging.Formatter('demux<%(message)s>'))  # overwritten
    root_handler = ListHandler()
    root_handler.setFormatter(logging.Formatter('root<%(message)s>'))
    logging.getLogger().addHandler(root_handler)

    test1_handler = ListHandler()
    test1_handler.setFormatter(logging.Formatter('test1<%(message)s>'))
    test2_handler = ListHandler()  # inherits demux.formatter <- root formatter
    test_logger = logging.getLogger('test')

    async def coro1():
        with demux.context_handler(test1_handler):
            test_logger.error('BEFORE')
            await asyncio.create_task(coro2())
            test_logger.error('AFTER')

    async def coro2():
        with demux.context_handler(test2_handler):
            test_logger.error('DURING')

    with demux.installed(copy_formatter=True):  # overwrites demux.formatter
        test_logger.error('START')
        asyncio.run(coro1())
        test_logger.error('END')

    assert root_handler.messages == ['root<START>', 'root<END>']
    assert test1_handler.messages == ['test1<BEFORE>', 'test1<AFTER>']
    assert test2_handler.messages == ['root<DURING>']
