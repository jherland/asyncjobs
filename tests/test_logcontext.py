import asyncio
import logging
import pytest
import threading

from asyncjobs import logcontext

from conftest import ListHandler


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

    assert list(root_handler.messages()) == ['BEFORE', 'DURING', 'AFTER']


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

    assert list(root_handler.messages()) == ['BEFORE', 'AFTER']
    assert list(test_handler.messages()) == []


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

    assert list(root_handler.messages()) == ['BEFORE', 'DURING', 'AFTER']


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

    assert list(root_handler.messages()) == ['BEFORE', 'AFTER']
    assert list(test_handler.messages()) == ['DURING']


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

    assert list(root_handler.messages()) == ['BEFORE', 'DURING', 'AFTER']


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

    assert list(root_handler.messages()) == ['BEFORE', 'AFTER']
    assert list(test_handler.messages()) == ['DURING']


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

    assert list(root_handler.messages()) == [
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

    assert list(root_handler.messages()) == ['BEFORE', 'DURING', 'AFTER']


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

    assert list(root_handler.messages()) == ['BEFORE', 'AFTER']
    assert list(test_handler.messages()) == ['DURING']


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

    assert list(root_handler.messages()) == [
        'BEFORE',
        'before',
        'AFTER',
        'after',
    ]
    assert list(test_handler.messages()) == ['DURING']
