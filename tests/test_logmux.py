import asyncio
import pytest

from asyncjobs.logmux import LogMux

pytestmark = pytest.mark.asyncio


async def test_no_output(verify_output):
    async with LogMux():
        pass
    assert verify_output([[]])


async def test_one_stream_undecorated(verify_output):
    async with LogMux() as logmux, logmux.new_stream() as f:
        print('This is the first line', file=f)
        print('This is the second line', file=f)
    assert verify_output(
        [['This is the first line', 'This is the second line']]
    )


async def test_two_streams_undecorated(verify_output):
    async with LogMux() as logmux, logmux.new_stream() as f:
        print('This is stream 1 line 1', file=f)
        async with logmux.new_stream() as g:
            print('This is stream 2 line 1', file=g)
            print('This is stream 2 line 2', file=g)
        print('This is stream 1 line 2', file=f)
    assert verify_output(
        [
            ['This is stream 1 line 1', 'This is stream 1 line 2'],
            ['This is stream 2 line 1', 'This is stream 2 line 2'],
        ],
    )


async def test_one_stream_decorated(verify_output):
    async with LogMux() as logmux:
        decorator = LogMux.simple_decorator('[pre]{}[post]')  # long-winded
        async with logmux.new_stream(decorator) as f:
            print('This is the first line', file=f)
            print('This is the second line', file=f)
    assert verify_output(
        [
            [
                '[pre]This is the first line[post]',
                '[pre]This is the second line[post]',
            ]
        ]
    )


async def test_two_streams_decorated(verify_output):
    async with LogMux() as logmux:
        async with logmux.new_stream(b'1>>{}<<1') as f:  # shorter version
            print('This is stream 1 line 1', file=f)
            async with logmux.new_stream('2>>{}<<2') as g:
                print('This is stream 2 line 1', file=g)
                print('This is stream 2 line 2', file=g)
            print('This is stream 1 line 2', file=f)
    assert verify_output(
        [
            ['1>>This is stream 1 line 1<<1', '1>>This is stream 1 line 2<<1'],
            ['2>>This is stream 2 line 1<<2', '2>>This is stream 2 line 2<<2'],
        ],
    )


async def test_one_charwise_stream_decorated(verify_output):
    s = 'foo\nbar\nbaz'
    async with LogMux() as logmux:
        async with logmux.new_stream('<{}>') as f:
            for c in s:
                f.write(c)
    assert verify_output([['<foo>', '<bar>', '<baz>']])


async def test_one_charwise_interrupted_stream_decorated(verify_output):
    s = 'foo\nbar\nbaz'
    async with LogMux() as logmux:
        async with logmux.new_stream('<{}>') as f:
            for c in s:
                f.write(c)
                f.flush()
                await asyncio.sleep(0.001)
    assert verify_output([['<foo>', '<bar>', '<baz>']])


async def test_two_charwise_streams_decorated(verify_output):
    s = 'foo\nbar\nbaz'
    t = '123\n456\n789'
    async with LogMux() as logmux:
        async with logmux.new_stream(b'<{}>') as f:
            async with logmux.new_stream('[{}]') as g:
                for c, d in zip(s, t):
                    f.write(c)
                    g.write(d)
    assert verify_output(
        [['<foo>', '<bar>', '<baz>'], ['[123]', '[456]', '[789]']]
    )


async def test_two_charwise_interrupted_streams_decorated(verify_output):
    s = 'foo\nbar\nbaz'
    t = '123\n456\n789'
    async with LogMux() as logmux:
        async with logmux.new_stream(b'<{}>') as f:
            async with logmux.new_stream('[{}]') as g:
                for c, d in zip(s, t):
                    f.write(c)
                    g.write(d)
                    f.flush()
                    g.flush()
                    await asyncio.sleep(0.001)
    assert verify_output(
        [['<foo>', '<bar>', '<baz>'], ['[123]', '[456]', '[789]']]
    )


async def test_one_bytewise_stream_with_garbage(capfdbinary):
    lines = [
        b'first line...',
        b'latin-1: \xc6\xd8\xc5...',
        b'utf-8:   \xe2\x9c\x94\xe2\x88\x80\xe2\x9c\x98...',
        b'f8 - ff: \xf8\xf9\xfa\xfb\xfc\xfd\xfe\xff...',
        b'last line without newline',
    ]
    bytestring = b'\n'.join(lines)
    prefix, suffix = '❰'.encode('utf-8'), '❱\n'.encode('utf-8')
    expect_bytestring = b''.join(prefix + line + suffix for line in lines)
    async with LogMux() as logmux:
        async with logmux.new_stream('❰{}❱') as f:
            f.buffer.write(bytestring)
    actual = capfdbinary.readouterr()
    assert actual.out == expect_bytestring
    assert actual.err == b''


async def test_one_bytewise_stream_in_binary_mode_with_garbage(capfdbinary):
    lines = [
        b'first line...',
        b'latin-1: \xc6\xd8\xc5...',
        b'utf-8:   \xe2\x9c\x94\xe2\x88\x80\xe2\x9c\x98...',
        b'f8 - ff: \xf8\xf9\xfa\xfb\xfc\xfd\xfe\xff...',
        b'last line without newline',
    ]
    bytestring = b'\n'.join(lines)
    prefix = b'>>> '  # test passing bytes w/o placeholder to simple_decorator
    expect_bytestring = b''.join(prefix + line + b'\n' for line in lines)
    async with LogMux() as logmux:
        async with logmux.new_stream(prefix, mode='wb') as f:
            f.write(bytestring)
    actual = capfdbinary.readouterr()
    assert actual.out == expect_bytestring
    assert actual.err == b''
