import pytest

from asyncjobs.logmux import LogMux

pytestmark = pytest.mark.asyncio


async def test_no_output(verify_output):
    async with LogMux():
        pass
    assert verify_output([[]])


async def test_one_stream_undecorated(verify_output):
    async with LogMux() as logmux:
        async with logmux.new_stream() as f:
            print('This is the first line', file=f)
            print('This is the second line', file=f)
    assert verify_output(
        [['This is the first line', 'This is the second line']]
    )


async def test_two_streams_undecorated(verify_output):
    async with LogMux() as logmux:
        async with logmux.new_stream() as f:
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
        async with logmux.new_stream(lambda s: f'[pre]{s[:-1]}[post]\n') as f:
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
        async with logmux.new_stream(lambda s: f'1>>{s[:-1]}<<1\n') as f:
            print('This is stream 1 line 1', file=f)
            async with logmux.new_stream(lambda s: f'2>>{s[:-1]}<<2\n') as g:
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
        async with logmux.new_stream(lambda s: f'<{s.rstrip()}>\n') as f:
            for c in s:
                f.write(c)
    assert verify_output([['<foo>', '<bar>', '<baz>']])


async def test_two_charwise_streams_decorated(verify_output):
    s = 'foo\nbar\nbaz'
    t = '123\n456\n789'
    async with LogMux() as logmux:
        async with logmux.new_stream(lambda s: f'<{s.rstrip()}>\n') as f:
            async with logmux.new_stream(lambda s: f'[{s.rstrip()}]\n') as g:
                for c, d in zip(s, t):
                    f.write(c)
                    g.write(d)
    assert verify_output(
        [['<foo>', '<bar>', '<baz>'], ['[123]', '[456]', '[789]']]
    )


async def test_one_bytewise_stream_with_garbage(tmp_path):
    # LogMux is text/line-based, but uses surrogateescape to preserve
    # undecodable bytes from input to output. We'd like to use capfdbinary
    # here, but _pytest.capture.EncodedFile does not support surrogateescape
    # handling on output. Use a temp file instead.
    output = tmp_path / 'output'
    lines = [
        b'first line...',
        b'latin-1: \xc6\xd8\xc5...',
        b'utf-8:   \xe2\x9c\x94\xe2\x88\x80\xe2\x9c\x98...',
        b'f8 - ff: \xf8\xf9\xfa\xfb\xfc\xfd\xfe\xff...',
        b'last line without newline',
    ]
    bytestring = b'\n'.join(lines)
    with output.open('w', errors='surrogateescape') as out:
        async with LogMux(out) as logmux:
            async with logmux.new_stream(lambda s: f'❰{s.rstrip()}❱\n') as f:
                f.buffer.write(bytestring)
    actual = output.read_text(errors='surrogateescape')
    expect_lines = [
        '❰first line...❱',
        '❰latin-1: \udcc6\udcd8\udcc5...❱',
        '❰utf-8:   ✔∀✘...❱',
        '❰f8 - ff: \udcf8\udcf9\udcfa\udcfb\udcfc\udcfd\udcfe\udcff...❱',
        '❰last line without newline❱',
    ]
    assert actual == ''.join(line + '\n' for line in expect_lines)
