import asyncio
import pytest

from asyncjobs.stream_mux import StreamMux

pytestmark = pytest.mark.asyncio


async def test_no_output(verify_output):
    with StreamMux():
        pass
    assert verify_output([])


async def test_one_stream_undecorated(verify_output):
    with StreamMux() as mux, mux.new_stream() as f:
        print('This is the first line', file=f)
        print('This is the second line', file=f)
    assert verify_output(
        [['This is the first line', 'This is the second line']]
    )


async def test_two_streams_undecorated(verify_output):
    with StreamMux() as mux, mux.new_stream() as f:
        print('This is stream 1 line 1', file=f)
        with mux.new_stream() as g:
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
    with StreamMux() as mux:
        decorator = StreamMux.simple_decorator('[pre]{}[post]')  # long-winded
        with mux.new_stream(decorator) as f:
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
    with StreamMux() as mux:
        with mux.new_stream(b'1>>{}<<1') as f:  # shorter version
            print('This is stream 1 line 1', file=f)
            with mux.new_stream('2>>{}<<2') as g:
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
    with StreamMux() as mux, mux.new_stream('<{}>') as f:
        for c in s:
            f.write(c)
    assert verify_output([['<foo>', '<bar>', '<baz>']])


async def test_one_charwise_interrupted_stream_decorated(verify_output):
    s = 'foo\nbar\nbaz'
    with StreamMux() as mux, mux.new_stream('<{}>') as f:
        for c in s:
            f.write(c)
            f.flush()
            await asyncio.sleep(0.001)
    assert verify_output([['<foo>', '<bar>', '<baz>']])


async def test_two_charwise_streams_decorated(verify_output):
    s = 'foo\nbar\nbaz'
    t = '123\n456\n789'
    with StreamMux() as mux:
        with mux.new_stream(b'<{}>') as f, mux.new_stream('[{}]') as g:
            for c, d in zip(s, t):
                f.write(c)
                g.write(d)
    assert verify_output(
        [['<foo>', '<bar>', '<baz>'], ['[123]', '[456]', '[789]']]
    )


async def test_two_charwise_interrupted_streams_decorated(verify_output):
    s = 'foo\nbar\nbaz'
    t = '123\n456\n789'
    with StreamMux() as mux:
        with mux.new_stream(b'<{}>') as f, mux.new_stream('[{}]') as g:
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
    with StreamMux() as mux, mux.new_stream('❰{}❱') as f:
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
    with StreamMux() as mux, mux.new_stream(prefix, mode='wb') as f:
        f.write(bytestring)
    actual = capfdbinary.readouterr()
    assert actual.out == expect_bytestring
    assert actual.err == b''


async def test_write_to_file(tmp_path):
    path = tmp_path / 'file'
    with path.open('w') as f, StreamMux(f) as mux, mux.new_stream() as g:
        g.write('first line\n')
        await asyncio.sleep(0.001)
        g.write('second line\n')

    assert path.read_text() == 'first line\nsecond line\n'


async def test_follow_file_reads_from_beginning(tmp_path, verify_output):
    path = tmp_path / 'file'
    with path.open('w') as f, StreamMux() as mux:
        print('first line', file=f, flush=True)
        with mux.follow_file(path, '<{}>'):
            print('second line', file=f, flush=True)
        print('third line', file=f, flush=True)  # not watched
    assert verify_output([['<first line>', '<second line>']])


async def test_follow_file_first_read_is_immediate(tmp_path, verify_output):
    path = tmp_path / 'file'
    with path.open('w') as f, StreamMux() as mux:
        print('first line', file=f, flush=True)
        with mux.follow_file(path, '<{}>'):
            await asyncio.sleep(0.001)
            assert verify_output([['<first line>']])
    assert verify_output([])


async def test_follow_file_second_read_after_period(tmp_path, verify_output):
    path = tmp_path / 'file'
    with path.open('w') as f, StreamMux() as mux:
        print('first line', file=f, flush=True)
        with mux.follow_file(path, '<{}>', period=0.01):
            await asyncio.sleep(0.001)
            assert verify_output([['<first line>']])
            print('second line', file=f, flush=True)
            await asyncio.sleep(0.001)
            assert verify_output([])
            await asyncio.sleep(0.01)
            assert verify_output([['<second line>']])
    assert verify_output([])


async def test_follow_file_read_after_writer_close(tmp_path, verify_output):
    path = tmp_path / 'file'
    f = path.open('w')
    print('first line', file=f, flush=True)
    with StreamMux() as mux, mux.follow_file(path, '<{}>', period=0.01):
        await asyncio.sleep(0.001)
        assert verify_output([['<first line>']])
        print('second line', file=f, flush=True)
        f.close()
        await asyncio.sleep(0.01)
        assert verify_output([['<second line>']])
    assert verify_output([])


async def test_follow_file_ignores_short_rewrites(tmp_path, verify_output):
    path = tmp_path / 'file'
    f = path.open('w')
    print('this is the first line', file=f, flush=True)
    with StreamMux() as mux, mux.follow_file(path, '<{}>', period=0.01):
        await asyncio.sleep(0.001)
        assert verify_output([['<this is the first line>']])
        print('second line', file=f, flush=True)
        f.close()
        f = path.open('w')
        print('short rewrite', file=f, flush=True)  # not seen by reader
        f.close()
    assert verify_output([])  # misses both 'second line' and 'short rewrite'


async def test_follow_file_reads_appends(tmp_path, verify_output):
    path = tmp_path / 'file'
    f = path.open('w')
    print('first line', file=f, flush=True)
    with StreamMux() as mux, mux.follow_file(path, '<{}>', period=0.01):
        await asyncio.sleep(0.001)
        assert verify_output([['<first line>']])
        print('second line', file=f, flush=True)
        f.close()
        f = path.open('a')
        print('third line', file=f, flush=True)
        f.close()
    assert verify_output([['<second line>', '<third line>']])


async def test_internal_errors_are_propagated(tmp_path):
    path = tmp_path / 'file'
    f = path.open('w')
    with pytest.raises(ValueError):
        with StreamMux(f) as mux, mux.new_stream() as g:
            g.write('first line\n')
            g.flush()
            await asyncio.sleep(0.001)
            f.flush()
            f.close()
            g.write('second line\n')  # raises ValueError: write to closed file
        # context exit raises ValueError: flush of closed file

    assert path.read_text() == 'first line\n'
