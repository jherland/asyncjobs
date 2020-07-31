import asyncio
import io
import logging
import os
from pathlib import Path
import sys
from tempfile import TemporaryDirectory

logger = logging.getLogger(__name__)


def default_decorator(line):
    return line


def simple_decorator(pattern=None):
    """Build a simple decorator function from the given pattern.

    The decorator will prefix each line with the given pattern. If the given
    pattern contains a '{}' placeholder, it will be used as a separator
    between the prefix and suffix. In other words, this:

      simple_decorator('foo {} bar')

    is equivalent to this

      lambda bs: b'foo ' + bs + b' bar' + b'\n'

    Since StreamMux works on bytes only, the decorator function returned from
    here will also work on bytes only. However, for your convenience (and as
    shown above), any strings given to this factory will be automatically
    converted to bytes using string.encode('utf-8', errors='surrogateescape').
    """
    # process pattern as string, so we can use .format() below
    if isinstance(pattern, (bytes, bytearray)):
        pattern = pattern.decode('utf-8', errors='surrogateescape')
    elif pattern is None:
        pattern = ''
    assert isinstance(pattern, str)

    try:
        assert '\0' not in pattern
        prefix, suffix = pattern.format('\0').split('\0')
    except (AssertionError, ValueError, IndexError):
        prefix, suffix = pattern, ''

    # convert everything to bytes
    prefix = prefix.encode('utf-8', errors='surrogateescape')
    suffix = suffix.encode('utf-8', errors='surrogateescape')
    return lambda line: prefix + line.rstrip() + suffix.rstrip() + b'\n'


class DecoratedStream:
    """Manage the opening/closing of a StreamMux FIFO."""

    def __init__(self, mux, decorator=None, mode='w'):
        self.mux = mux
        self.decorator = decorator
        self.mode = mode
        self.path = None
        self.fifo = None
        self.used_by = 0

    def open(self):
        if self.path is None:  # FIFO not yet open
            assert self.fifo is None
            self.path = self.mux.watched_fifo(self.decorator)
            self.fifo = self.path.open(self.mode)
        self.used_by += 1
        return self.fifo

    def close(self):
        if self.path is None:  # already closed
            assert self.used_by == 0 and self.fifo is None
            return

        self.used_by -= 1
        if self.used_by <= 0:
            if self.fifo is not None:
                self.fifo.close()
                self.fifo = None
            self.mux.unwatch(self.path)
            self.path = None

    def __enter__(self):
        return self.open()

    def __exit__(self, *_):
        self.close()


class StreamMux:
    """Multiplex several output streams (w/decoration) into a single stream."""

    default_decorator = staticmethod(default_decorator)
    simple_decorator = staticmethod(simple_decorator)

    def __init__(self, out=None, tmp_base=None):
        if out is None:
            self.out = sys.stdout.buffer
        elif not isinstance(out, (io.RawIOBase, io.BufferedIOBase)):
            try:
                self.out = out.buffer
            except AttributeError:
                raise ValueError(f'Cannot find binary stream in {out}')
        else:
            self.out = out
        self.tempdir = TemporaryDirectory(dir=tmp_base, prefix='StreamMux_')
        self.fifonum = 0
        self.watches = {}  # map path -> (f, decorator, buffer)

    def new_stream(self, decorator=None, mode='w'):
        return DecoratedStream(self, decorator, mode)

    def watched_fifo(self, decorator=None):
        """Create a FIFO (aka. named pipe) that is watched by StreamMux.

        Creates a FIFO in self.tempdir, and returns its path.
        The FIFO is watched by this StreamMux, so anything written into it will
        appear on this StreamMux' output (after passing through 'decorator').

        The FIFO (and the rest of self.tempdir) will be automatically removed
        on shutdown.
        """
        self.fifonum += 1
        fifopath = Path(self.tempdir.name, f'fifo{self.fifonum}')
        assert not fifopath.exists()
        os.mkfifo(fifopath)
        self._watch(fifopath, decorator)
        return fifopath

    def _watch(self, path, decorator=None):
        """Add the given 'path' to be watched by StreamMux.

        Lines read from 'path' will be passed through 'decorator' before being
        written to this StreamMux' shared output.
        """
        if decorator is None:  # => identity decorator
            decorator = self.default_decorator
        elif isinstance(decorator, (str, bytes, bytearray)):  # => pattern
            decorator = self.simple_decorator(decorator)
        else:
            assert callable(decorator)

        logger.debug(f'Watching {path}')
        assert path not in self.watches
        f = open(os.open(path, os.O_RDONLY | os.O_NONBLOCK), mode='rb')
        buffer = bytearray()
        self.watches[path] = f, decorator, buffer
        asyncio.get_running_loop().add_reader(
            f, self._do_read, f, decorator, buffer
        )

    def _do_read(self, f, decorator, buffer, *, last=False):
        buffer.extend(f.read())
        lines = buffer.splitlines(keepends=True)
        if lines and not lines[-1].endswith(b'\n') and not last:
            # keep partial line in buffer
            del buffer[: -len(lines[-1])]
            lines.pop()
        else:
            buffer.clear()
        for line in lines:
            self.out.write(decorator(line))

    def unwatch(self, path):
        """Stop watching the given 'path'."""
        logger.debug(f'Unwatching {path}')
        assert path in self.watches
        f, decorator, buffer = self.watches.pop(path)
        asyncio.get_running_loop().remove_reader(f)
        self._do_read(f, decorator, buffer, last=True)
        f.close()
        assert not buffer

    def shutdown(self):
        """Shutdown this StreamMux instance.

        Stop watching all files and cleanup temporary FIFOs.
        """
        logger.debug('Shutting down')
        for path in sorted(self.watches.keys()):
            logger.warning(f'{path} was not unwatched!')
            self.unwatch(path)
        if not self.out.closed:
            self.out.flush()
        self.tempdir.cleanup()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.shutdown()
