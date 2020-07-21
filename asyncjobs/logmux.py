import asyncio
import contextlib
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

    Since LogMux works on bytes only, the decorator function returned from
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


class LogMux:
    """Async task to multiplex many write streams into a single stream."""

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
        self._q = None
        self.tempdir = TemporaryDirectory(dir=tmp_base, prefix='LogMux_')
        self.fifonum = 0
        self._task = None

    @property
    def q(self):
        if self._q is None:
            self._q = asyncio.Queue()
        return self._q

    async def _watch(self, path, decorator=None):
        """Add the given 'path' to be watched by LogMux.

        Lines read from 'path' will be passed through 'decorator' before being
        written to this logmux's shared output.
        """
        if decorator is None:  # => identity decorator
            decorator = self.default_decorator
        elif isinstance(decorator, (str, bytes, bytearray)):  # => pattern
            decorator = self.simple_decorator(decorator)
        else:
            assert callable(decorator)
        await self.q.put(('watch', path, decorator))
        await self.q.join()

    async def watched_fifo(self, decorator=None):
        """Create a FIFO (aka. named pipe) that is watched by LogMux.

        Creates a FIFO in self.tempdir, and returns its path.
        The FIFO is watched by LogMux, so anything written into it will appear
        on LogMux's output (after being passed through 'decorator').

        The FIFO (and the rest of self.tempdir) will be automatically removed
        on shutdown.
        """
        self.fifonum += 1
        fifopath = Path(self.tempdir.name, f'fifo{self.fifonum}')
        assert not fifopath.exists()
        os.mkfifo(fifopath)
        await self._watch(fifopath, decorator)
        return fifopath

    async def unwatch(self, path):
        """Stop watching the given 'path'."""
        await self.q.put(('unwatch', path))
        await self.q.join()

    @contextlib.asynccontextmanager
    async def new_stream(self, decorator=None, *, mode='w'):
        """Context manager wrapping .watched_fifo() and .unwatch()."""
        path = await self.watched_fifo(decorator)
        try:
            with open(path, mode) as f:
                yield f
        finally:
            await self.unwatch(path)

    async def shutdown(self):
        """Shutdown LogMux. Stop watching all files and cleanup temporaries."""
        await self.q.put(('shutdown',))  # Signal shutdown
        await self.q.join()
        self.tempdir.cleanup()

    async def service(self):
        """Coroutine reading from watched stream and writing to the shared out.

        Communicates with the above methods via self.q. Runs until .shutdown()
        is called.
        """

        class Muxer:
            def __init__(self, q, out):
                self.q = q
                self.out = out
                self.paths = {}  # map path -> (f, decorator, buffer)
                self.running = False
                self.loop = asyncio.get_running_loop()

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
                    try:
                        self.out.write(decorator(line))
                    except Exception as e:
                        logger.error(f'Ignored exception: {e!r}')

            def watch(self, path, decorator):
                logger.debug(f'Watching {path}')
                assert path not in self.paths
                f = open(os.open(path, os.O_RDONLY | os.O_NONBLOCK), mode='rb')
                buffer = bytearray()
                self.paths[path] = f, decorator, buffer
                self.loop.add_reader(f, self._do_read, f, decorator, buffer)

            def unwatch(self, path):
                logger.debug(f'Unwatching {path}')
                assert path in self.paths
                f, decorator, buffer = self.paths.pop(path)
                self.loop.remove_reader(f)
                self._do_read(f, decorator, buffer, last=True)
                f.close()
                assert not buffer

            def shutdown(self):
                logger.debug('Shutting down')
                for path in sorted(self.paths.keys()):
                    logger.warning(f'{path} was not unwatched!')
                    self.unwatch(path)
                self.out.flush()
                self.running = False

            async def run(self):
                self.running = True
                while self.running:
                    cmd, *args = await self.q.get()
                    assert cmd in {'watch', 'unwatch', 'shutdown'}
                    getattr(self, cmd)(*args)
                    self.q.task_done()

        await Muxer(self.q, self.out).run()

    async def __aenter__(self):
        assert self._task is None
        self._task = asyncio.create_task(self.service())
        return self

    async def __aexit__(self, *_):
        assert self._task is not None
        assert not self._task.done()
        await self.shutdown()
        await self._task
        self._task = None
