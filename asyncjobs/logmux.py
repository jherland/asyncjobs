import asyncio
import contextlib
import logging
import os
from pathlib import Path
import sys
from tempfile import TemporaryDirectory

logger = logging.getLogger(__name__)


class LogMux:
    """Async task to multiplex many write streams into a single stream."""

    def __init__(self, out=None, tmp_base=None):
        self.out = sys.stdout if out is None else out
        self.q = asyncio.Queue()
        self.tempdir = TemporaryDirectory(dir=tmp_base, prefix='LogMux_')
        self.fifonum = 0
        self._task = None

    async def _watch(self, path, decorator=None):
        """Add the given 'path' to be watched by LogMux.

        Lines read from 'path' will be passed through 'decorator' before being
        written to this logmux's shared output.
        """
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
    async def new_stream(self, decorator=None):
        """Context manager wrapping .watched_fifo() and .unwatch()."""
        path = await self.watched_fifo(decorator)
        try:
            with open(path, 'w') as f:
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
                self.paths = {}  # map path -> (f, decorator)
                self.running = False
                self.loop = asyncio.get_running_loop()

            def _do_read(self, f, decorator):
                while True:
                    line = f.readline()
                    if not line:
                        break
                    self.out.write(decorator(line))

            def watch(self, path, decorator=None):
                logger.debug(f'Watching {path}')
                if decorator is None:

                    def identity(s):
                        return s

                    decorator = identity
                assert path not in self.paths
                f = open(
                    os.open(str(path), os.O_RDONLY | os.O_NONBLOCK),
                    mode='r',
                    errors='surrogateescape',
                )
                self.loop.add_reader(f, self._do_read, f, decorator)
                self.paths[path] = f, decorator

            def unwatch(self, path):
                logger.debug(f'Unwatching {path}')
                assert path in self.paths
                f, decorator = self.paths.pop(path)
                self.loop.remove_reader(f)
                self._do_read(f, decorator)

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
