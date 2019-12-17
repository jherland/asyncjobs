from contextlib import contextmanager
import logging
import os
import signal
import time


logger = logging.getLogger('conftest')


@contextmanager
def abort_in(when=0, assert_on_escape=True):
    """Simulate Ctrl+C (aka. KeyboardInterrupt/SIGINT) after the given delay.

    This will schedule (via SIGALRM) a SIGINT signal in the current process
    after the given number of seconds if we're still within the context.
    Upon context exit, the signal is cancelled and the signal handler
    deregistered.

    If the signal is handled within the context, no further action is taken
    (except deregistering the signal handler on context exit). Otherwise, if
    the signal/KeyboardInterrupt escapes the context, an error is logged and
    (unless assert_on_escape is explicitly disabled) an assertion is raised
    (to fail the current test).
    """

    def handle_SIGALRM(signal_number, stack_frame):
        logger.warning('Raising SIGINT to simulate Ctrl+C…')
        os.kill(os.getpid(), signal.SIGINT)

    prev_handler = signal.signal(signal.SIGALRM, handle_SIGALRM)
    signal.setitimer(signal.ITIMER_REAL, when)
    try:
        yield
    except KeyboardInterrupt:
        logger.error('SIGINT/KeyboardInterrupt escaped the context!')
        if assert_on_escape:
            assert False, 'SIGINT/KeyboardInterrupt escaped the context!'
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, prev_handler)


@contextmanager
def assert_elapsed_time_within(time_limit):
    """Measure time used in context and fail test if not within given limit."""
    before = time.time()
    try:
        yield
    finally:
        after = time.time()
        assert after < before + time_limit
