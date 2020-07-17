#!/usr/bin/env python3
"""Helper program to simulate various subprocess behaviors."""

import os
import logging
from pathlib import Path
import signal
import sys
import time

logger = logging.getLogger('subprocess_helper')


def main(args):
    out = sys.stdout
    for arg in args:
        if arg == 'err:':
            out = sys.stderr
        elif arg == 'out:':
            out = sys.stdout
        elif arg == 'in:':
            print(sys.stdin.readline().rstrip(), file=out)
        elif arg == 'cwd:':
            print(Path.cwd(), file=out)
        elif arg.startswith('env:'):
            print(os.environ[arg[4:]], file=out)
        elif arg.startswith('sleep:'):
            time.sleep(float(arg[6:]))
        elif arg.startswith('touch:'):
            Path(arg[6:]).touch()
        elif arg.startswith('log:'):
            logger.error(arg[4:])
        elif arg.startswith('ignore:'):
            signal.signal(getattr(signal, arg[7:]), signal.SIG_IGN)
        elif arg.startswith('exit:'):
            return int(arg[5:])
        else:
            print(arg, file=out)

    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
