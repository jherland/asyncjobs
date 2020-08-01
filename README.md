# asyncjobs

[![Tests](https://github.com/jherland/asyncjobs/workflows/tests/badge.svg)](
https://github.com/jherland/asyncjobs/actions)
[![Build Status](https://travis-ci.org/jherland/asyncjobs.svg?branch=master)](
https://travis-ci.org/jherland/asyncjobs)
[![PyPI version](https://badge.fury.io/py/asyncjobs.svg)](
https://badge.fury.io/py/asyncjobs)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/asyncjobs)](
https://pypi.org/project/asyncjobs/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](
https://opensource.org/licenses/MIT)

Asynchronous job scheduler.
Using asyncio to run jobs in worker threads/processes.

## Description

A job scheduler for running asynchronous (and synchronous) jobs with
dependencies using asyncio. Jobs are _coroutines_ (`async def` functions) with
a _name_, and (optionally) a set of _dependencies_ (i.e. names of other jobs
that must complete successfully before this job can start). The job coroutine
may await the results from other jobs, schedule work to be done in a thread or
subprocess, or various other things provided by the particular `Context` object
passed to the coroutine. The job coroutines are run by a `Scheduler`, which
control the execution of the jobs, as well as the number of concurrent threads
and processes doing work. The Scheduler emits events which allow e.g. progress
and statistics to be easily collected and monitored. A separate module is
provided to turn Scheduler events into an interactive scheduling plot:

![Example schedule plot](
https://github.com/jherland/asyncjobs/raw/master/examples/random_jobs_plot.png)

A job coroutine completes in one of three ways:

 - Jobs complete _successfully_ by returning, and the returned value (if any)
   is known as the _job result_.
 - Jobs are considered to have _failed_ if any exception propagates from its
   coroutine. Any job that depend on (i.e. await the result of) another job
   will be automatically cancelled by the scheduler if that other job fails.
 - Jobs may be _cancelled_, which is implented by the scheduler raising an
   `asyncio.CancelledError` inside the coroutine, and having it propagate out
   of the coroutine.

The Scheduler handles its own cancellation (e.g. _Ctrl-C_) by cancelling all
ongoing and remaining tasks as quickly and cleanly as possible.

## Usage examples

### Run three simple jobs in sequence

```python
import asyncio
from asyncjobs import Scheduler
import time


def sleep():  # Run in a worker thread by job #2 below
    print(f'{time.ctime()}: Sleep for a second')
    time.sleep(1)
    print(f'{time.ctime()}: Finished sleep')


s = Scheduler()

# Job #1 prints uptime
s.add_subprocess_job('#1', ['uptime'])

# Job #2 waits for #1 and then sleeps in a thread
s.add_thread_job('#2', sleep, deps={'#1'})

# Job #3 waits for #2 and then prints uptime (again)
s.add_subprocess_job('#3', ['uptime'], deps={'#2'})

asyncio.run(s.run())
```

([code also available here](
https://github.com/jherland/asyncjobs/blob/master/examples/simple_usage.py))
should produce output like this:

```
 16:35:58  up 9 days  3:29,  1 user,  load average: 0.62, 0.55, 0.55
Tue Feb 25 16:35:58 2020: Sleep for a second
Tue Feb 25 16:35:59 2020: Finished sleep
 16:35:59  up 9 days  3:29,  1 user,  load average: 0.62, 0.55, 0.55
```

### Fetching web content in parallel

[This example](
https://github.com/jherland/asyncjobs/blob/master/examples/random_wikipedia.py)
fetches a random Wikipedia article, and then follows links to other articles
until 10 articles have been fetched. Sample output:

```
    fetching https://en.wikipedia.org/wiki/Special:Random...
  * [Nauru national netball team] links to 3 articles
      fetching https://en.wikipedia.org/wiki/Nauru...
      fetching https://en.wikipedia.org/wiki/Netball...
      fetching https://en.wikipedia.org/wiki/Netball_at_the_1985_South_Pacific_Mini_Games...
    * [Netball at the 1985 South Pacific Mini Games] links to 4 articles
    * [Netball] links to 114 articles
        fetching https://en.wikipedia.org/wiki/1985_South_Pacific_Mini_Games...
        fetching https://en.wikipedia.org/wiki/Rarotonga...
        fetching https://en.wikipedia.org/wiki/Cook_Islands...
    * [Nauru] links to 257 articles
        fetching https://en.wikipedia.org/wiki/Ball_sport...
      * [Ball game] links to 8 articles
        fetching https://en.wikipedia.org/wiki/Commonwealth_of_Nations...
      * [Rarotonga] links to 43 articles
        fetching https://en.wikipedia.org/wiki/Netball_Superleague...
      * [Cook Islands] links to 124 articles
      * [Netball Superleague] links to 25 articles
      * [Commonwealth of Nations] links to 434 articles
      * [1985 South Pacific Mini Games] links to 5 articles
```

### Wasting time efficiently across multiple threads

[The final example](
https://github.com/jherland/asyncjobs/blob/master/examples/random_jobs.py)
(which was used to produce the schedule plot above) simulates a simple build
system: It creates a number of jobs (default: 10), each job sleeps for some
random time (default: <=100ms), and has some probability of depending on each
preceding job (default: 0.5). After awaiting its dependencies, each job may
also split portions of its work into one or more sub-jobs, and await their
completion, before finishing its remaining work. Everything is scheduled
across a fixed number of worker threads (default: 4).

## Installation

Run the following to install:

```bash
$ pip install asyncjobs
```

## Development

To work on asyncjobs, clone [this repo](https://github.com/jherland/asyncjobs/),
and run the following (in a virtualenv) to get everything you need to develop
and run tests:

```bash
$ pip install -e .[dev]
```

Additionally, if you want to generate scheduling plots (as seen above), you
need a couple more dependencies ([`plotly`](https://plotly.com/python/) and
[`numpy`](https://numpy.org/)):

```bash
$ pip install -e .[dev,plot]
```

Alternatively, if you are using [Nix](https://nixos.org/nix/), use the included
`shell.nix` to get a development environment with everything automatically
installed:

```bash
$ nix-shell
```

Use [`nox`](https://nox.thea.codes/) to run all tests, formatters and linters:

```bash
$ nox
```

This will run the [`pytest`](https://pytest.org)-based test suite under all
supported Python versions, format the code with
[`black`](https://black.readthedocs.io/) and run the
[`flake8`](https://flake8.pycqa.org/) linter.

## Contributing

Main development happens at <https://github.com/jherland/asyncjobs/>.
Post issues and PRs there.
