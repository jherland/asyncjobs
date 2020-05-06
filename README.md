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
dependencies using asyncio. Jobs are identified by their _name_ and implement
an async `__call__` method. Jobs may await other jobs or schedule work to be
done in a thread or subprocess. Jobs are run by a Scheduler, which control the
execution of the jobs, as well as the number of concurrent threads and
processes doing work. The Scheduler emits events which allow e.g. progress and
statistics to be easily collected and monitored. A separate module is provided
to turn Scheduler events into an interactive scheduling plot:

![Example schedule plot](
https://github.com/jherland/asyncjobs/raw/master/examples/random_jobs_plot.png)

Jobs complete successfully by returning (with or without a return value). Any
exception propagated from a job's `__call__` method is regarded as a failure.
Any job that depend on (i.e. await the result of) another job will be
automatically cancelled by the scheduler if that other job fails.
The Scheduler handles cancellation (e.g. _Ctrl-C_) by cancelling all ongoing
and remaining tasks as quickly and cleanly as possible.

## Usage examples

### Run three simple jobs in sequence

```python
import asyncio
from asyncjobs import Job, Scheduler

# Helper function
def sleep():
    import time
    print(f'{time.ctime()}: Sleep for a second')
    time.sleep(1)
    print(f'{time.ctime()}: Finished sleep')

# Job #1 prints uptime
job1 = Job('#1')
job1.subprocess_argv = ['uptime']

# Job #2 waits for #1 and then sleeps in a thread
job2 = Job('#2', deps={'#1'})
job2.thread_func = sleep

# Job #3 waits for #2 and then prints uptime (again)
job3 = Job('#3', deps={'#2'})
job3.subprocess_argv = ['uptime']

# Run all jobs in the scheduler
s = Scheduler()
for job in [job1, job2, job3]:
    s.add(job)
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
  * [Indonesia–Mongolia relations] links to 7 articles
      fetching https://en.wikipedia.org/wiki/Indonesia...
      fetching https://en.wikipedia.org/wiki/Mongolia...
      fetching https://en.wikipedia.org/wiki/Jakarta...
      fetching https://en.wikipedia.org/wiki/Mongolian_National_University,_Ulan_Bator...
    * [Mongolia] links to 529 articles
      fetching https://en.wikipedia.org/wiki/Sukarno...
    * [Indonesia] links to 697 articles
      fetching https://en.wikipedia.org/wiki/Megawati_Soekarnoputri...
    * [Jakarta] links to 757 articles
      fetching https://en.wikipedia.org/wiki/Susilo_Bambang_Yudhoyono...
    * [Mongolian National University] links to 2 articles
        fetching https://en.wikipedia.org/wiki/Mongolian_language...
    * [Sukarno] links to 523 articles
        fetching https://en.wikipedia.org/wiki/Mongolian_script...
    * [Susilo Bambang Yudhoyono] links to 159 articles
    * [Megawati Sukarnoputri] links to 88 articles
      * [Mongolian language] links to 259 articles
      * [Mongolian script] links to 142 articles

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

This will run the test suite under all supported Python versions, format the
code with [`black`](https://black.readthedocs.io/) and run the
[`flake8`](https://flake8.pycqa.org/) linter.

## Contributing

Main development happens at <https://github.com/jherland/asyncjobs/>.
Post issues and PRs there.
