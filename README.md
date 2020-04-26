# asyncjobs

Asynchronous job scheduler.

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

## Usage

Run three simple jobs in sequence
([code also available here](examples/simple_usage.py)):

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

should produce output like this:

```
 16:35:58  up 9 days  3:29,  1 user,  load average: 0.62, 0.55, 0.55
Tue Feb 25 16:35:58 2020: Sleep for a second
Tue Feb 25 16:35:59 2020: Finished sleep
 16:35:59  up 9 days  3:29,  1 user,  load average: 0.62, 0.55, 0.55
```

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

Alternatively, if you are using Nix, simply use the bundled `shell.nix` to get
a development environment:

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
