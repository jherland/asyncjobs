#!/usr/bin/env python3
"""Create random jobs that sleep in threads, and plot their scheduling."""

import argparse
import asyncio
import itertools
import logging
import random
import sys
import time

from asyncjobs import Scheduler

logger = logging.getLogger('random_jobs')


class TimeWaster:
    def __init__(self, *, name, work, deps=None):
        self.name = name
        self.work = work
        self.deps = deps

    def __str__(self):
        deps = ', '.join(sorted(self.deps)) if self.deps is not None else 'N/A'
        return f'{self.name} ({deps})'

    async def __call__(self, ctx):
        logger.info(f'{str(self):>32}')
        return await ctx.call_in_thread(self.do_work, ctx)

    def do_work(self, ctx):
        logger.info(f'{str(self):>32} Doing own work: {self.work}')
        time.sleep(self.work / 1000)
        logger.info(f'{str(self):>32} Finished own work: {self.work}')
        return {self.name: self.work}


class RandomJob(TimeWaster):
    @classmethod
    def generate(cls, dep_prob, max_work=100, work_threshold=None):
        if work_threshold is None:
            work_threshold = max_work // 2
            assert work_threshold > 0
        names = []
        i = 0
        while True:
            letter = chr(ord('a') + i)
            work = random.randint(0, max_work)
            name = f'{letter}/{work}'
            yield cls(
                name=name,
                deps=set(filter(lambda _: random.random() < dep_prob, names)),
                work=work,
                work_threshold=work_threshold,
            )
            names.append(name)
            i += 1

    def __init__(self, *, work_threshold, **kwargs):
        super().__init__(**kwargs)
        self.work_threshold = work_threshold

    async def __call__(self, ctx):
        i = 0
        while self.work > self.work_threshold:
            i += 1
            work = random.randint(
                self.work_threshold * 2 // 3, self.work_threshold
            )
            name = f'{self.name}_{i}/{work}'
            logger.info(f'{str(self):>32} Splitting off {name}')
            job = TimeWaster(name=name, work=work)
            ctx.add_job(job.name, job)
            self.deps.add(name)
            self.work -= work
        return await super().__call__(ctx)

    def do_work(self, ctx):
        result = {}
        logger.info(f'{str(self):>32} From deps:')
        for dep, dep_result in ctx.deps.items():
            logger.info(f'{" "*32}   {dep}: {dep_result}')
            result.update(dep_result)
        result.update(super().do_work(ctx))
        logger.info(f'{str(self):>32} Returning {result}')
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        'num_jobs',
        type=int,
        nargs='?',
        default=10,
        help='Number of jobs to run',
    )
    parser.add_argument(
        'dep_prob',
        type=float,
        nargs='?',
        default=0.5,
        help='Probability of depending on each preceding job',
    )
    parser.add_argument(
        'max_work',
        type=int,
        nargs='?',
        default=100,
        help='Max duration of each job (msecs)',
    )
    parser.add_argument(
        'workers',
        type=int,
        nargs='?',
        default=4,
        help='Max number of parallel workers',
    )
    parser.add_argument(
        '-p', '--plot', action='store_true', help='Plot job schedule'
    )
    parser.add_argument(
        '-v', '--verbose', action='count', default=0, help='Increase log level'
    )
    parser.add_argument(
        '-q', '--quiet', action='count', default=0, help='Decrease log level'
    )
    args = parser.parse_args()

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter(
            fmt=(
                '{relativeCreated:8.0f} {process:5}/{threadName:16} '
                '{name:>16}: {message}'
            ),
            style='{',
        )
    )
    loglevel = logging.WARNING + 10 * (args.quiet - args.verbose)
    logging.basicConfig(handlers=[handler], level=loglevel)

    print(f'Generating {args.num_jobs} jobs of work <= {args.max_work}…')
    random.seed(0)  # deterministic
    job_generator = RandomJob.generate(args.dep_prob, args.max_work)
    jobs = list(itertools.islice(job_generator, args.num_jobs))

    events = []
    s = Scheduler(workers=args.workers, event_handler=events.append)
    for job in jobs:
        s.add_job(job.name, job, job.deps)
    results = asyncio.run(s.run(), debug=False)
    longest_work = max(sum(f.result().values()) for f in results.values())
    print(f'Finished with max(sum(work)) == {longest_work}')

    if args.plot:
        from asyncjobs.plot_schedule import plot_schedule

        plot_schedule(title=' '.join(sys.argv), events=events).show()


if __name__ == '__main__':
    main()
