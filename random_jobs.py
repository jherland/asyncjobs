#!/usr/bin/env python3
import argparse
import asyncio
import concurrent.futures
import itertools
import logging
import random
import sys
import time

from ansicolors import AnsiColors, AnsiColorFormatter
from scheduler import JobInWorker, JobWithDeps, Scheduler

logger = logging.getLogger('random_jobs')


class TimeWaster(JobInWorker):
    def __init__(self, work, **kwargs):
        self.work = work
        super().__init__(**kwargs)

    def __str__(self):
        return f'{super().__str__()}/{self.work}'

    def do_work(self):
        self.logger.info(f'Doing own work: {self.work}')
        time.sleep(self.work / 1000)
        self.logger.info(f'Finished own work: {self.work}')
        return {self.name: self.work}


class ParallelTimeWaster(JobWithDeps, TimeWaster):
    def __init__(self, *, work_threshold, **kwargs):
        self.work_threshold = work_threshold
        super().__init__(**kwargs)

    async def __call__(self, scheduler):
        i = 0
        while self.work > self.work_threshold:
            i += 1
            name = f'{self.name}-{i}'
            self.logger.info(f'Splitting off {name}')
            scheduler.add(TimeWaster(name=name, work=self.work_threshold))
            self.deps.add(name)
            self.work -= self.work_threshold
        return await super().__call__(scheduler)

    def do_work(self):
        result = {}
        self.logger.info('From deps:')
        for dep, dep_result in self.dep_results.items():
            self.logger.info(f'  {dep}: {dep_result}')
            result.update(dep_result)
        result.update(super().do_work())
        self.logger.info(f'Returning {result}')
        return result


class RandomJob(ParallelTimeWaster):
    @classmethod
    def generate(cls, dep_prob, max_work=100, work_threshold=None):
        if work_threshold is None:
            work_threshold = int(max_work / 2)
            assert work_threshold > 0
        names = []
        i = 0
        while True:
            name = 'random:' + chr(ord('a') + i)
            yield cls(
                name=name,
                deps=set(filter(lambda _: random.random() < dep_prob, names)),
                work=random.randint(0, max_work),
                work_threshold=work_threshold,
            )
            names.append(name)
            i += 1


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('num_jobs', type=int, help='Number of jobs to run')
    parser.add_argument(
        'dep_prob',
        type=float,
        help='Probability of depending on each preceding job',
    )
    parser.add_argument(
        'max_work', type=int, help='Max duration of each job (msecs)'
    )
    parser.add_argument(
        'workers',
        type=int,
        help='How many workers (<0: #procs, 0: synchronous, >0: #threads)',
    )
    parser.add_argument(
        '-v', '--verbose', action='count', default=0, help='Increase log level'
    )
    parser.add_argument(
        '-q', '--quiet', action='count', default=0, help='Decrease log level'
    )
    args = parser.parse_args()

    formatter = AnsiColorFormatter if AnsiColors.enabled else logging.Formatter
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        formatter(
            fmt=(
                '{relativeCreated:8.0f} {process:5}/{threadName:10} '
                '{name:>10}: {message}'
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

    if args.workers == 0:
        print('Running synchronously…')
        workers = None
    elif args.workers > 0:
        print(f'Running with {args.workers} worker threads…')
        workers = concurrent.futures.ThreadPoolExecutor(
            args.workers, 'Builder'
        )
    elif args.workers < 0:
        worker_procs = -args.workers
        print(f'Running with {worker_procs} worker processes…')
        workers = concurrent.futures.ProcessPoolExecutor(worker_procs)
    builder = Scheduler(workers)
    for job in jobs:
        builder.add(job)
    results = asyncio.run(builder.run(), debug=False)
    longest_work = max(sum(f.result().values()) for f in results.values())
    print(f'Finished with max(sum(work)) == {longest_work}')


if __name__ == '__main__':
    main()
