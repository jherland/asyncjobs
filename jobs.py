import argparse
import asyncio
import concurrent.futures
import itertools
import logging
import random
import time


logger = logging.getLogger('jobs')


class Job:
    """ABC for jobs. Jobs have a name, and are callable."""

    def __init__(self, name):
        self.name = name
        self.logger = logger.getChild(name)
        self.logger.info(f'Created {self}')

    def __str__(self):
        return self.name

    async def __call__(self, scheduler):
        raise NotImplementedError


class JobInWorker(Job):
    """A Job where the actual work is scheduled in a worker."""

    def __init__(self, *, work, **kwargs):
        self.work = work
        super().__init__(**kwargs)

    def __str__(self):
        return f'{super().__str__()}/{self.work}'

    def do_work(self):
        self.logger.info(f'Doing own work: {self.work}')
        time.sleep(self.work / 1000)
        self.logger.info(f'Finished own work: {self.work}')
        return self.work

    async def __call__(self, scheduler):
        self.logger.info(f'Scheduling own work')
        future = scheduler.do_in_worker(self.do_work)
        self.logger.info(f'Awaiting own work')
        result = await future
        self.logger.info(f'Awaited own work: {result}')
        return result


class JobWithDeps(Job):
    """A Job that has to wait for dependencies to finish."""

    def __init__(self, *, deps, **kwargs):
        self.deps = set(deps)
        self.dep_results = {}  # Filled by __call__()
        super().__init__(**kwargs)

    def __str__(self):
        deps = ' '.join(sorted(self.deps))
        return f'{super().__str__()} [{deps}]'

    async def __call__(self, scheduler):
        self.logger.info('Awaiting dependencies...')
        self.dep_results = {
            dep: await scheduler.result(dep) for dep in self.deps
        }
        return await super().__call__(scheduler)


class JobSplitAcrossWorkers(JobWithDeps, JobInWorker):
    def __init__(self, *, subjob, split_threshold, **kwargs):
        self.subjob = subjob
        self.split_threshold = split_threshold
        super().__init__(**kwargs)

    async def __call__(self, scheduler):
        i = 0
        while self.work >= self.split_threshold:
            i += 1
            name = f'{self.name}-{i}'
            self.logger.info(f'Splitting off {name}')
            scheduler.add(self.subjob(name=name, work=self.split_threshold))
            self.deps.add(name)
            self.work -= self.split_threshold
        return await super().__call__(scheduler)


class SubJob(JobInWorker):
    def do_work(self):
        return {self.name: super().do_work()}


class RandomJob(JobSplitAcrossWorkers):
    @classmethod
    def generate(cls, dep_prob, max_work):
        names = []
        i = 0
        while True:
            name = chr(ord('a') + i)
            yield cls(
                name=name,
                deps=set(filter(lambda _: random.random() < dep_prob, names)),
                work=random.randint(0, max_work),
                subjob=SubJob,
                split_threshold=10,
            )
            names.append(name)
            i += 1

    def do_work(self):
        self.logger.info('From deps:')
        for dep, result in self.dep_results.items():
            self.logger.info(f'  {dep}: {result}')
        done = super().do_work()
        result = {self.name: done}
        for dep_result in self.dep_results.values():
            result.update(dep_result)
        self.logger.info(f'Returning {result}')
        return result


class Scheduler:
    def __init__(self, workers=None):
        self.workers = workers
        self.jobs = {}  # name -> Job
        self.results = {}  # name -> (future) result from job()
        self.logger = logger.getChild(self.__class__.__name__)
        self.running = False

    def _caller(self):
        return asyncio.Task.current_task().job_name

    def _start_job(self, job):
        task = asyncio.ensure_future(job(self))
        task.job_name = job.name
        self.results[job.name] = task
        #  self.logger.info(f'Started job {job}')

    def add(self, job):
        """Add a job to be run.

        If we're already running (i.e. inside .run()) schedule the job
        immediately, otherwise the job will be scheduled by .run().
        """
        assert job.name not in self.jobs
        self.jobs[job.name] = job
        #  self.logger.info(f'Added job {job}')
        if self.running:
            self._start_job(job)

    async def result(self, job_name):
        """Wait until the given job is finished, and return its result."""
        assert self.running
        caller = self._caller()
        assert job_name in self.results
        if not self.results[job_name].done():
            self.logger.info(f'  {caller} is waiting on {job_name}...')
        result = await (self.results[job_name])
        self.logger.info(f'  {job_name} done, returning {result} to {caller}')
        return result

    def do_in_worker(self, func, *args):
        """Call 'func(*args)' in a worker and return its future result."""
        assert self.running
        self.logger.info(f'Submit to worker: {func}(*{args})')
        loop = asyncio.get_running_loop()
        if self.workers is None:  # single-threaded, run synchronously
            future = loop.create_future()
            try:
                future.set_result(func(*args))
            except Exception as e:
                future.set_exception(e)
        else:
            future = loop.run_in_executor(self.workers, func, *args)
        self.logger.info(f'Returned {future} from worker')
        return future

    async def run(self, max_runtime=10, stop_on_first_error=False):
        """Run until all jobs are finished."""
        self.logger.info('Running')
        to_start = reversed(list(self.jobs.values()))
        self.running = True
        for job in to_start:
            self._start_job(job)
        if stop_on_first_error:
            return_when = concurrent.futures.FIRST_EXCEPTION
        else:
            return_when = concurrent.futures.ALL_COMPLETED
        await asyncio.wait(
            self.results.values(),
            timeout=max_runtime,
            return_when=return_when,
        )
        if self.workers is not None:
            self.workers.shutdown()
        return self.results


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
        help='How many workers to use (0: purely single-threaded)',
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        style='{',
        format='{relativeCreated:8.0f} {process:5}/{threadName:10} {name:>14}: {message}',
    )

    random.seed(0)  # deterministic
    job_generator = RandomJob.generate(args.dep_prob, args.max_work)
    jobs = list(itertools.islice(job_generator, args.num_jobs))

    if args.workers == 0:
        workers = None
    elif args.workers > 0:
        workers = concurrent.futures.ThreadPoolExecutor(
            args.workers, 'Builder'
        )
    elif args.workers < 0:
        workers = concurrent.futures.ProcessPoolExecutor(-args.workers)
    builder = Scheduler(workers)
    for job in jobs:
        builder.add(job)
    results = asyncio.run(builder.run())
    longest_work = max(sum(f.result().values()) for f in results.values())
    print(f'Finished with max(sum(work)) == {longest_work}!')


if __name__ == '__main__':
    main()
