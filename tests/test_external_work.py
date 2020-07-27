import asyncio
import os
from pathlib import Path
import pytest
import re
from subprocess import CalledProcessError, PIPE, STDOUT
import time

from asyncjobs import external_work

from conftest import (
    assert_elapsed_time,
    Cancelled,
    mock_argv,
    TExternalWorkJob,
    verified_events,
    verify_number_of_tasks,
    verify_tasks,
)

pytestmark = pytest.mark.asyncio

TJob = TExternalWorkJob


@pytest.fixture
def Scheduler(scheduler_with_workers):
    return scheduler_with_workers(external_work.Scheduler)


@pytest.fixture
def run(Scheduler):
    async def _run(todo):
        scheduler = Scheduler()
        with verified_events(scheduler, todo):
            for job in todo:
                scheduler.add_job(job.name, job, getattr(job, 'deps', None))
            try:
                return await scheduler.run()
            finally:
                verify_number_of_tasks(1)  # no other tasks than test itself

    return _run


async def copy_lines(src, dst):
    while True:
        line = await src.readline()
        if not line:
            dst.close()
            break
        dst.write(line)
        await dst.drain()


# simple thread scenarios with call_in_thread() helper


async def test_one_ok_job_in_thread(run):
    todo = [TJob('foo', thread=lambda ctx: 'foo worked')]
    done = await run(todo)
    assert verify_tasks(done, {'foo': 'foo worked'})


async def test_one_failed_between_two_ok_jobs_in_threads_cancels_last(run):
    def raiseUGH(ctx):
        raise ValueError('UGH')

    todo = [
        TJob('foo', before={'bar'}, thread=lambda ctx: 'foo worked'),
        TJob('bar', {'foo'}, before={'baz'}, thread=raiseUGH),
        TJob('baz', {'bar'}, thread=lambda ctx: 'baz worked'),
    ]
    done = await run(todo)
    assert verify_tasks(
        done, {'foo': 'foo worked', 'bar': ValueError('UGH'), 'baz': Cancelled}
    )


# simple subprocess scenarios with run_in_subprocess() helper


async def test_one_ok_job_in_subproc(run, tmp_path):
    path = tmp_path / 'foo'
    todo = [TJob('foo', argv=mock_argv(f'touch:{path}'))]
    done = await run(todo)
    assert verify_tasks(done, {'foo': 0})
    assert path.is_file()


async def test_one_failed_between_two_in_subprocs_cancels_last(run, tmp_path):
    foo_path = tmp_path / 'foo'
    baz_path = tmp_path / 'baz'
    todo = [
        TJob('foo', before={'bar'}, argv=mock_argv(f'touch:{foo_path}')),
        TJob('bar', {'foo'}, before={'baz'}, argv=mock_argv('exit:1')),
        TJob('baz', {'bar'}, argv=mock_argv(f'touch:{baz_path}')),
    ]
    done = await run(todo)
    assert verify_tasks(
        done,
        {
            'foo': 0,
            'bar': CalledProcessError(1, mock_argv('exit:1')),
            'baz': Cancelled,
        },
    )
    assert foo_path.is_file()
    assert not baz_path.exists()


# using the subprocess context manager for more advanced scenarios


async def test_one_ok_subprocess_with_output(run):
    argv = mock_argv('FOO')

    async def coro(ctx):
        with ctx.tjob.subprocess_xevents(argv, result=0):
            async with ctx.subprocess(argv, stdout=PIPE) as proc:
                output = await proc.stdout.read()
                await proc.wait()
        return output

    todo = [TJob('foo', coro=coro)]
    done = await run(todo)
    assert verify_tasks(done, {'foo': b'FOO\n'})


async def test_one_ok_subprocess_with_error_output(run):
    argv = mock_argv('err:', 'FOO')

    async def coro(ctx):
        with ctx.tjob.subprocess_xevents(argv, result=0):
            async with ctx.subprocess(argv, stderr=PIPE) as proc:
                output = await proc.stderr.read()
                await proc.wait()
        return output

    todo = [TJob('foo', coro=coro)]
    done = await run(todo)
    assert verify_tasks(done, {'foo': b'FOO\n'})


async def test_one_ok_subprocess_with_error_and_output_redirected(run):
    argv = mock_argv('err:', 'FOO', 'out:', 'BAR')

    async def coro(ctx):
        with ctx.tjob.subprocess_xevents(argv, result=0):
            async with ctx.subprocess(
                argv, stdout=PIPE, stderr=STDOUT
            ) as proc:
                output = await proc.stdout.read()
                await proc.wait()
        return output

    todo = [TJob('foo', coro=coro)]
    done = await run(todo)
    assert verify_tasks(done, {'foo': b'FOO\nBAR\n'})


async def test_one_failing_subprocess_with_output_default_check(run):
    argv = mock_argv('FOO', 'exit:1')

    async def coro(ctx):
        with ctx.tjob.subprocess_xevents(argv, result=1):
            async with ctx.subprocess(argv, stdout=PIPE) as proc:
                output = await proc.stdout.read()
                await proc.wait()
                assert proc.returncode == 1
        return output

    todo = [TJob('foo', coro=coro)]
    done = await run(todo)
    assert verify_tasks(done, {'foo': b'FOO\n'})


async def test_one_failing_subprocess_with_output_disable_check(run):
    argv = mock_argv('FOO', 'exit:1')

    async def coro(ctx):
        with ctx.tjob.subprocess_xevents(argv, result=1):
            async with ctx.subprocess(argv, stdout=PIPE, check=False) as proc:
                output = await proc.stdout.read()
                await proc.wait()
                assert proc.returncode == 1
        return output

    todo = [TJob('foo', coro=coro)]
    done = await run(todo)
    assert verify_tasks(done, {'foo': b'FOO\n'})


async def test_one_failing_subprocess_with_output_enable_check(run):
    argv = mock_argv('FOO', 'exit:1')

    async def coro(ctx):
        with ctx.tjob.subprocess_xevents(argv, result=1):
            async with ctx.subprocess(argv, stdout=PIPE, check=True) as proc:
                output = await proc.stdout.read()
                assert output == b'FOO\n'
                await proc.wait()
                assert proc.returncode == 1

        assert False  # skipped due to CalledProcessError on context exit

    todo = [TJob('foo', coro=coro)]
    done = await run(todo)
    assert verify_tasks(done, {'foo': CalledProcessError(1, argv)})


async def test_subprocess_with_custom_cwd(run, tmp_path):
    argv = mock_argv('cwd:')
    cwd = tmp_path / 'custom'
    cwd.mkdir()

    async def coro(ctx):
        with ctx.tjob.subprocess_xevents(argv, result=0):
            async with ctx.subprocess(argv, stdout=PIPE, cwd=cwd) as proc:
                output = await proc.stdout.read()
                await proc.wait()
        return Path(os.fsdecode(output.rstrip()))

    todo = [TJob('foo', coro=coro)]
    done = await run(todo)
    assert verify_tasks(done, {'foo': cwd})


async def test_subprocess_with_custom_env(run):
    argv = mock_argv('env:FOOBAR')
    env = os.environ.copy()
    env['FOOBAR'] = 'BAZZLE'

    async def coro(ctx):
        with ctx.tjob.subprocess_xevents(argv, result=0):
            async with ctx.subprocess(argv, stdout=PIPE, env=env) as proc:
                output = await proc.stdout.read()
                await proc.wait()
        return output

    todo = [TJob('foo', coro=coro)]
    done = await run(todo)
    assert verify_tasks(done, {'foo': b'BAZZLE\n'})


# proper termination/killing on subprocess misbehavior


async def test_not_awaiting_subprocess_terminates_it(run):
    argv = mock_argv('FOO', 'sleep:5')

    async def coro(ctx):
        with ctx.tjob.subprocess_xevents(argv, result='terminate'):
            async with ctx.subprocess(argv, stdout=PIPE) as proc:
                output = await proc.stdout.readline()
                # MISSING await proc.wait() here to trigger termination
        return output

    todo = [TJob('foo', coro=coro)]
    done = await run(todo)
    assert verify_tasks(done, {'foo': b'FOO\n'})


async def test_partial_read_from_subprocess_before_terminating_it(run):
    argv = mock_argv('FOO', 'BAR', 'sleep:5', 'BAZ')

    async def coro(ctx):
        with ctx.tjob.subprocess_xevents(argv, result='terminate'):
            async with ctx.subprocess(argv, stdout=PIPE) as proc:
                output = await proc.stdout.readline()
                output += await proc.stdout.readline()
                # MISSING await proc.wait() here to trigger termination
        return output

    todo = [TJob('foo', coro=coro)]
    done = await run(todo)
    assert verify_tasks(done, {'foo': b'FOO\nBAR\n'})


async def test_non_terminating_subprocess_is_killed(run):
    argv = mock_argv('ignore:SIGTERM', 'FOO', 'sleep:5')

    async def coro(ctx):
        with ctx.tjob.subprocess_xevents(argv, result='kill'):
            async with ctx.subprocess(
                argv, stdout=PIPE, kill_delay=0.1
            ) as proc:
                output = await proc.stdout.readline()
                # MISSING await proc.wait() here to trigger termination
        return output

    todo = [TJob('foo', coro=coro)]
    with assert_elapsed_time(lambda t: t < 0.5):
        done = await run(todo)
    assert verify_tasks(done, {'foo': b'FOO\n'})


async def test_killing_subprocess_w_check_raises_CalledProcessError(run):
    argv = mock_argv('ignore:SIGTERM', 'FOO', 'sleep:5')

    async def coro(ctx):
        with ctx.tjob.subprocess_xevents(argv, result='kill'):
            async with ctx.subprocess(
                argv, stdout=PIPE, check=True, kill_delay=0.1
            ) as proc:
                output = await proc.stdout.readline()
                # MISSING await proc.wait() here to trigger termination
        return output

    todo = [TJob('foo', coro=coro)]
    with assert_elapsed_time(lambda t: t < 0.5):
        done = await run(todo)
    assert verify_tasks(done, {'foo': CalledProcessError(-9, argv)})


# multiple threads/subprocesses from a single job


async def test_two_threads_sequentially_on_same_ticket_succeeds(run):
    def func(ret):
        time.sleep(0.001)
        return ret

    async def coro(ctx):
        async with ctx.reserve_worker() as ticket:
            with ctx.tjob.thread_xevents():
                result = await ctx.call_in_thread(func, 'FOO', ticket=ticket)
            with ctx.tjob.thread_xevents(reuse_ticket=True):
                result += await ctx.call_in_thread(func, 'BAR', ticket=ticket)
            return result

    job = TJob('foo', coro=coro)
    done = await run([job])
    assert verify_tasks(done, {'foo': 'FOOBAR'})


def expect_ValueError_already_in_use(task):
    if not isinstance(task.exception(), ValueError):
        return False
    (msg,) = task.exception().args
    return re.match(r'Cannot use ticket \d+: Already in use$', msg)


async def test_two_threads_concurrently_on_same_ticket_fails(run):
    def func(ret):
        time.sleep(0.001)
        return ret

    async def coro(ctx):
        async with ctx.reserve_worker() as ticket:
            with ctx.tjob.thread_xevents():  # Only one thread started
                results = await asyncio.gather(
                    ctx.call_in_thread(func, 'FOO', ticket=ticket),
                    ctx.call_in_thread(func, 'BAR', ticket=ticket),
                    return_exceptions=True,
                )
            for result in results:
                if isinstance(result, BaseException):
                    raise result
            return results

    job = TJob('foo', coro=coro)
    done = await run([job])
    assert verify_tasks(done, {'foo': expect_ValueError_already_in_use})


async def test_two_threads_concurrently(run, num_workers):
    def func(ret):
        time.sleep(0.01)
        return ret

    async def coro(ctx):
        results = await asyncio.gather(
            ctx.call_in_thread(func, 'FOO'),
            ctx.call_in_thread(func, 'BAR'),
            return_exceptions=True,
        )
        for result in results:
            if isinstance(result, BaseException):
                raise result
        return results

    job = TJob('foo', coro=coro)
    if num_workers < 2:  # Cannot reserve second worker
        with job.thread_xevents():  # Only one thread started
            expect_result = RuntimeError(
                f'Cannot reserve >={num_workers} worker(s)!'
            )
    else:
        with job.thread_xevents():
            with job.thread_xevents():  # Both threads started
                expect_result = ['FOO', 'BAR']
    done = await run([job])
    assert verify_tasks(done, {'foo': expect_result})


async def test_two_subprocesses_sequentially_on_same_ticket_succeeds(run):
    argv = mock_argv()

    async def coro(ctx):
        async with ctx.reserve_worker() as ticket:
            with ctx.tjob.subprocess_xevents(argv, result=0):
                result = await ctx.run_in_subprocess(argv, ticket=ticket)
            with ctx.tjob.subprocess_xevents(
                argv, result=0, reuse_ticket=True
            ):
                result += await ctx.run_in_subprocess(argv, ticket=ticket)
            return result

    job = TJob('foo', coro=coro)
    done = await run([job])
    assert verify_tasks(done, {'foo': 0})


async def test_two_subprocesses_concurrently_on_same_ticket_fails(run):
    argv = mock_argv()

    async def coro(ctx):
        async with ctx.reserve_worker() as ticket:
            with ctx.tjob.subprocess_xevents(argv, result=0):  # Only one
                results = await asyncio.gather(
                    ctx.run_in_subprocess(argv, ticket=ticket),
                    ctx.run_in_subprocess(argv, ticket=ticket),
                    return_exceptions=True,
                )
            for result in results:
                if isinstance(result, BaseException):
                    raise result
            return results

    job = TJob('foo', coro=coro)
    done = await run([job])
    assert verify_tasks(done, {'foo': expect_ValueError_already_in_use})


async def test_two_subprocesses_concurrently(run, num_workers):
    argv = mock_argv()

    async def coro(ctx):
        results = await asyncio.gather(
            ctx.run_in_subprocess(argv),
            ctx.run_in_subprocess(argv),
            return_exceptions=True,
        )
        for result in results:
            if isinstance(result, Exception):
                raise result
        return results

    job = TJob('foo', coro=coro)
    if num_workers < 2:  # Cannot reserve second worker
        with job.subprocess_xevents(argv, result=0):  # Only one subprocess
            expect_result = RuntimeError(
                f'Cannot reserve >={num_workers} worker(s)!'
            )
    else:
        with job.subprocess_xevents(argv, result=0):
            with job.subprocess_xevents(argv, result=0):
                expect_result = [0, 0]
    done = await run([job])
    assert verify_tasks(done, {'foo': expect_result})


async def test_two_subprocesses_with_output_concurrently(run, num_workers):
    argv1 = mock_argv('FOO')
    argv2 = mock_argv('BAR')

    async def coro(ctx):
        async with ctx.subprocess(argv1, stdout=PIPE) as proc1:
            async with ctx.subprocess(argv2, stdout=PIPE) as proc2:
                output2 = await proc2.stdout.read()
                await proc2.wait()
            output1 = await proc1.stdout.read()
            await proc1.wait()
        return output1 + output2

    job = TJob('foo', coro=coro)
    if num_workers < 2:  # Cannot reserve second worker
        with job.subprocess_xevents(argv1, result='terminate'):
            expect_result = RuntimeError(
                f'Cannot reserve >={num_workers} worker(s)!'
            )
    else:
        with job.subprocess_xevents(argv1, result=0):
            with job.subprocess_xevents(argv2, result=0):
                expect_result = b'FOO\nBAR\n'
    done = await run([job])
    assert verify_tasks(done, {'foo': expect_result})


async def test_pass_data_between_subprocesses(run, num_workers):
    if num_workers < 2:
        pytest.skip('need Scheduler with at least 2 workers')

    argv_src = mock_argv('FOO', 'BAR', 'BAZ')
    argv_dst = mock_argv('foo', 'in:', 'bar', 'in:', 'baz', 'in:')

    async def coro(ctx):
        with ctx.tjob.subprocess_xevents(argv_src, result=0):
            async with ctx.subprocess(argv_src, stdout=PIPE) as proc_src:
                with ctx.tjob.subprocess_xevents(argv_dst, result=0):
                    async with ctx.subprocess(
                        argv_dst, stdin=PIPE, stdout=PIPE
                    ) as proc_dst:
                        await copy_lines(proc_src.stdout, proc_dst.stdin)
                        output = await proc_dst.stdout.read()
                        await proc_dst.wait()
                await proc_src.wait()
        return output

    job = TJob('foo', coro=coro)
    done = await run([job])
    assert verify_tasks(done, {'foo': b'foo\nFOO\nbar\nBAR\nbaz\nBAZ\n'})


async def test_pass_data_between_subprocesses_outer_fails(run, num_workers):
    if num_workers < 2:
        pytest.skip('need Scheduler with at least 2 workers')

    argv_src = mock_argv('FOO', 'BAR', 'exit:1')
    argv_dst = mock_argv('foo', 'in:', 'bar', 'in:', 'baz', 'in:')
    # Outer/src process does exit 1 while inner process is waiting for input.
    # Inner/dst process gets EOF, prints empty string, and does exit 0.

    async def coro(ctx):
        with ctx.tjob.subprocess_xevents(argv_src, result=1):
            async with ctx.subprocess(
                argv_src, stdout=PIPE, check=True
            ) as proc_src:
                with ctx.tjob.subprocess_xevents(argv_dst, result=0):
                    async with ctx.subprocess(
                        argv_dst, stdin=PIPE, stdout=PIPE, check=True
                    ) as proc_dst:
                        await copy_lines(proc_src.stdout, proc_dst.stdin)
                        output = await proc_dst.stdout.read()
                        await proc_dst.wait()
                await proc_src.wait()
        return output

    job = TJob('foo', coro=coro)
    done = await run([job])
    assert verify_tasks(done, {'foo': CalledProcessError(1, argv_src)})


async def test_pass_data_between_subprocesses_inner_fails(run, num_workers):
    if num_workers < 2:
        pytest.skip('need Scheduler with at least 2 workers')

    argv_src = mock_argv('FOO', 'BAR', 'BAZ')
    argv_dst = mock_argv('foo', 'in:', 'bar', 'in:', 'exit:2')
    # Outer/src process produces all its output and does exit 0.
    # Inner/dst process does exit 2 without consuming all input.
    # Adding check=True here complicates the test as the outer process
    # may or may not be terminated by the inner's CalledProcessError(2).

    async def coro(ctx):
        with ctx.tjob.subprocess_xevents(argv_src, result=0):
            async with ctx.subprocess(
                argv_src, stdout=PIPE, check=True
            ) as proc_src:
                with ctx.tjob.subprocess_xevents(argv_dst, result=2):
                    async with ctx.subprocess(
                        argv_dst, stdin=PIPE, stdout=PIPE
                    ) as proc_dst:
                        output = b''
                        await copy_lines(proc_src.stdout, proc_dst.stdin)
                        output += await proc_dst.stdout.read()
                        await proc_dst.wait()
                await proc_src.wait()
        return proc_dst.returncode, output

    job = TJob('foo', coro=coro)
    done = await run([job])
    assert verify_tasks(done, {'foo': (2, b'foo\nFOO\nbar\nBAR\n')})


# add_thread_job() helper


async def test_one_threaded_job_before_another(Scheduler):
    def first_job():
        return 'return value from first job'

    def second_job():
        return 'return value from second job'

    scheduler = Scheduler()
    scheduler.add_thread_job('first_job', first_job)
    scheduler.add_thread_job('second_job', second_job, deps={'first_job'})
    done = await scheduler.run()

    assert verify_tasks(
        done,
        {
            'first_job': 'return value from first job',
            'second_job': 'return value from second job',
        },
    )


async def test_one_threaded_failure_cancels_the_next(Scheduler):
    def first_job():
        raise ValueError('FAIL')

    def second_job():
        return 'return value from second job'

    scheduler = Scheduler()
    scheduler.add_thread_job('first_job', first_job)
    scheduler.add_thread_job('second_job', second_job, deps={'first_job'})
    done = await scheduler.run()

    assert verify_tasks(
        done, {'first_job': ValueError('FAIL'), 'second_job': Cancelled},
    )


# add_subprocess_job() helper


async def test_one_subproc_job_before_another(Scheduler):
    scheduler = Scheduler()
    scheduler.add_subprocess_job('first_job', ['true'])
    scheduler.add_subprocess_job('second_job', ['false'], deps={'first_job'})
    done = await scheduler.run()

    assert verify_tasks(done, {'first_job': 0, 'second_job': 1},)


async def test_one_subproc_job_before_another_with_check_fails(Scheduler):
    scheduler = Scheduler()
    scheduler.add_subprocess_job('first_job', ['true'], check=True)
    scheduler.add_subprocess_job(
        'second_job', ['false'], deps={'first_job'}, check=True
    )
    done = await scheduler.run()

    assert verify_tasks(
        done, {'first_job': 0, 'second_job': CalledProcessError(1, ['false'])},
    )


async def test_one_subprocess_failure_with_check_cancels_the_next(Scheduler):
    scheduler = Scheduler()
    scheduler.add_subprocess_job('first_job', ['false'], check=True)
    scheduler.add_subprocess_job('second_job', ['true'], deps={'first_job'})
    done = await scheduler.run()

    assert verify_tasks(
        done,
        {
            'first_job': CalledProcessError(1, ['false']),
            'second_job': Cancelled,
        },
    )


async def test_one_subprocess_failure_wo_check_proceeds_to_next(Scheduler):
    scheduler = Scheduler()
    scheduler.add_subprocess_job('first_job', ['false'], check=False)
    scheduler.add_subprocess_job('second_job', ['true'], deps={'first_job'})
    done = await scheduler.run()

    assert verify_tasks(done, {'first_job': 1, 'second_job': 0})


async def test_one_subprocess_failure_by_default_proceeds_to_next(Scheduler):
    scheduler = Scheduler()
    scheduler.add_subprocess_job('first_job', ['false'])
    scheduler.add_subprocess_job('second_job', ['true'], deps={'first_job'})
    done = await scheduler.run()

    assert verify_tasks(done, {'first_job': 1, 'second_job': 0})
