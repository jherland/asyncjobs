"""Verify expected events from the Scheduler.

Allow test jobs to declare a sequence of expected events, and verify that
these events are indeed emitted by the Scheduler.
"""
import time

# Wildcard used for don't-care values in expected event fields
Whatever = object()


def verify_one(expect, actual):
    """Verify one event against its expected counterpart."""
    expect_keys = set(expect.keys()) - {'MAY_CANCEL'}
    assert expect_keys == set(actual.keys())
    keys = sorted(expect_keys)
    for e, a in zip((expect[k] for k in keys), (actual[k] for k in keys)):
        if e is not Whatever:
            assert e == a
    return True


class ExpectedJobEvents:
    """Keep track of expected events per job."""

    def __init__(self, job_name):
        self.name = job_name
        self.expected = []
        self.verifying = False

    def add(self, event, *, may_cancel=False, **kwargs):
        assert not self.verifying
        d = {'event': event, 'job': self.name, 'timestamp': Whatever}
        d.update(kwargs)
        if may_cancel:
            d['MAY_CANCEL'] = True
        self.expected.append(d)

    def end(self):
        """Call this when the job ends, before we start verifying events."""
        assert not self.verifying
        if self.expected[-1]['event'] != 'finish':  # we were cancelled
            self.add('finish', fate='cancelled')
        self.verifying = True

    def verify_next(self, actual):
        assert self.verifying
        if actual['event'] == 'finish' and actual['fate'] == 'cancelled':
            while self.expected[0].get('MAY_CANCEL', False):
                self.expected.pop(0)
        return verify_one(self.expected.pop(0), actual)

    def __len__(self):
        return len(self.expected)


class EventVerifier:
    """Keep track of emitted events."""

    def __init__(self):
        self._start = time.time()
        self._end = None
        self.events = []

    def __call__(self, event):
        assert self._end is None
        self.events.append(event)

    def end(self):
        assert self._end is None
        self._end = time.time()

    def _verify_timestamps(self):
        """Timestamps are in sorted order and all between ._start and ._end."""
        assert self._end is not None
        timestamps = [e['timestamp'] for e in self.events]
        assert timestamps == sorted(timestamps)
        assert timestamps[0] >= self._start
        assert timestamps[-1] <= self._end

    def _verify_initial_adds(self, initial_xjobs):
        """Jobs added before Scheduler.run() emit 'add' events in order.

        Verify that the 'n' first events are 'add' events for the first 'n'
        jobs, in order.
        """
        for i in range(len(initial_xjobs)):
            actual = self.events.pop(0)
            assert actual['event'] == 'add'
            assert initial_xjobs[i].verify_next(actual)

    def _verify_overall_start_and_finish(self, num_initial, num_total):
        """Overall execution is bookended by start and finish events."""
        first, last = self.events.pop(0), self.events.pop()
        assert verify_one(
            {
                'event': 'start',
                'num_jobs': num_initial,
                'keep_going': Whatever,
                'timestamp': Whatever,
            },
            first,
        )
        assert verify_one(
            {'event': 'finish', 'num_tasks': num_total, 'timestamp': Whatever},
            last,
        )

    def _verify_initial_starts(self, initial_xjobs):
        """Initial jobs are started when Scheduler starts running."""
        for i in range(len(initial_xjobs)):
            actual = self.events.pop(0)
            assert actual['event'] == 'start'
            assert initial_xjobs[i].verify_next(actual)

    def _verify_await_tasks(self, initial_job_names):
        """Task running is bookended by "await/awaited tasks" events."""
        first, last = self.events.pop(0), self.events.pop()
        assert verify_one(
            {
                'event': 'await tasks',
                'jobs': initial_job_names,
                'timestamp': Whatever,
            },
            first,
        )
        assert verify_one(
            {'event': 'awaited tasks', 'timestamp': Whatever}, last,
        )

    def verify_all(self, initial_xevents, spawned_xevents):
        """Verify all events emitted by a scheduler instance."""
        # Transition into verifying state
        if self._end is None:
            self.end()
        for xjob in initial_xevents + spawned_xevents:
            xjob.end()

        total_jobs = len(initial_xevents) + len(spawned_xevents)

        self._verify_timestamps()
        self._verify_initial_adds(initial_xevents)
        self._verify_overall_start_and_finish(len(initial_xevents), total_jobs)

        if initial_xevents or spawned_xevents:
            self._verify_initial_starts(initial_xevents)
            self._verify_await_tasks([xjob.name for xjob in initial_xevents])

            # Remaining events belong to individual jobs; this includes
            # expected events from jobs in spawned_xevents.
            xjobs = {x.name: x for x in initial_xevents + spawned_xevents}
            assert len(xjobs) == total_jobs

            # Verify each event by pairing it w/that job's next expected event
            # (we cannot deterministically sequence events between jobs)
            while self.events:
                actual = self.events.pop(0)
                if actual['event'] == 'cancelling tasks':
                    continue
                xjobs[actual['job']].verify_next(actual)

            # No more expected events
            assert all(len(xjob) == 0 for xjob in xjobs.values())

        assert not self.events  # no more actual events
        return True
