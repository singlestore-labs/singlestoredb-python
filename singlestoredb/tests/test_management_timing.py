#!/usr/bin/env python
# type: ignore
"""
Time accounting for the management API.

These are unit tests: no token, no deployment and no HTTP. The point of the
module under test is to separate time spent in requests from time spent
sleeping in a ``wait_on_*`` loop, so that is what is asserted -- against a
mocked session, with :func:`time.sleep` patched out.
"""
import contextlib
import io
import unittest
from unittest.mock import MagicMock
from unittest.mock import patch

import singlestoredb as s2
from singlestoredb.management import timing


FAKE_TOKEN = 'test-token-12345'
FAKE_BASE_URL = 'https://api.example.com'
FAKE_ID = '44444444-4444-4444-8444-444444444444'


def _response(status_code=200, body=b'{}', request_body=None, retries=None):
    """Return a stand-in for a requests.Response."""
    out = MagicMock()
    out.status_code = status_code
    out.headers = {'Content-Length': str(len(body))}
    out.content = body
    out.request.body = request_body
    if retries is None:
        del out.raw.retries
    else:
        out.raw.retries.history = retries
    return out


@contextlib.contextmanager
def _tracing(enabled):
    """
    Force the ``management.trace`` option for the duration of the block.

    Both directions are needed: the option is read from
    ``SINGLESTOREDB_MANAGEMENT_TRACE``, which a traced test run sets, so the
    tests that assert nothing is recorded have to turn it off explicitly rather
    than assume it. Any ambient trace is detached for the same reason -- the
    conftest opens one around every test in a traced run.
    """
    token = timing._active.set(())
    s2.config.set_option('management.trace', enabled)
    try:
        yield
    finally:
        s2.config.reset_option('management.trace')
        timing._active.reset(token)


def _manager(response=None):
    """Return a Manager whose session answers with ``response``."""
    from singlestoredb.management.manager import Manager
    with patch(
        'singlestoredb.management.manager.get_token',
        return_value=FAKE_TOKEN,
    ):
        mgr = Manager(access_token=FAKE_TOKEN, base_url=FAKE_BASE_URL)
    mgr._sess = MagicMock()
    mgr._sess.get.return_value = response or _response()
    mgr._sess.post.return_value = response or _response()
    return mgr


class TestRouteOf(unittest.TestCase):
    """Aggregation keys. One row per route, not one per resource."""

    def test_ids_are_collapsed(self):
        self.assertEqual(
            timing.route_of('get', f'clusters/{FAKE_ID}'),
            'GET clusters/{id}',
        )
        self.assertEqual(
            timing.route_of('get', 'jobs/12345/executions'),
            'GET jobs/{id}/executions',
        )

    def test_route_segments_are_kept(self):
        self.assertEqual(
            timing.route_of('post', 'clusters'), 'POST clusters',
        )
        self.assertEqual(
            timing.route_of('get', 'regions/sharedtier'),
            'GET regions/sharedtier',
        )

    def test_query_strings_and_slashes_are_dropped(self):
        self.assertEqual(
            timing.route_of('get', '/clusters/?force=true'), 'GET clusters',
        )


class TestRecording(unittest.TestCase):
    """What is and is not collected."""

    def test_nothing_is_recorded_without_a_trace(self):
        with _tracing(False):
            self.assertFalse(timing.recording())
            # Nothing to assert but that this does not raise or cost
            # anything: the event is dropped before an Event is even built.
            timing.record_request('get', 'clusters', 1.0, 0.0)

    def test_a_trace_collects_requests(self):
        mgr = _manager(_response(body=b'{"a": 1}', request_body=b'{"b": 2}'))
        with timing.trace() as trace:
            mgr._get('clusters')
            mgr._get(f'clusters/{FAKE_ID}')

        self.assertEqual(len(trace.events), 2)
        self.assertEqual(
            [x.label for x in trace.events],
            ['GET clusters', 'GET clusters/{id}'],
        )
        for event in trace.events:
            self.assertEqual(event.kind, timing.REQUEST)
            self.assertEqual(event.status, 200)
            self.assertEqual(event.response_bytes, 8)
            self.assertEqual(event.request_bytes, 8)
            self.assertEqual(event.retries, 0)
            self.assertIsNone(event.error)
            self.assertGreaterEqual(event.duration, 0.0)

    def test_a_trace_stops_collecting_at_the_end_of_the_block(self):
        mgr = _manager()
        with timing.trace() as trace:
            mgr._get('clusters')
        mgr._get('clusters')
        self.assertEqual(len(trace.events), 1)

    def test_retries_are_reported(self):
        """
        A retried request has to be distinguishable from a slow one.

        Retries happen under ``requests``, so without this the backoff shows up
        as one long response and nothing says why.
        """
        mgr = _manager(_response(retries=[object(), object()]))
        with timing.trace() as trace:
            mgr._get('clusters')
        self.assertEqual(trace.events[0].retries, 2)
        self.assertEqual(trace.stats()[0].retries, 2)

    def test_a_failed_request_is_recorded_with_its_error(self):
        import requests
        mgr = _manager()
        mgr._sess.get.side_effect = requests.exceptions.ConnectionError('boom')
        with timing.trace() as trace:
            with self.assertRaises(s2.ManagementError):
                mgr._get('clusters')
        self.assertEqual(len(trace.events), 1)
        self.assertEqual(trace.events[0].error, 'ConnectionError')
        self.assertIsNone(trace.events[0].status)
        self.assertEqual(trace.stats()[0].errors, 1)

    def test_an_http_error_is_still_a_recorded_request(self):
        mgr = _manager(_response(status_code=404, body=b'nope'))
        with timing.trace() as trace:
            with self.assertRaises(s2.ManagementError):
                mgr._get('clusters')
        self.assertEqual(trace.events[0].status, 404)

    def test_nested_traces_both_collect(self):
        mgr = _manager()
        with timing.trace() as outer:
            mgr._get('clusters')
            with timing.trace() as inner:
                mgr._get('regions')
            mgr._get('clusters')

        self.assertEqual([x.label for x in inner.events], ['GET regions'])
        self.assertEqual(
            [x.label for x in outer.events],
            ['GET clusters', 'GET regions', 'GET clusters'],
        )


class TestWaiting(unittest.TestCase):
    """Polling sleeps, which is where the wall clock usually goes."""

    def test_sleep_is_recorded_as_a_wait(self):
        with patch('singlestoredb.management.timing.time.sleep') as slept:
            with timing.trace() as trace:
                timing.sleep(20, 'cluster state -> active')
        slept.assert_called_once_with(20)
        self.assertEqual(len(trace.events), 1)
        self.assertEqual(trace.events[0].kind, timing.WAIT)
        self.assertEqual(trace.events[0].label, 'cluster state -> active')

    def test_sleep_still_sleeps_when_nothing_is_recording(self):
        with patch('singlestoredb.management.timing.time.sleep') as slept:
            timing.sleep(20, 'cluster state -> active')
        slept.assert_called_once_with(20)

    def test_timed_labels_blocking_work_that_is_neither(self):
        with timing.trace() as trace:
            with timing.timed('cluster endpoint connect'):
                pass
        self.assertEqual(trace.events[0].kind, timing.WAIT)
        self.assertEqual(trace.events[0].label, 'cluster endpoint connect')

    def test_timed_records_even_when_the_block_raises(self):
        with timing.trace() as trace:
            with self.assertRaises(ValueError):
                with timing.timed('cluster endpoint connect'):
                    raise ValueError('boom')
        self.assertEqual(len(trace.events), 1)

    def test_wait_on_state_records_one_wait_per_poll(self):
        """
        The ``wait_on_*`` loops are the reason this module exists.

        Three polls of a cluster that is still PENDING have to show up as three
        waits and two requests, not as one opaque 40 seconds.
        """
        mgr = _manager()
        mgr.obj_type = 'cluster'
        pending, active = MagicMock(), MagicMock()
        pending.state = 'PENDING'
        pending.id = FAKE_ID
        active.state = 'ACTIVE'
        active.id = FAKE_ID
        mgr.get_cluster = MagicMock(side_effect=[pending, active])

        with patch('singlestoredb.management.timing.time.sleep'):
            with timing.trace() as trace:
                out = mgr._wait_on_state(pending, 'ACTIVE', interval=20)

        self.assertIs(out, active)
        waits = [x for x in trace.events if x.kind == timing.WAIT]
        self.assertEqual(len(waits), 2)
        self.assertEqual({x.label for x in waits}, {'cluster state -> active'})


class TestPollCost(unittest.TestCase):
    """
    ``wait_timeout`` has to be a duration, not a poll count.

    The loops used to charge every iteration a flat ``interval`` and never
    counted the refetch between sleeps. Since the session gained retries and a
    180 second read timeout a single poll can cost minutes, so a caller asking
    to wait 600 seconds could wait for an hour without a timeout being raised.
    """

    @contextlib.contextmanager
    def _clock(self, per_call=0.0):
        """
        Run with a fake monotonic clock and no real sleeping.

        ``per_call`` is the number of seconds each *refetch* is made to appear
        to take, which is what the old accounting ignored.
        """
        reading = [0.0]

        def advance():
            reading[0] += per_call
            return reading[0]

        with patch('singlestoredb.management.timing.time.sleep'):
            with patch(
                'singlestoredb.management.timing.now',
                side_effect=lambda: reading[0],
            ):
                yield advance

    def test_poll_cost_floors_at_the_interval(self):
        with self._clock():
            self.assertEqual(timing.poll_cost(timing.now(), 10), 10)

    def test_poll_cost_charges_measured_time_when_it_exceeds_the_interval(self):
        with self._clock(per_call=100.0) as advance:
            started_at = timing.now()
            advance()
            self.assertEqual(timing.poll_cost(started_at, 10), 100.0)

    def test_a_slow_refetch_counts_against_the_timeout(self):
        """
        Six polls of a refetch that costs 100s, not sixty of a nominal 10s.

        The whole point of the fix: ``timeout=600`` means ten minutes of wall
        clock, so a poll that really takes 100 seconds exhausts it in six
        iterations rather than sixty.
        """
        mgr = _manager()
        mgr.obj_type = 'cluster'
        pending = MagicMock()
        pending.state = 'PENDING'
        pending.id = FAKE_ID

        with self._clock(per_call=100.0) as advance:
            def refetch(id):
                advance()
                return pending
            mgr.get_cluster = MagicMock(side_effect=refetch)

            with self.assertRaises(s2.ManagementError):
                mgr._wait_on_state(
                    pending, 'ACTIVE', interval=10, timeout=600,
                )

        self.assertEqual(mgr.get_cluster.call_count, 6)

    def test_the_interval_floor_still_bounds_a_patched_out_sleep(self):
        """
        With ``time.sleep`` patched out the measured time is ~0, so without the
        floor in :func:`timing.poll_cost` nothing would ever charge the timeout
        and the loop would spin forever. Every offline test that polls relies
        on this.
        """
        mgr = _manager()
        mgr.obj_type = 'cluster'
        pending = MagicMock()
        pending.state = 'PENDING'
        pending.id = FAKE_ID
        mgr.get_cluster = MagicMock(return_value=pending)

        with self._clock():
            with self.assertRaises(s2.ManagementError):
                mgr._wait_on_state(
                    pending, 'ACTIVE', interval=20, timeout=60,
                )

        self.assertEqual(mgr.get_cluster.call_count, 3)


class TestReporting(unittest.TestCase):
    """Totals, aggregates and the summary text."""

    def _trace(self):
        """Return a stopped trace holding known durations."""
        trace = timing.Trace().start()
        trace.add(timing.Event(timing.REQUEST, 'GET clusters', 1.0, 0.0, status=200))
        trace.add(
            timing.Event(timing.REQUEST, 'GET clusters/{id}', 2.0, 1.0, status=200),
        )
        trace.add(
            timing.Event(timing.REQUEST, 'GET clusters/{id}', 4.0, 3.0, status=200),
        )
        trace.add(timing.Event(timing.WAIT, 'cluster state -> active', 40.0, 7.0))
        return trace.stop()

    def test_totals_split_requests_from_waiting(self):
        trace = self._trace()
        self.assertEqual(trace.total(timing.REQUEST), 7.0)
        self.assertEqual(trace.total(timing.WAIT), 40.0)
        self.assertEqual(trace.total(), 47.0)

    def test_stats_aggregate_by_label_slowest_first(self):
        stats = self._trace().stats(timing.REQUEST)
        self.assertEqual([x.label for x in stats], ['GET clusters/{id}', 'GET clusters'])
        first = stats[0]
        self.assertEqual(first.calls, 2)
        self.assertEqual(first.total, 6.0)
        self.assertEqual(first.mean, 3.0)
        self.assertEqual(first.min, 2.0)
        self.assertEqual(first.max, 4.0)

    def test_unaccounted_time_is_never_negative(self):
        # The events claim 47s; a trace that was open for less than that (these
        # durations are fabricated) must report 0 rather than a negative.
        self.assertEqual(self._trace().unaccounted, 0.0)

    def test_summary_names_the_split_and_the_routes(self):
        out = self._trace().summary()
        self.assertIn('requests', out)
        self.assertIn('waiting', out)
        self.assertIn('GET clusters/{id}', out)
        self.assertIn('cluster state -> active', out)

    def test_of_holds_the_given_events_and_reports_the_given_elapsed(self):
        events = self._trace().events
        out = timing.Trace.of(events, 100.0)
        self.assertEqual(len(out.events), 4)
        self.assertEqual(out.elapsed, 100.0)
        self.assertEqual(out.total(timing.WAIT), 40.0)
        # Given out of order, reported in completion order.
        shuffled = timing.Trace.of(list(reversed(events)), 100.0)
        self.assertEqual(
            [x.started_at for x in shuffled.events], [0.0, 1.0, 3.0, 7.0],
        )

    def test_of_never_reports_a_negative_elapsed(self):
        # The conftest subtracts nested traces' elapsed from their parent's,
        # and rounding or an overlapping trace could take that below zero.
        self.assertEqual(timing.Trace.of([], -5.0).elapsed, 0.0)

    def test_a_nested_trace_shares_event_objects_with_its_parent(self):
        """
        The conftest separates class-fixture time from test time by identity.

        A class-scoped trace spans its tests as well as its fixtures, so the
        fixture share is the parent's events minus the children's. That is only
        exact because :func:`timing._emit` hands the *same* Event to every
        active trace rather than a copy per trace.
        """
        mgr = _manager()
        with timing.trace() as outer:
            mgr._get('regions')
            with timing.trace() as inner:
                mgr._get('clusters')

        fixture_only = [
            x for x in outer.events if id(x) not in {
                id(y) for y in inner.events
            }
        ]
        self.assertEqual([x.label for x in fixture_only], ['GET regions'])

    def test_combine_folds_traces_and_sums_their_elapsed(self):
        one, two = self._trace(), self._trace()
        combined = timing.Trace.combine([one, two])
        self.assertEqual(len(combined.events), 8)
        self.assertEqual(combined.total(timing.WAIT), 80.0)
        self.assertAlmostEqual(combined.elapsed, one.elapsed + two.elapsed)


class TestStderrLogging(unittest.TestCase):
    """The zero-code-change path: SINGLESTOREDB_MANAGEMENT_TRACE."""

    def test_events_are_logged_when_the_option_is_on(self):
        mgr = _manager()
        err = io.StringIO()
        with _tracing(True):
            self.assertTrue(timing.recording())
            with contextlib.redirect_stderr(err):
                mgr._get(f'clusters/{FAKE_ID}')
        out = err.getvalue()
        self.assertIn('GET clusters/{id}', out)
        self.assertIn('-> 200', out)

    def test_waits_are_logged_too(self):
        err = io.StringIO()
        with _tracing(True):
            with patch('singlestoredb.management.timing.time.sleep'):
                with contextlib.redirect_stderr(err):
                    timing.sleep(20, 'cluster state -> active')
        self.assertIn('cluster state -> active', err.getvalue())

    def test_nothing_is_logged_when_the_option_is_off(self):
        mgr = _manager()
        err = io.StringIO()
        with _tracing(False):
            self.assertFalse(timing.recording())
            with contextlib.redirect_stderr(err):
                mgr._get('clusters')
        self.assertEqual(err.getvalue(), '')


if __name__ == '__main__':
    unittest.main()
