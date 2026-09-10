#!/usr/bin/env python
"""
Time accounting for the management API.

Management calls are slow for two quite different reasons, and telling them
apart is the whole point of this module: an HTTP request that the server takes
its time answering, and a ``wait_on_*`` loop that sleeps between polls while a
deployment transitions. A stopwatch around ``create_cluster`` cannot separate
the two -- and it is the second that usually dominates -- so both are recorded
as events here.

Every management HTTP request funnels through :meth:`Manager._doit`, and every
polling sleep through :func:`sleep`, so instrumenting those two covers all of
it. Recording is off unless something asks for it:

.. code-block:: python

    from singlestoredb.management import timing

    with timing.trace() as t:
        wm.create_cluster('my-cluster', size='S-00', wait_on_active=True)

    print(t.summary())

Set ``SINGLESTOREDB_MANAGEMENT_TRACE=1`` (the ``management.trace`` option) to
log every event to stderr as it finishes instead, which needs no code change.

Traces are per-context: a trace opened on one thread does not see requests
issued on another.

"""
import contextlib
import contextvars
import re
import sys
import threading
import time
from collections.abc import Iterator
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

from .. import config


#: Kinds of event. ``REQUEST`` is time spent in an HTTP call, ``WAIT`` is time
#: spent sleeping between polls of a resource that is still transitioning.
REQUEST = 'request'
WAIT = 'wait'

#: Path segments that identify one particular resource rather than a route.
#: Collapsed to ``{id}`` so that 40 polls of one cluster aggregate into one
#: row instead of 40. UUIDs and integers cover every ID the API hands out.
_UUID_RE = re.compile(
    r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}'
    r'-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$',
)
_INT_RE = re.compile(r'^\d+$')


def route_of(method: str, path: str) -> str:
    """
    Return the aggregation key for a request.

    Parameters
    ----------
    method : str
        HTTP method
    path : str
        Path of the resource, relative to the version root, as passed to
        :meth:`Manager._get` and friends

    Returns
    -------
    str
        The method and path with resource IDs replaced by ``{id}``, e.g.
        ``GET clusters/{id}``

    """
    # Query strings are part of the path for some callers; they are noise here.
    path = path.split('?')[0].strip('/')
    parts = [
        '{id}' if _UUID_RE.match(x) or _INT_RE.match(x) else x
        for x in path.split('/')
    ]
    return '{} {}'.format(method.upper(), '/'.join(parts))


class Event:
    """
    One timed operation.

    This object is not instantiated directly; :func:`record_request` and
    :func:`sleep` create them.

    """

    __slots__ = (
        'kind', 'label', 'duration', 'started_at', 'status',
        'retries', 'request_bytes', 'response_bytes', 'error',
    )

    def __init__(
        self,
        kind: str,
        label: str,
        duration: float,
        started_at: float,
        status: Optional[int] = None,
        retries: int = 0,
        request_bytes: Optional[int] = None,
        response_bytes: Optional[int] = None,
        error: Optional[str] = None,
    ):
        #: Kind of event: REQUEST or WAIT
        self.kind = kind

        #: Aggregation key: a route for a request, a reason for a wait
        self.label = label

        #: Seconds the operation took
        self.duration = duration

        #: Value of :func:`time.monotonic` when the operation started
        self.started_at = started_at

        #: HTTP status code, if the request got a response
        self.status = status

        #: Number of transport-level retries urllib3 made inside this request.
        #: Non-zero here means the duration includes retry backoff.
        self.retries = retries

        #: Size of the request body in bytes
        self.request_bytes = request_bytes

        #: Size of the response body in bytes
        self.response_bytes = response_bytes

        #: Exception type name, if the request never got a response
        self.error = error

    def __str__(self) -> str:
        out = f'{self.duration:7.3f}s {self.label}'
        if self.error is not None:
            out += f' -> {self.error}'
        elif self.status is not None:
            out += f' -> {self.status}'
        if self.retries:
            out += f' (retries={self.retries})'
        return out

    def __repr__(self) -> str:
        return f'<Event {self}>'


class Stat:
    """Aggregate of every :class:`Event` sharing a label."""

    __slots__ = ('label', 'calls', 'total', 'min', 'max', 'retries', 'errors')

    def __init__(self, label: str):
        #: The shared label
        self.label = label

        #: Number of events
        self.calls = 0

        #: Total seconds across all of them
        self.total = 0.0

        #: Fastest and slowest of them, in seconds
        self.min = 0.0
        self.max = 0.0

        #: Total transport-level retries
        self.retries = 0

        #: Number of events that never got a response
        self.errors = 0

    @property
    def mean(self) -> float:
        """Mean seconds per event."""
        return self.total / self.calls if self.calls else 0.0

    def add(self, event: 'Event') -> None:
        """Fold an event into the aggregate."""
        self.min = event.duration if not self.calls else min(self.min, event.duration)
        self.max = max(self.max, event.duration)
        self.calls += 1
        self.total += event.duration
        self.retries += event.retries
        if event.error is not None:
            self.errors += 1

    def __str__(self) -> str:
        return '{} calls={} total={:.3f}s mean={:.3f}s max={:.3f}s'.format(
            self.label, self.calls, self.total, self.mean, self.max,
        )

    def __repr__(self) -> str:
        return f'<Stat {self}>'


class Trace:
    """
    Collector of management API timing events.

    Use :func:`trace` rather than instantiating this directly.

    """

    def __init__(self) -> None:
        #: Every event recorded, in completion order
        self.events: List[Event] = []

        self._lock = threading.Lock()
        self._started_at: Optional[float] = None
        self._stopped_at: Optional[float] = None
        self._token: Optional[contextvars.Token[Tuple['Trace', ...]]] = None

    @classmethod
    def of(cls, events: Any, elapsed: float) -> 'Trace':
        """
        Return a stopped trace holding ``events`` and reporting ``elapsed``.

        For traces that are derived rather than collected -- a nested trace's
        events subtracted from its parent's, say -- where the wall clock the
        result stands for is not one this object measured.

        Parameters
        ----------
        events : iterable of :class:`Event`
            Events the trace should hold, in any order
        elapsed : float
            Seconds :attr:`elapsed` should report

        Returns
        -------
        :class:`Trace`

        """
        out = cls()
        out.events = sorted(events, key=lambda x: x.started_at)
        out._started_at = 0.0
        out._stopped_at = max(0.0, elapsed)
        return out

    @classmethod
    def combine(cls, traces: Any) -> 'Trace':
        """
        Return one trace holding every event from ``traces``.

        The result's :attr:`elapsed` is the sum of theirs, so it reads as the
        time the traced sections covered between them rather than as wall clock
        -- the sections need not have been contiguous.

        Parameters
        ----------
        traces : iterable of :class:`Trace`
            Traces to fold together

        Returns
        -------
        :class:`Trace`

        """
        events: List[Event] = []
        elapsed = 0.0
        for one in traces:
            events.extend(one.events)
            elapsed += one.elapsed
        return cls.of(events, elapsed)

    def start(self) -> 'Trace':
        """Begin collecting events issued from this context."""
        self._started_at = time.monotonic()
        self._stopped_at = None
        self._token = _active.set(_active.get() + (self,))
        return self

    def stop(self) -> 'Trace':
        """Stop collecting."""
        self._stopped_at = time.monotonic()
        if self._token is not None:
            _active.reset(self._token)
            self._token = None
        return self

    def add(self, event: Event) -> None:
        """Record an event."""
        with self._lock:
            self.events.append(event)

    @property
    def elapsed(self) -> float:
        """Wall clock seconds the trace covers."""
        if self._started_at is None:
            return 0.0
        end = self._stopped_at if self._stopped_at is not None else time.monotonic()
        return end - self._started_at

    def total(self, kind: Optional[str] = None) -> float:
        """
        Return the seconds accounted for.

        Parameters
        ----------
        kind : str, optional
            Restrict to REQUEST or WAIT events. Defaults to all of them.

        Returns
        -------
        float

        """
        return sum(
            x.duration for x in self.events
            if kind is None or x.kind == kind
        )

    @property
    def unaccounted(self) -> float:
        """
        Seconds spent neither in a request nor sleeping.

        This is the client's own work -- JSON parsing, object construction, and
        whatever the caller did inside the trace.

        """
        return max(0.0, self.elapsed - self.total())

    def stats(self, kind: Optional[str] = None) -> List[Stat]:
        """
        Return per-label aggregates, slowest total first.

        Parameters
        ----------
        kind : str, optional
            Restrict to REQUEST or WAIT events. Defaults to all of them.

        Returns
        -------
        List[:class:`Stat`]

        """
        out: Dict[str, Stat] = {}
        for event in self.events:
            if kind is not None and event.kind != kind:
                continue
            out.setdefault(event.label, Stat(event.label)).add(event)
        return sorted(out.values(), key=lambda x: x.total, reverse=True)

    def summary(self) -> str:
        """Return a human-readable report of where the time went."""
        elapsed = self.elapsed
        lines = [f'Management API: {elapsed:.3f}s elapsed, {len(self.events)} events']

        def share(seconds: float) -> str:
            pct = 100.0 * seconds / elapsed if elapsed else 0.0
            return f'{seconds:9.3f}s  {pct:5.1f}%'

        requests = self.total(REQUEST)
        waits = self.total(WAIT)
        lines.append(
            '  requests    {}  {} calls'.format(
                share(requests), sum(1 for x in self.events if x.kind == REQUEST),
            ),
        )
        lines.append(
            '  waiting     {}  {} sleeps'.format(
                share(waits), sum(1 for x in self.events if x.kind == WAIT),
            ),
        )
        lines.append(f'  other       {share(self.unaccounted)}')

        for kind, heading in ((REQUEST, 'route'), (WAIT, 'waiting on')):
            stats = self.stats(kind)
            if not stats:
                continue
            lines.append('')
            lines.append(
                '  {:<38} {:>5} {:>9} {:>9} {:>9}'.format(
                    heading, 'calls', 'total', 'mean', 'max',
                ),
            )
            for stat in stats:
                row = '  {:<38} {:>5} {:>8.3f}s {:>8.3f}s {:>8.3f}s'.format(
                    stat.label[:38], stat.calls, stat.total, stat.mean, stat.max,
                )
                if stat.retries:
                    row += f'  retries={stat.retries}'
                if stat.errors:
                    row += f'  errors={stat.errors}'
                lines.append(row)

        return '\n'.join(lines)

    def __str__(self) -> str:
        return self.summary()

    def __repr__(self) -> str:
        return '<Trace events={} elapsed={:.3f}s>'.format(
            len(self.events), self.elapsed,
        )


#: Traces collecting events in this context. A tuple so that nesting one trace
#: inside another feeds both, and so that resetting is a single assignment.
_active: contextvars.ContextVar[Tuple[Trace, ...]] = contextvars.ContextVar(
    'singlestoredb_management_traces', default=(),
)


@contextlib.contextmanager
def trace() -> Iterator[Trace]:
    """
    Collect timing events for the duration of the block.

    Returns
    -------
    :class:`Trace`

    """
    out = Trace().start()
    try:
        yield out
    finally:
        out.stop()


def logging_enabled() -> bool:
    """Is every event being logged to stderr as it finishes?"""
    return bool(config.get_option('management.trace'))


def _emit(event: Event) -> None:
    """Hand an event to every trace collecting in this context."""
    for out in _active.get():
        out.add(event)
    if logging_enabled():
        print(f'[singlestoredb.management] {event}', file=sys.stderr)


def recording() -> bool:
    """
    Is anything recording?

    Checked before the bookkeeping in :func:`record_request` so that an
    untraced call pays for one ``ContextVar.get`` and nothing else.

    """
    return bool(_active.get()) or logging_enabled()


def _response_size(res: Any) -> Optional[int]:
    """Return the size of a response body in bytes, if it can be had cheaply."""
    length = res.headers.get('Content-Length')
    if length is not None:
        try:
            return int(length)
        except ValueError:
            pass
    try:
        return len(res.content)
    except Exception:
        return None


def _retry_count(res: Any) -> int:
    """
    Return the number of retries urllib3 made inside a request.

    Retries happen below ``requests``, so a request that took 30 seconds
    because it was retried four times is otherwise indistinguishable from one
    slow response.

    """
    try:
        history = res.raw.retries.history
    except Exception:
        return 0
    return len(history or ())


def record_request(
    method: str,
    path: str,
    duration: float,
    started_at: float,
    response: Any = None,
    error: Optional[BaseException] = None,
) -> None:
    """
    Record one management HTTP request.

    Parameters
    ----------
    method : str
        HTTP method
    path : str
        Path of the resource, relative to the version root
    duration : float
        Seconds the request took
    started_at : float
        Value of :func:`time.monotonic` when the request started
    response : requests.Response, optional
        The response, if one arrived
    error : Exception, optional
        The transport failure, if none did

    """
    if not recording():
        return
    request_bytes: Optional[int] = None
    status: Optional[int] = None
    retries = 0
    if response is not None:
        status = response.status_code
        retries = _retry_count(response)
        body = getattr(response.request, 'body', None)
        if body is not None:
            request_bytes = len(body)
    _emit(
        Event(
            REQUEST, route_of(method, path), duration, started_at,
            status=status, retries=retries, request_bytes=request_bytes,
            response_bytes=None if response is None else _response_size(response),
            error=None if error is None else type(error).__name__,
        ),
    )


@contextlib.contextmanager
def timed(label: str, kind: str = WAIT) -> Iterator[None]:
    """
    Record the duration of a block of blocking work.

    For the parts of a wait that are not an HTTP request and not a sleep --
    a connection probe, say -- which would otherwise land in
    :attr:`Trace.unaccounted` with no label on it.

    Parameters
    ----------
    label : str
        Aggregation key for the block
    kind : str, optional
        REQUEST or WAIT. Defaults to WAIT.

    """
    if not recording():
        yield
        return
    started_at = time.monotonic()
    try:
        yield
    finally:
        _emit(Event(kind, label, time.monotonic() - started_at, started_at))


def now() -> float:
    """Monotonic clock reading, for measuring what a poll iteration cost."""
    return time.monotonic()


def poll_cost(started_at: float, interval: float) -> float:
    """
    Seconds to charge one poll iteration against its wait timeout.

    The ``wait_on_*`` loops used to charge every iteration a flat ``interval``,
    which made ``wait_timeout`` a poll count rather than a duration: the
    refetch between sleeps costs real time -- and, since the session gained
    retries and a 180 second read timeout, can cost minutes of it -- that the
    countdown never saw. A caller asking to wait 600 seconds could wait far
    longer with no timeout raised. Charging the measured wall time instead
    makes ``wait_timeout`` a genuine ceiling.

    The floor of ``interval`` keeps the loops bounded when :func:`time.sleep`
    is patched out, as the offline tests do to poll without waiting. There the
    measured time is ~0, and an iteration charged ~0 would never exhaust the
    timeout.

    Parameters
    ----------
    started_at : float
        Reading from :func:`now` taken at the top of the iteration
    interval : float
        Nominal seconds between polls, used as the floor

    Returns
    -------
    float

    """
    return max(interval, now() - started_at)


def sleep(seconds: float, label: str) -> None:
    """
    Sleep between polls of a resource, recording the time as a wait.

    Every ``wait_on_*`` loop sleeps through here so that time spent waiting on
    the server is reported separately from time spent talking to it.

    Parameters
    ----------
    seconds : float
        Seconds to sleep
    label : str
        What is being waited on, e.g. ``cluster state -> ACTIVE``. Used as the
        aggregation key in :meth:`Trace.summary`.

    """
    if not recording():
        time.sleep(seconds)
        return
    started_at = time.monotonic()
    time.sleep(seconds)
    _emit(Event(WAIT, label, time.monotonic() - started_at, started_at))
