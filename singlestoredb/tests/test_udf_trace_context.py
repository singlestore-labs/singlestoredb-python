"""Tests for W3C trace-context propagation through the UDF application."""
import asyncio
import unittest
from typing import Any
from typing import Dict
from typing import List
from typing import Tuple

from ..functions import udf
from ..functions.ext.asgi import Application
from .test_udf_event_loop import _make_invoke_args

try:
    from opentelemetry import propagate
except ImportError:
    propagate = None  # type: ignore[assignment]


TRACE_ID = '4bf92f3577b34da6a3ce929d0e0e4736'
TRACEPARENT = f'00-{TRACE_ID}-00f067aa0ba902b7-01'

# Outbound traceparent each UDF would send, keyed by the UDF's input tag.
_outbound: Dict[str, str] = {}


def _record_outbound(tag: str) -> None:
    carrier: Dict[str, str] = {}
    propagate.inject(carrier)
    _outbound[tag] = carrier.get('traceparent', '')


@udf
async def _async_trace_udf(tag: str) -> int:
    _record_outbound(tag)
    return len(tag)


@udf
def _sync_trace_udf(tag: str) -> int:
    _record_outbound(tag)
    return len(tag)


@unittest.skipIf(propagate is None, 'opentelemetry-api is not installed')
class TestUDFTraceContext(unittest.TestCase):

    def setUp(self) -> None:
        _outbound.clear()
        self.app = Application(
            functions=[_async_trace_udf, _sync_trace_udf],
            app_name='trace_test',
            disable_metrics=True,
        )

    def _invoke(
        self,
        name: str,
        tag: str,
        extra_headers: List[Tuple[bytes, bytes]],
    ) -> List[Any]:
        scope, receive, send, sent = _make_invoke_args(name, [(tag,)])
        scope['headers'] = list(scope['headers']) + extra_headers
        with self.assertLogs(
            'singlestoredb.functions.ext.asgi.trace_test', level='INFO',
        ) as logs:
            asyncio.run(self.app(scope, receive, send))
        statuses = [m for m in sent if m.get('type') == 'http.response.start']
        self.assertTrue(statuses and statuses[0]['status'] == 200, sent)
        return logs.records

    def test_async_udf_propagates_incoming_traceparent(self) -> None:
        records = self._invoke(
            '_async_trace_udf', 'alpha', [(b'traceparent', TRACEPARENT.encode())],
        )
        self.assertEqual(_outbound['alpha'], TRACEPARENT)
        self.assertEqual([getattr(r, 'trace_id') for r in records], [TRACE_ID])

    def test_sync_udf_propagates_incoming_traceparent(self) -> None:
        records = self._invoke(
            '_sync_trace_udf', 'beta', [(b'traceparent', TRACEPARENT.encode())],
        )
        self.assertEqual(_outbound['beta'], TRACEPARENT)
        self.assertEqual([getattr(r, 'trace_id') for r in records], [TRACE_ID])

    def test_request_without_traceparent_propagates_nothing(self) -> None:
        records = self._invoke('_async_trace_udf', 'gamma', [])
        self.assertEqual(_outbound['gamma'], '')
        self.assertFalse(any(hasattr(r, 'trace_id') for r in records))

    def test_invalid_traceparent_is_ignored(self) -> None:
        records = self._invoke(
            '_sync_trace_udf', 'delta', [(b'traceparent', b'00-not-a-trace-01')],
        )
        self.assertEqual(_outbound['delta'], '')
        self.assertFalse(any(hasattr(r, 'trace_id') for r in records))


if __name__ == '__main__':
    unittest.main()
