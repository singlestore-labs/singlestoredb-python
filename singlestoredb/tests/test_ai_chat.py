#!/usr/bin/env python
# type: ignore
"""SingleStoreChatFactory OTel header injection tests."""
import unittest
from types import ModuleType
from unittest.mock import patch

try:
    import httpx
    from singlestoredb.ai import chat as chat_mod
except ImportError:
    httpx = None
    chat_mod = None


@unittest.skipIf(chat_mod is None, 'singlestoredb.ai.chat dependencies missing')
class TestInjectOtelHeaders(unittest.TestCase):

    def test_calls_otel_inject(self):
        headers = {}

        def fake_inject(target):
            target['baggage'] = 'session=abc,turn=def'

        fake_propagate = ModuleType('opentelemetry.propagate')
        fake_propagate.inject = fake_inject
        fake_otel = ModuleType('opentelemetry')
        with patch.dict(
            'sys.modules',
            {
                'opentelemetry': fake_otel,
                'opentelemetry.propagate': fake_propagate,
            },
        ):
            chat_mod._inject_otel_headers(headers)
        self.assertEqual(headers['baggage'], 'session=abc,turn=def')

    def test_httpx_hook_appends_once(self):
        client = httpx.Client()
        try:
            chat_mod._attach_otel_request_hook(client)
            chat_mod._attach_otel_request_hook(client)
            self.assertEqual(
                client.event_hooks['request'].count(chat_mod._httpx_inject_otel),
                1,
            )
        finally:
            client.close()


if __name__ == '__main__':
    unittest.main()
