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

    @patch('singlestoredb.ai.chat.ChatOpenAI')
    def test_openai_default_timeout_is_600(self, mock_chat_openai):
        chat_mod.SingleStoreChatFactory(
            model_name='gpt-4o',
            base_url='https://example.com',
            hosting_platform='OpenAI',
        )
        self.assertTrue(mock_chat_openai.called)
        _, kwargs = mock_chat_openai.call_args
        http_client = kwargs.get('http_client')
        http_async_client = kwargs.get('http_async_client')
        try:
            self.assertIsNotNone(http_client)
            self.assertEqual(http_client.timeout.read, 600.0)
            self.assertIsNotNone(http_async_client)
            self.assertEqual(http_async_client.timeout.read, 600.0)
        finally:
            if http_client:
                http_client.close()
            if http_async_client:
                import asyncio
                asyncio.run(http_async_client.aclose())

    @patch('singlestoredb.ai.chat.ChatOpenAI')
    def test_openai_inherits_custom_client_timeout(self, mock_chat_openai):
        custom_sync = httpx.Client(timeout=httpx.Timeout(42.0))
        http_async_client = None
        try:
            chat_mod.SingleStoreChatFactory(
                model_name='gpt-4o',
                base_url='https://example.com',
                hosting_platform='OpenAI',
                http_client=custom_sync,
            )
            self.assertTrue(mock_chat_openai.called)
            _, kwargs = mock_chat_openai.call_args
            http_client = kwargs.get('http_client')
            http_async_client = kwargs.get('http_async_client')
            self.assertIs(http_client, custom_sync)
            self.assertIsNotNone(http_async_client)
            self.assertEqual(http_async_client.timeout.read, 42.0)
        finally:
            custom_sync.close()
            if http_async_client:
                import asyncio
                asyncio.run(http_async_client.aclose())


if __name__ == '__main__':
    unittest.main()
