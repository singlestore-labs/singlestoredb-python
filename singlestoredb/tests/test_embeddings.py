#!/usr/bin/env python
# type: ignore
"""SingleStoreDB embeddings testing."""
import asyncio
import importlib.util
import math
import os
import sys
import types
import unittest


class MockOpenAIEmbeddings:

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        for key, value in kwargs.items():
            setattr(self, key, value)
        self.seen_documents = []
        self.async_seen_documents = []

    @staticmethod
    def _embedding_for(text):
        if text.startswith('a'):
            return [1.0, 0.0]
        if text.startswith('b'):
            return [0.0, 1.0]
        return [0.0, -1.0]

    def embed_documents(self, texts, chunk_size=None, **kwargs):
        self.seen_documents.extend(texts)
        self.seen_chunk_size = chunk_size
        self.seen_kwargs = kwargs
        return [self._embedding_for(text) for text in texts]

    async def aembed_documents(self, texts, chunk_size=None, **kwargs):
        self.async_seen_documents.extend(texts)
        self.async_seen_chunk_size = chunk_size
        self.async_seen_kwargs = kwargs
        return [self._embedding_for(text) for text in texts]


class MockBedrockEmbeddings:

    def __init__(self, **kwargs):
        self.kwargs = kwargs


class MockConfig:

    def __init__(self, **kwargs):
        self.kwargs = kwargs


class MockClient:
    pass


class MockTimeout:

    def __init__(self, connect=None, read=None):
        self.connect = connect
        self.read = read


class MockBoto3(types.ModuleType):

    def client(self, *args, **kwargs):
        return types.SimpleNamespace(
            _endpoint=types.SimpleNamespace(
                _event_emitter=types.SimpleNamespace(
                    register_first=lambda *args, **kwargs: None,
                ),
            ),
        )


class TestEmbeddings(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        sys.modules.pop('singlestoredb.ai.embeddings', None)
        sys.modules.pop('_test_embeddings_module', None)

        httpx = types.ModuleType('httpx')
        httpx.Client = MockClient
        httpx.Timeout = MockTimeout
        sys.modules['httpx'] = httpx

        langchain_openai = types.ModuleType('langchain_openai')
        langchain_openai.OpenAIEmbeddings = MockOpenAIEmbeddings
        sys.modules['langchain_openai'] = langchain_openai

        langchain_aws = types.ModuleType('langchain_aws')
        langchain_aws.BedrockEmbeddings = MockBedrockEmbeddings
        sys.modules['langchain_aws'] = langchain_aws

        botocore = types.ModuleType('botocore')
        botocore.UNSIGNED = 'unsigned'
        sys.modules['botocore'] = botocore

        botocore_config = types.ModuleType('botocore.config')
        botocore_config.Config = MockConfig
        sys.modules['botocore.config'] = botocore_config

        sys.modules['boto3'] = MockBoto3('boto3')

        path = os.path.join(os.path.dirname(__file__), '..', 'ai', 'embeddings.py')
        spec = importlib.util.spec_from_file_location('_test_embeddings_module', path)
        module = importlib.util.module_from_spec(spec)
        sys.modules['_test_embeddings_module'] = module
        assert spec.loader is not None, spec
        spec.loader.exec_module(module)
        cls.embeddings = module

    def test_non_azure_factory_sends_raw_strings_and_uses_chunk_cap(self):
        embedding = self.embeddings.SingleStoreEmbeddingsFactory(
            model_name='shared-qwen3-embed-0-6b',
            api_key='token',
            base_url='http://localhost:8000',
            hosting_platform='NovaMultiTenant',
        )

        assert isinstance(embedding, self.embeddings._ChunkedOpenAIEmbeddings)
        assert embedding.kwargs['check_embedding_ctx_length'] is False
        assert embedding.max_chunk_chars == 24000, embedding.max_chunk_chars

        embedding.embed_documents(['a' * 24001])
        assert [len(x) for x in embedding.seen_documents] == [24000, 1]

    def test_azure_factory_keeps_langchain_tokenization(self):
        embedding = self.embeddings.SingleStoreEmbeddingsFactory(
            model_name='text-embedding-3-small',
            api_key='token',
            base_url='http://localhost:8000',
            hosting_platform='Azure',
        )

        assert isinstance(embedding, MockOpenAIEmbeddings)
        assert not isinstance(embedding, self.embeddings._ChunkedOpenAIEmbeddings)
        assert embedding.kwargs['check_embedding_ctx_length'] is True

    def test_chunked_embedding_reduces_weighted_average_to_one_vector(self):
        embedding = self.embeddings._ChunkedOpenAIEmbeddings(
            model='shared-qwen3-embed-0-6b',
            api_key='token',
            base_url='http://localhost:8000',
            check_embedding_ctx_length=False,
        )
        embedding.max_chunk_chars = 3

        out = embedding.embed_documents(['aaabbb', 'zz'])

        assert embedding.seen_documents == ['aaa', 'bbb', 'zz']
        assert len(out) == 2, out
        assert math.isclose(out[0][0], 1.0 / math.sqrt(2.0)), out[0]
        assert math.isclose(out[0][1], 1.0 / math.sqrt(2.0)), out[0]
        assert out[1] == [0.0, -1.0], out[1]

    def test_async_chunked_embedding_uses_same_reduction(self):
        async def run():
            embedding = self.embeddings._ChunkedOpenAIEmbeddings(
                model='shared-qwen3-embed-0-6b',
                api_key='token',
                base_url='http://localhost:8000',
                check_embedding_ctx_length=False,
            )
            embedding.max_chunk_chars = 3

            out = await embedding.aembed_documents(['aaabbb'])

            assert embedding.async_seen_documents == ['aaa', 'bbb']
            assert len(out) == 1, out
            assert math.isclose(out[0][0], 1.0 / math.sqrt(2.0)), out[0]
            assert math.isclose(out[0][1], 1.0 / math.sqrt(2.0)), out[0]

        asyncio.run(run())


if __name__ == '__main__':
    unittest.main()
