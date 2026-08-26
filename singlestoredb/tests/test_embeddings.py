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

# Stand-ins for a model's BOS/EOS, outside the fake tokenizers' ord()-derived range.
# Not the real Qwen3 EOS (151643): affixes come from the tokenizer, so no test should
# know a real special ID.
FAKE_BOS = 900001
FAKE_EOS = 900002

INJECTED_MODULES = (
    'httpx',
    'langchain_openai',
    'langchain_aws',
    'botocore',
    'botocore.config',
    'boto3',
    'transformers',
)


class MockOpenAIEmbeddings:

    specials = (FAKE_BOS, FAKE_EOS)

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        for key, value in kwargs.items():
            setattr(self, key, value)
        self.seen_documents = []
        self.async_seen_documents = []

    @classmethod
    def _embedding_for(cls, item):
        if not isinstance(item, str):
            item = ''.join(
                chr(token) for token in item if token not in cls.specials
            )
        if item.startswith('a'):
            return [1.0, 0.0]
        if item.startswith('b'):
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


class FakeTokenizer:
    """One token per character, plus configurable special-token affixes."""

    def __init__(self, prefix=(), suffix=()):
        self.prefix = list(prefix)
        self.suffix = list(suffix)

    def encode(self, text, add_special_tokens=True):
        tokens = [ord(char) for char in text]
        if add_special_tokens:
            return self.prefix + tokens + self.suffix
        return tokens


class RewritingTokenizer:
    """Rewrites content when adding specials instead of wrapping it."""

    def encode(self, text, add_special_tokens=True):
        if add_special_tokens:
            return [FAKE_BOS, FAKE_EOS]
        return [ord(char) for char in text]


class EmptyTokenizer:
    """Encodes the probe to nothing, so no affixes can be located."""

    def encode(self, text, add_special_tokens=True):
        return []


class FakeAutoTokenizer:
    """Stands in for ``transformers.AutoTokenizer`` so unit tests stay offline."""

    tokenizer = None
    error = None

    @classmethod
    def from_pretrained(cls, name, **kwargs):
        if cls.error is not None:
            raise cls.error
        return cls.tokenizer


class TestEmbeddings(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.saved_modules = {
            name: sys.modules.get(name) for name in INJECTED_MODULES
        }

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

        transformers = types.ModuleType('transformers')
        transformers.AutoTokenizer = FakeAutoTokenizer
        sys.modules['transformers'] = transformers

        path = os.path.join(os.path.dirname(__file__), '..', 'ai', 'embeddings.py')
        spec = importlib.util.spec_from_file_location('_test_embeddings_module', path)
        module = importlib.util.module_from_spec(spec)
        sys.modules['_test_embeddings_module'] = module
        assert spec.loader is not None, spec
        spec.loader.exec_module(module)
        cls.embeddings = module

    @classmethod
    def tearDownClass(cls):
        # Any later real import of these must not get the fakes. test_embeddings_live
        # would pass against a constant vector if it did.
        sys.modules.pop('_test_embeddings_module', None)
        for name, module in cls.saved_modules.items():
            if module is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = module

    def tearDown(self):
        # _load_tokenizer memoizes, so each test must start from an empty cache.
        self.embeddings._load_tokenizer.cache_clear()
        FakeAutoTokenizer.tokenizer = None
        FakeAutoTokenizer.error = None

    def use_tokenizer(self, prefix=(), suffix=()):
        FakeAutoTokenizer.tokenizer = FakeTokenizer(prefix=prefix, suffix=suffix)

    def fail_tokenizer_load(self, error):
        FakeAutoTokenizer.error = error

    def qwen_embedding(self, **kwargs):
        return self.embeddings.SingleStoreEmbeddingsFactory(
            model_name='Qwen/Qwen3-Embedding-0.6B',
            api_key='token',
            base_url='http://localhost:8000',
            hosting_platform='NovaMultiTenant',
            **kwargs,
        )

    def test_unregistered_model_sends_raw_strings_and_uses_chunk_cap(self):
        # No registry entry keeps the old behavior: text on the wire, split on
        # characters. A deployment alias lands here, so it must warn rather than look
        # like success.
        with self.assertWarns(
            self.embeddings.TokenizationFallbackWarning,
        ) as caught:
            embedding = self.embeddings.SingleStoreEmbeddingsFactory(
                model_name='shared-qwen3-embed-0-6b',
                api_key='token',
                base_url='http://localhost:8000',
                hosting_platform='NovaMultiTenant',
            )

        assert 'no tokenization policy is registered' in str(caught.warning)
        assert 'deployment alias' in str(caught.warning)
        assert isinstance(embedding, self.embeddings._ChunkedOpenAIEmbeddings)
        assert embedding.kwargs['check_embedding_ctx_length'] is False
        assert embedding.token_chunker is None
        assert embedding.max_chunk_chars == 24000, embedding.max_chunk_chars

        embedding.embed_documents(['a' * 24001])
        assert all(isinstance(x, str) for x in embedding.seen_documents)
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

    def test_registry_entry_for_qwen3_embedding(self):
        self.use_tokenizer(suffix=[FAKE_EOS])

        chunker = self.embeddings._token_chunker_for(
            'Qwen/Qwen3-Embedding-0.6B', 'Nova',
        )

        assert chunker is not None
        assert chunker.max_input_tokens == 32768, chunker.max_input_tokens
        assert chunker.prefix == []
        assert chunker.suffix == [FAKE_EOS]
        # 32768 window - 1 suffix token - 1 safety margin.
        assert chunker.budget == 32766, chunker.budget

    def test_token_path_wraps_every_chunk_and_stays_within_budget(self):
        self.use_tokenizer(suffix=[FAKE_EOS])
        embedding = self.qwen_embedding(max_input_tokens=4)

        assert embedding.token_chunker is not None
        assert embedding.token_chunker.budget == 2

        embedding.embed_documents(['abcdefg'])

        sent = embedding.seen_documents
        assert len(sent) == 4, sent
        for chunk in sent:
            assert isinstance(chunk, list), chunk
            assert all(isinstance(token, int) for token in chunk), chunk
            # Strictly under the window, so an off-by-one in the server's length check
            # cannot reject the longest chunks.
            assert len(chunk) < 4, chunk
            # Every chunk carries the affix. Slicing a wrapped encoding would mispool
            # all but the last one.
            assert chunk[-1] == FAKE_EOS, chunk
        assert sent == [
            [ord('a'), ord('b'), FAKE_EOS],
            [ord('c'), ord('d'), FAKE_EOS],
            [ord('e'), ord('f'), FAKE_EOS],
            [ord('g'), FAKE_EOS],
        ], sent

    def test_token_path_weights_reduction_by_content_tokens_only(self):
        self.use_tokenizer(prefix=[FAKE_BOS], suffix=[FAKE_EOS])
        embedding = self.qwen_embedding(max_input_tokens=6)

        assert embedding.token_chunker.budget == 3

        out = embedding.embed_documents(['aaab'])

        # Content weights are 3 and 1. Counting the two affix tokens too would make
        # them 5 and 3, pulling the result toward the shorter chunk.
        assert len(out) == 1, out
        assert math.isclose(out[0][0], 3.0 / math.sqrt(10.0)), out[0]
        assert math.isclose(out[0][1], 1.0 / math.sqrt(10.0)), out[0]

    def test_affix_derivation_covers_prefix_suffix_and_neither(self):
        derive = self.embeddings._derive_special_affixes

        assert derive(FakeTokenizer(suffix=[FAKE_EOS])) == ([], [FAKE_EOS])
        assert derive(FakeTokenizer(prefix=[FAKE_BOS])) == ([FAKE_BOS], [])
        assert derive(
            FakeTokenizer(prefix=[FAKE_BOS], suffix=[FAKE_EOS]),
        ) == ([FAKE_BOS], [FAKE_EOS])
        # A tokenizer that adds nothing is a real match, not a failure.
        assert derive(FakeTokenizer()) == ([], [])

    def test_affix_derivation_refuses_unrecognizable_tokenizers(self):
        # Empty affixes here would send token IDs with no special tokens, the exact
        # mispooling this derivation prevents.
        derive = self.embeddings._derive_special_affixes

        with self.assertRaises(ValueError):
            derive(RewritingTokenizer())
        with self.assertRaises(ValueError):
            derive(EmptyTokenizer())

    def test_unrecognizable_tokenizer_falls_back_and_warns(self):
        self.embeddings._load_tokenizer.cache_clear()
        FakeAutoTokenizer.tokenizer = RewritingTokenizer()

        with self.assertWarns(
            self.embeddings.TokenizationFallbackWarning,
        ) as caught:
            embedding = self.qwen_embedding()

        assert 'could not be prepared' in str(caught.warning)
        assert embedding.token_chunker is None

    def test_window_too_small_for_affixes_falls_back_and_warns(self):
        # Clamping the budget instead would quietly emit chunks larger than the window.
        self.use_tokenizer(prefix=[FAKE_BOS], suffix=[FAKE_EOS])

        with self.assertWarns(
            self.embeddings.TokenizationFallbackWarning,
        ) as caught:
            embedding = self.qwen_embedding(max_input_tokens=3)

        assert 'leaves no room for content' in str(caught.warning)
        assert embedding.token_chunker is None

    def test_token_ids_refused_on_platforms_outside_the_allowlist(self):
        self.use_tokenizer(suffix=[FAKE_EOS])

        # Bedrock decodes integer inputs with tiktoken, so model-native IDs would
        # quietly become unrelated text and get embedded.
        with self.assertWarns(
            self.embeddings.TokenizationFallbackWarning,
        ) as caught:
            refused = self.embeddings._token_chunker_for(
                'Qwen/Qwen3-Embedding-0.6B', 'Amazon',
            )

        assert refused is None
        assert 'not known to accept token IDs' in str(caught.warning)
        assert self.embeddings._token_chunker_for(
            'Qwen/Qwen3-Embedding-0.6B', 'Nova',
        ) is not None

    def test_tokenizer_load_failure_falls_back_and_warns(self):
        self.fail_tokenizer_load(RuntimeError('huggingface.co unreachable'))

        with self.assertWarns(
            self.embeddings.TokenizationFallbackWarning,
        ) as caught:
            embedding = self.qwen_embedding()

        assert 'huggingface.co unreachable' in str(caught.warning)
        assert embedding.token_chunker is None
        assert embedding.max_chunk_chars == 24000

        embedding.embed_documents(['a' * 24001])
        assert all(isinstance(x, str) for x in embedding.seen_documents)
        assert [len(x) for x in embedding.seen_documents] == [24000, 1]

    def test_max_input_tokens_override_beats_registry_and_info(self):
        self.use_tokenizer(suffix=[FAKE_EOS])
        chunker_for = self.embeddings._token_chunker_for
        info = types.SimpleNamespace(max_input_tokens=1024)

        assert chunker_for(
            'Qwen/Qwen3-Embedding-0.6B', 'Nova', max_input_tokens=512,
        ).max_input_tokens == 512
        assert chunker_for(
            'Qwen/Qwen3-Embedding-0.6B', 'Nova', info=info,
        ).max_input_tokens == 1024
        assert chunker_for(
            'Qwen/Qwen3-Embedding-0.6B', 'Nova', info=info, max_input_tokens=256,
        ).max_input_tokens == 256


if __name__ == '__main__':
    unittest.main()
