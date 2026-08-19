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

# Arbitrary IDs outside the fake tokenizers' character-derived range, standing in for
# a model's BOS/EOS. The real Qwen3 EOS (151643) is deliberately not used here: the
# affixes are derived from the tokenizer, so no test should know a real special ID.
FAKE_BOS = 900001
FAKE_EOS = 900002

LIVE_MODEL_ENV = 'SINGLESTOREDB_EMBEDDINGS_LIVE_MODEL'

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
        # The live check below, and anything else importing these for real, must not
        # inherit the fakes.
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
        # A model with no registry entry keeps the pre-tokenization behavior: text on
        # the wire, split on characters.
        embedding = self.embeddings.SingleStoreEmbeddingsFactory(
            model_name='shared-qwen3-embed-0-6b',
            api_key='token',
            base_url='http://localhost:8000',
            hosting_platform='NovaMultiTenant',
        )

        assert isinstance(embedding, self.embeddings._ChunkedOpenAIEmbeddings)
        assert embedding.kwargs['check_embedding_ctx_length'] is False
        assert embedding.token_chunker is None
        assert embedding.max_chunk_chars == 6000, embedding.max_chunk_chars

        embedding.embed_documents(['a' * 6001])
        assert all(isinstance(x, str) for x in embedding.seen_documents)
        assert [len(x) for x in embedding.seen_documents] == [6000, 1]

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
        assert chunker.budget == 32767, chunker.budget

    def test_token_path_wraps_every_chunk_and_stays_within_budget(self):
        self.use_tokenizer(suffix=[FAKE_EOS])
        embedding = self.qwen_embedding(max_input_tokens=4)

        assert embedding.token_chunker is not None
        assert embedding.token_chunker.budget == 3

        embedding.embed_documents(['abcdefg'])

        sent = embedding.seen_documents
        assert len(sent) == 3, sent
        for chunk in sent:
            assert isinstance(chunk, list), chunk
            assert all(isinstance(token, int) for token in chunk), chunk
            assert len(chunk) <= 4, chunk
            # Every chunk carries the affix, not just the last one. Slicing a wrapped
            # encoding instead would mispool every chunk but the final one.
            assert chunk[-1] == FAKE_EOS, chunk
        assert sent == [
            [ord('a'), ord('b'), ord('c'), FAKE_EOS],
            [ord('d'), ord('e'), ord('f'), FAKE_EOS],
            [ord('g'), FAKE_EOS],
        ], sent

    def test_token_path_weights_reduction_by_content_tokens_only(self):
        self.use_tokenizer(prefix=[FAKE_BOS], suffix=[FAKE_EOS])
        embedding = self.qwen_embedding(max_input_tokens=5)

        assert embedding.token_chunker.budget == 3

        out = embedding.embed_documents(['aaab'])

        # Chunks weigh 3 and 1 content tokens; counting the two affix tokens as well
        # would weigh them 5 and 3 and pull the result toward the shorter chunk.
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
        assert derive(FakeTokenizer()) == ([], [])

    def test_token_ids_refused_on_platforms_outside_the_allowlist(self):
        self.use_tokenizer(suffix=[FAKE_EOS])

        # The Bedrock route decodes integer inputs with tiktoken, so model-native IDs
        # would be silently decoded into unrelated text and embedded.
        assert self.embeddings._token_chunker_for(
            'Qwen/Qwen3-Embedding-0.6B', 'Amazon',
        ) is None
        assert self.embeddings._token_chunker_for(
            'Qwen/Qwen3-Embedding-0.6B', 'Nova',
        ) is not None

    def test_tokenizer_load_failure_falls_back_and_warns(self):
        self.fail_tokenizer_load(RuntimeError('huggingface.co unreachable'))

        with self.assertWarns(UserWarning) as caught:
            embedding = self.qwen_embedding()

        assert 'huggingface.co unreachable' in str(caught.warning)
        assert embedding.token_chunker is None
        assert embedding.max_chunk_chars == 6000

        embedding.embed_documents(['a' * 6001])
        assert all(isinstance(x, str) for x in embedding.seen_documents)
        assert [len(x) for x in embedding.seen_documents] == [6000, 1]

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


@unittest.skipUnless(
    os.environ.get(LIVE_MODEL_ENV),
    f'set {LIVE_MODEL_ENV} to a deployed embedding model name to run the live '
    'server-contract check',
)
class TestLiveTokenIdParity(unittest.TestCase):
    """Server-contract tripwire against a real deployment; never runs in CI.

    Now that the client owns tokenization, this is what detects a vLLM upgrade, an
    ``--hf-overrides`` change, or a model revision that alters tokenization or
    pooling. Needs ``SINGLESTOREDB_USER_TOKEN`` plus either an org context or
    ``SINGLESTOREDB_INFERENCE_API_BASE_URL`` and
    ``SINGLESTOREDB_INFERENCE_API_HOSTING_PLATFORM``.
    """

    text = (
        'SingleStore is a distributed SQL database that supports both '
        'transactional and analytical workloads over the same data, with '
        'vector search built in.'
    )

    @staticmethod
    def cosine(left, right):
        dot = sum(a * b for a, b in zip(left, right))
        left_norm = sum(a * a for a in left) ** 0.5
        right_norm = sum(b * b for b in right) ** 0.5
        return dot / (left_norm * right_norm)

    def native_embedding(self):
        """An embeddings model on the token path, as the factory built it."""
        from singlestoredb.ai.embeddings import SingleStoreEmbeddingsFactory

        model_name = os.environ[LIVE_MODEL_ENV]
        embedding = SingleStoreEmbeddingsFactory(model_name=model_name)
        assert embedding.token_chunker is not None, (
            f'{model_name} did not take the token path; check its registry entry '
            'and that the tokenizer loaded'
        )
        return embedding

    def text_embedding(self):
        """An embeddings model that puts raw text on the wire, as the baseline."""
        embedding = self.native_embedding()
        embedding.token_chunker = None
        return embedding

    def retokenized_embedding(self, tokenizer, prefix, suffix):
        """An embeddings model that puts ``tokenizer``'s IDs on the wire."""
        import dataclasses

        embedding = self.native_embedding()
        embedding.token_chunker = dataclasses.replace(
            embedding.token_chunker,
            tokenizer=tokenizer,
            prefix=prefix,
            suffix=suffix,
        )
        return embedding

    def cosine_against_text(self, embedding):
        return self.cosine(
            self.text_embedding().embed_documents([self.text])[0],
            embedding.embed_documents([self.text])[0],
        )

    def test_native_token_ids_match_raw_text(self):
        cos = self.cosine_against_text(self.native_embedding())
        assert cos > 0.9999, cos

    def test_dropping_special_affixes_breaks_parity(self):
        # Guards the LAST-pooling assumption: without the trailing special token the
        # sentence vector becomes the hidden state of the last content token instead.
        unwrapped = self.retokenized_embedding(
            self.native_embedding().token_chunker.tokenizer, [], [],
        )

        cos = self.cosine_against_text(unwrapped)
        assert cos < 0.99, (
            f'dropping the special affixes still matched raw text (cos={cos}); the '
            'server-side tokenization or pooling contract has changed'
        )

    def test_tiktoken_ids_are_not_equivalent(self):
        import tiktoken

        encoding = tiktoken.get_encoding('cl100k_base')

        class TiktokenShim:
            def encode(self, text, add_special_tokens=True):
                return encoding.encode(text)

        cos = self.cosine_against_text(
            self.retokenized_embedding(TiktokenShim(), [], []),
        )
        assert cos < 0.9, (
            f'tiktoken IDs matched raw text (cos={cos}); the server is no longer '
            'interpreting the input as model-native token IDs'
        )


if __name__ == '__main__':
    unittest.main()
