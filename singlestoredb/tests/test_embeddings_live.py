#!/usr/bin/env python
# type: ignore
"""Live server-contract checks for singlestoredb.ai.embeddings.

Not unit tests. They need a real deployment, real credentials, and network access,
and a green run is a fact about one model on one platform at one moment, not an
invariant of this code. They exist because no offline test can verify what the
token path assumes: that the server reads our integers as model-native token IDs.
A fake tokenizer agrees with whatever we assert.

Kept out of test_embeddings.py on purpose. That module swaps fakes into sys.modules
for langchain_openai and transformers, so if its teardown were ever skipped these
would import the fakes and pass against a constant vector -- the one false green
this file cannot afford.

Marked ``management`` like the other tests that need real cloud credentials, so
CI's ``-m 'not management'`` excludes them by policy rather than by whether an env
var happens to be unset. The skipUnless on top of that is just so a local run
without a deployment says what to set.

Run against a live deployment, named by its deployment name not its HF id::

    SINGLESTOREDB_EMBEDDINGS_LIVE_MODEL=shared-qwen3-embed-0-6b
    SINGLESTOREDB_MANAGEMENT_TOKEN=...  # resolves that name to the HF id
    SINGLESTOREDB_PROJECT=...
    SINGLESTOREDB_USER_TOKEN=...        # authenticates the embeddings call
    SINGLESTOREDB_URL=...               # any value; skips the test container

Keep SINGLESTOREDB_INFERENCE_API_BASE_URL unset. With it the factory builds the
model info itself, so the registry key and the wire ``model`` collapse into one
string and no value satisfies both.
"""
import os
import unittest

import pytest

LIVE_MODEL_ENV = 'SINGLESTOREDB_EMBEDDINGS_LIVE_MODEL'


@pytest.mark.management
@unittest.skipUnless(
    os.environ.get(LIVE_MODEL_ENV),
    f'set {LIVE_MODEL_ENV} to a deployed embedding model name to run the live '
    'server-contract check',
)
class TestLiveTokenIdParity(unittest.TestCase):
    """Catches a vLLM upgrade, an ``--hf-overrides`` change, or a model revision
    that shifts tokenization or pooling."""

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
        # Guards the LAST-pooling assumption: with no trailing special token, the
        # sentence vector becomes the last content token's hidden state instead.
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

    def test_batched_inputs_match_single_inputs(self):
        # Several inputs in one call put a list of token-ID arrays on the wire. The
        # other tests send one input, so they only ever cover a one-element list.
        embedding = self.native_embedding()
        texts = [
            'Vector search over transactional data.',
            'Distributed SQL with both columnstore and rowstore tables.',
            'Client-side tokenization for Nova-hosted embedding models.',
        ]

        batched = embedding.embed_documents(texts)
        assert len(batched) == len(texts), len(batched)
        for text, got in zip(texts, batched):
            cos = self.cosine(embedding.embed_documents([text])[0], got)
            # Looser than the native-vs-text check: a batched vLLM forward pads
            # mixed-length sequences into one GEMM, so the vectors are not
            # bit-identical to three solo calls. Wrong token IDs would land far
            # below this, as in test_tiktoken_ids_are_not_equivalent.
            assert cos > 0.999, (text, cos)


if __name__ == '__main__':
    unittest.main()
