"""LangChain embeddings models for SingleStore-hosted inference APIs.

Models served on the 'Nova' platforms are not OpenAI models, so tiktoken is the wrong
tokenizer for them. Where this module knows a model's real tokenizer it chunks and
encodes client-side and puts model-native token IDs on the wire; otherwise it falls
back to sending raw text in character-bounded chunks.
"""
import os
import warnings
from dataclasses import dataclass
from functools import lru_cache
from typing import Any
from typing import Callable
from typing import Dict
from typing import FrozenSet
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

import httpx

from singlestoredb import manage_workspaces
from singlestoredb.management.inference_api import InferenceAPIInfo

try:
    from langchain_openai import OpenAIEmbeddings
except ImportError:
    raise ImportError(
        'Could not import langchain_openai python package. '
        'Please install it with `pip install langchain_openai`.',
    )

try:
    from langchain_aws import BedrockEmbeddings
except ImportError:
    raise ImportError(
        'Could not import langchain-aws python package. '
        'Please install it with `pip install langchain-aws`.',
    )

import boto3
from botocore import UNSIGNED
from botocore.config import Config

_DEFAULT_TOKEN_ID_PLATFORMS = frozenset({'Nova', 'NovaMultiTenant'})


@dataclass(frozen=True)
class _ModelPolicy:
    """Tokenization policy for one model."""

    max_input_tokens: int
    send_token_ids: bool = False
    tokenizer_name: Optional[str] = None
    """HuggingFace repo to load the tokenizer from. None means use the model name."""

    token_id_platforms: FrozenSet[str] = _DEFAULT_TOKEN_ID_PLATFORMS
    """Hosting platforms whose route accepts token IDs for this model.

    This is per-model-and-route rather than global: the 'Amazon' route decodes integer
    ``input`` arrays with tiktoken, so model-native IDs sent that way are decoded into
    unrelated text and embedded without error.
    """


# Keyed on a lowercased HuggingFace repo id, which is what InferenceAPIInfo.model_name
# resolves to, so entries survive deployment alias renames.
#
# Entries are explicit opt-in: there is no family-prefix or wildcard matching, even
# for models known to share a tokenizer. A model that reaches the token path without a
# parity run is the failure mode this registry exists to prevent -- mismatched special
# tokens return a well-formed, unit-norm, plausible-looking vector and raise nothing.
# To add a model:
#
#   1. Confirm the serving stack accepts token IDs on that route. vLLM types the
#      embeddings input as list[int] | list[list[int]] | str | list[str]; Bedrock
#      does not.
#   2. Confirm the context window actually served, including any --max-model-len
#      override at launch, rather than the window the model card advertises.
#   3. Run test_live_token_id_parity from singlestoredb/tests/test_embeddings.py
#      against a real deployment and require cosine > 0.9999.
#   4. Add the entry plus a unit test asserting its resolved budget and affixes.
_MODEL_POLICIES: Dict[str, _ModelPolicy] = {
    'qwen/qwen3-embedding-0.6b': _ModelPolicy(
        max_input_tokens=32768,
        send_token_ids=True,
    ),
}


@lru_cache(maxsize=None)
def _load_tokenizer(tokenizer_name: str) -> Any:
    """Load and memoize a HuggingFace tokenizer.

    Memoized because parsing Qwen3's ~11 MB tokenizer.json is slow, and because there
    is no baked tokenizer cache in the serving image, so the first load in a container
    fetches from huggingface.co over the network.
    """
    from transformers import AutoTokenizer  # type: ignore[import-not-found]
    return AutoTokenizer.from_pretrained(tokenizer_name)


def _derive_special_affixes(tokenizer: Any) -> Tuple[List[int], List[int]]:
    """Return the token IDs a tokenizer adds before and after content.

    Derived by diffing a probe encode rather than hardcoded, so this holds for
    BOS-style models too and self-corrects if a model revision or a ``transformers``
    upgrade changes special-token handling.
    """
    bare = list(tokenizer.encode('x', add_special_tokens=False))
    wrapped = list(tokenizer.encode('x', add_special_tokens=True))
    if not bare:
        return [], []
    for start in range(len(wrapped) - len(bare) + 1):
        if wrapped[start:start + len(bare)] == bare:
            return wrapped[:start], wrapped[start + len(bare):]
    return [], []


@dataclass(frozen=True)
class _TokenChunker:
    """Splits text into model-native token ID chunks that fit the context window."""

    tokenizer: Any
    max_input_tokens: int
    prefix: List[int]
    suffix: List[int]

    @property
    def budget(self) -> int:
        """Content tokens allowed per chunk, after reserving room for the affixes."""
        return max(1, self.max_input_tokens - len(self.prefix) - len(self.suffix))

    def chunks(self, text: str) -> List[List[int]]:
        """Encode ``text`` into wrapped, in-budget token ID chunks."""
        content = list(self.tokenizer.encode(text, add_special_tokens=False))
        budget = self.budget
        # Every chunk is wrapped individually. Slicing an already-wrapped encoding
        # would put the special suffix on the last chunk only, leaving every earlier
        # chunk pooled at the wrong position.
        return [
            self.prefix + content[i:i + budget] + self.suffix
            for i in range(0, max(len(content), 1), budget)
        ]


def _resolve_max_input_tokens(
    policy: _ModelPolicy,
    info: Any,
    override: Optional[int],
) -> int:
    """Resolve the context window, preferring caller and server values over the policy.

    ``max_input_tokens`` is read off ``info`` defensively so that the registry constant
    is superseded automatically if the inference API ever starts reporting the window,
    without needing another SDK release.
    """
    if override is not None:
        return int(override)
    from_info = getattr(info, 'max_input_tokens', None)
    if from_info:
        return int(from_info)
    return policy.max_input_tokens


def _token_chunker_for(
    model_name: str,
    hosting_platform: Optional[str],
    info: Any = None,
    max_input_tokens: Optional[int] = None,
) -> Optional[_TokenChunker]:
    """Build a token chunker for a model, or None to keep character chunking.

    Returns None when the model is not in the registry, when its route does not accept
    token IDs, or when the tokenizer cannot be loaded.
    """
    policy = _MODEL_POLICIES.get(model_name.strip().lower())
    if policy is None or not policy.send_token_ids:
        return None
    if hosting_platform not in policy.token_id_platforms:
        return None

    tokenizer_name = policy.tokenizer_name or model_name
    try:
        tokenizer = _load_tokenizer(tokenizer_name)
    except Exception as exc:
        # Any failure -- transformers missing, blocked egress, hub outage, renamed
        # repo -- degrades to character chunking with text on the wire. That is
        # correct, just coarser. Warn so the degradation is not silent: an egress
        # change switching this feature off invisibly is the failure class this
        # tokenization work exists to eliminate.
        warnings.warn(
            f'Could not load tokenizer {tokenizer_name!r} for model '
            f'{model_name!r} ({type(exc).__name__}: {exc}). Falling back to '
            'character-based chunking with raw text; long inputs may be chunked '
            'less precisely.',
        )
        return None

    prefix, suffix = _derive_special_affixes(tokenizer)
    return _TokenChunker(
        tokenizer=tokenizer,
        max_input_tokens=_resolve_max_input_tokens(policy, info, max_input_tokens),
        prefix=prefix,
        suffix=suffix,
    )


_Chunk = Union[str, List[int]]


class _ChunkedOpenAIEmbeddings(OpenAIEmbeddings):
    """OpenAIEmbeddings for non-OpenAI models behind an OpenAI-compatible endpoint.

    tiktoken is the wrong tokenizer for these models (e.g. Qwen served on the 'Nova'
    platforms), so ``check_embedding_ctx_length`` should be False to keep langchain
    from encoding with it. Because the server rejects (or silently truncates) inputs
    longer than its context window, this class splits long inputs into chunks itself,
    embeds each chunk, and weighted-averages them back into a single vector per input
    -- irrespective of the flag -- so long texts never hit the server's hard limit.

    With a ``token_chunker`` set, chunking uses the model's own tokenizer and sends
    token IDs. Without one, it falls back to a coarse character split and sends text
    for the server to tokenize.
    """

    max_chunk_chars: int = 6000
    """Maximum characters per chunk, used only when ``token_chunker`` is None.

    Coarse character-based guard used because the client does not have the model's
    tokenizer. Sized to stay under common ~8k-token Nova embedding windows even when
    characters map roughly 1:1 to tokens (code / CJK). Models with larger windows
    (e.g. Qwen3 Embedding ~32k) can raise this; models with smaller windows (e.g.
    ~4k) should lower it.
    """

    token_chunker: Optional[Any] = None
    """A :class:`_TokenChunker`, or None to chunk by characters and send raw text."""

    def _chunks(self, text: str) -> List[_Chunk]:
        if self.token_chunker is not None:
            return list(self.token_chunker.chunks(text))
        n = max(1, self.max_chunk_chars)
        if len(text) <= n:
            return [text]
        return [text[i:i + n] for i in range(0, len(text), n)]

    def _weight(self, chunk: _Chunk) -> int:
        """Weight of a chunk in the reduction, in units of content."""
        if self.token_chunker is None:
            return max(1, len(chunk))
        affix_len = len(self.token_chunker.prefix) + len(self.token_chunker.suffix)
        return max(1, len(chunk) - affix_len)

    @staticmethod
    def _average(vectors: List[List[float]], weights: List[int]) -> List[float]:
        total = float(sum(weights)) or 1.0
        dim = len(vectors[0])
        avg = [0.0] * dim
        for vec, w in zip(vectors, weights):
            for k in range(dim):
                avg[k] += vec[k] * w
        avg = [x / total for x in avg]
        norm = sum(x * x for x in avg) ** 0.5
        if norm > 0:
            avg = [x / norm for x in avg]
        return avg

    def _plan(
        self, texts: List[str],
    ) -> Tuple[List[_Chunk], List[int], List[int]]:
        """Split every input into chunks, tracking which input each chunk came from."""
        flat: List[_Chunk] = []
        owner: List[int] = []
        weights: List[int] = []
        for i, text in enumerate(texts):
            for chunk in self._chunks(text):
                flat.append(chunk)
                owner.append(i)
                weights.append(self._weight(chunk))
        return flat, owner, weights

    def _reduce(
        self,
        num_texts: int,
        owner: List[int],
        weights: List[int],
        embeddings: List[List[float]],
    ) -> List[List[float]]:
        out: List[List[float]] = []
        for i in range(num_texts):
            idxs = [j for j, o in enumerate(owner) if o == i]
            if len(idxs) == 1:
                out.append(embeddings[idxs[0]])
            else:
                out.append(
                    self._average(
                        [embeddings[j] for j in idxs],
                        [weights[j] for j in idxs],
                    ),
                )
        return out

    def embed_documents(
        self, texts: List[str], chunk_size: Optional[int] = None, **kwargs: Any,
    ) -> List[List[float]]:
        flat, owner, weights = self._plan(texts)
        # langchain forwards batch elements untouched when check_embedding_ctx_length
        # is False, so token ID lists reach the wire as-is despite the str signature.
        embeddings = super().embed_documents(
            flat, chunk_size=chunk_size, **kwargs,  # type: ignore[arg-type]
        )
        return self._reduce(len(texts), owner, weights, embeddings)

    async def aembed_documents(
        self, texts: List[str], chunk_size: Optional[int] = None, **kwargs: Any,
    ) -> List[List[float]]:
        flat, owner, weights = self._plan(texts)
        embeddings = await super().aembed_documents(
            flat, chunk_size=chunk_size, **kwargs,  # type: ignore[arg-type]
        )
        return self._reduce(len(texts), owner, weights, embeddings)


def SingleStoreEmbeddingsFactory(
    model_name: str,
    api_key: Optional[str] = None,
    http_client: Optional[httpx.Client] = None,
    obo_token_getter: Optional[Callable[[], Optional[str]]] = None,
    base_url: Optional[str] = None,
    hosting_platform: Optional[str] = None,
    max_input_tokens: Optional[int] = None,
    **kwargs: Any,
) -> Union[OpenAIEmbeddings, BedrockEmbeddings]:
    """Return an embeddings model instance (OpenAIEmbeddings or BedrockEmbeddings).
    """
    # handle model info
    if base_url is None:
        base_url = os.environ.get('SINGLESTOREDB_INFERENCE_API_BASE_URL')
    if hosting_platform is None:
        hosting_platform = os.environ.get('SINGLESTOREDB_INFERENCE_API_HOSTING_PLATFORM')
    if base_url is None or hosting_platform is None:
        inference_api_manager = (
            manage_workspaces().organizations.current.inference_apis
        )
        info = inference_api_manager.get(model_name=model_name)
        if not info.internal_connection_url:
            info.internal_connection_url = info.connection_url
    else:
        info = InferenceAPIInfo(
            service_id='',
            model_name=model_name,
            name='',
            connection_url=base_url,
            internal_connection_url=base_url,
            project_id='',
            hosting_platform=hosting_platform,
        )
    if base_url is not None:
        info.connection_url = base_url
        info.internal_connection_url = base_url
    if hosting_platform is not None:
        info.hosting_platform = hosting_platform

    # Extract timeouts from http_client if provided
    t = http_client.timeout if http_client is not None else None
    connect_timeout = None
    read_timeout = None
    if t is not None:
        if isinstance(t, httpx.Timeout):
            if t.connect is not None:
                connect_timeout = float(t.connect)
            if t.read is not None:
                read_timeout = float(t.read)
            if connect_timeout is None and read_timeout is not None:
                connect_timeout = read_timeout
            if read_timeout is None and connect_timeout is not None:
                read_timeout = connect_timeout
        elif isinstance(t, (int, float)):
            connect_timeout = float(t)
            read_timeout = float(t)

    if info.hosting_platform == 'Amazon':
        # Instantiate Bedrock client
        cfg_kwargs = {
            'signature_version': UNSIGNED,
            'retries': {'max_attempts': 1, 'mode': 'standard'},
        }
        if read_timeout is not None:
            cfg_kwargs['read_timeout'] = read_timeout
        if connect_timeout is not None:
            cfg_kwargs['connect_timeout'] = connect_timeout

        cfg = Config(**cfg_kwargs)
        client = boto3.client(
            'bedrock-runtime',
            endpoint_url=info.internal_connection_url,
            region_name='us-east-1',
            aws_access_key_id='placeholder',
            aws_secret_access_key='placeholder',
            config=cfg,
        )

        def _inject_headers(request: Any, **_ignored: Any) -> None:
            """Inject dynamic auth/OBO headers prior to Bedrock sending."""
            token_env_val = os.environ.get('SINGLESTOREDB_USER_TOKEN')
            token_val = api_key if api_key is not None else token_env_val
            if token_val:
                request.headers['Authorization'] = f'Bearer {token_val}'
            if obo_token_getter is not None:
                obo_val = obo_token_getter()
                if obo_val:
                    request.headers['X-S2-OBO'] = obo_val
            request.headers.pop('X-Amz-Date', None)
            request.headers.pop('X-Amz-Security-Token', None)

        emitter = client._endpoint._event_emitter
        emitter.register_first(
            'before-send.bedrock-runtime.InvokeModel',
            _inject_headers,
        )
        emitter.register_first(
            'before-send.bedrock-runtime.InvokeModelWithResponseStream',
            _inject_headers,
        )

        return BedrockEmbeddings(
            model_id=model_name,
            endpoint_url=info.internal_connection_url,
            region_name='us-east-1',
            aws_access_key_id='placeholder',
            aws_secret_access_key='placeholder',
            client=client,
            **kwargs,
        )

    # OpenAI / Azure OpenAI path
    token_env = os.environ.get('SINGLESTOREDB_USER_TOKEN')
    token = api_key if api_key is not None else token_env

    openai_kwargs = dict(
        base_url=info.internal_connection_url,
        api_key=token,
        model=model_name,
    )
    if http_client is not None:
        openai_kwargs['http_client'] = http_client

    if info.hosting_platform == 'Azure':
        # Genuine OpenAI (Azure) models: tiktoken is the correct tokenizer, and the
        # model name is passed above so it selects the right encoding. Keep langchain's
        # client-side tokenization + long-input chunking (all correct for these models).
        kwargs.setdefault('check_embedding_ctx_length', True)
        return OpenAIEmbeddings(
            **openai_kwargs,
            **kwargs,
        )

    # Non-OpenAI models (e.g. Qwen on 'Nova'): tiktoken would send OpenAI token IDs the
    # model can't interpret -> nonsensical embeddings. Either encode with the model's
    # own tokenizer client-side, or send raw text and let the server tokenize. Either
    # way chunk long inputs ourselves, since the server otherwise rejects or silently
    # truncates over-context input.
    kwargs.setdefault('check_embedding_ctx_length', False)
    token_chunker = _token_chunker_for(
        info.model_name,
        info.hosting_platform,
        info=info,
        max_input_tokens=max_input_tokens,
    )
    if token_chunker is not None:
        kwargs['token_chunker'] = token_chunker
    return _ChunkedOpenAIEmbeddings(
        **openai_kwargs,
        **kwargs,
    )
