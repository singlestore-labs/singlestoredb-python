"""LangChain embeddings models for SingleStore-hosted inference APIs.

Nova-hosted models are not OpenAI models, so tiktoken is the wrong tokenizer for them.
For models in the registry below, this module encodes with the model's own tokenizer
and sends token IDs. For everything else it sends raw text in character-sized chunks
and lets the server tokenize.
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

# Throwaway text used to compare a bare encode against a wrapped one.
_AFFIX_PROBE = 'x'

# Tokens held back so a full chunk stays strictly under the context window. Without it
# a full chunk is exactly max_input_tokens long, so an off-by-one in the server's
# length check would reject only the longest inputs.
_WINDOW_SAFETY_MARGIN = 1


class TokenizationFallbackWarning(UserWarning):
    """A model could not use client-side tokenization.

    Embeddings stay correct, since the server tokenizes the raw text itself, but long
    inputs are split on a coarse character budget instead of the real context window.
    Every fallback warns, because a silent one looks exactly like success. Silence with
    ``warnings.filterwarnings('ignore', category=TokenizationFallbackWarning)``.
    """


def _warn_fallback(model_name: str, reason: str) -> None:
    warnings.warn(
        f'Using character-based chunking with raw text for model {model_name!r} '
        f'because {reason}. Embeddings remain correct, but long inputs are split on a '
        f'coarse character budget rather than on the model context window.',
        TokenizationFallbackWarning,
        stacklevel=3,
    )


@dataclass(frozen=True)
class _ModelPolicy:
    """Tokenization policy for one model."""

    max_input_tokens: int
    send_token_ids: bool = False
    tokenizer_name: Optional[str] = None
    """HuggingFace repo to load the tokenizer from. None means use the model name."""

    token_id_platforms: FrozenSet[str] = _DEFAULT_TOKEN_ID_PLATFORMS
    """Platforms whose route accepts token IDs for this model.

    Default-deny, so a platform added later does not inherit the token path untested.
    'Amazon' and 'Azure' return earlier in the factory and never reach this check.

    'NovaMultiTenant' is verified by a live parity run (Qwen3-Embedding-0.6B). 'Nova'
    is inferred from serving the same image: tenancy changes routing and auth, not the
    tokenizer inside the container.
    """


# Keyed on a lowercased HuggingFace repo id, which is what InferenceAPIInfo.model_name
# resolves to when the factory looks the model up through the management API.
#
# Opt-in only: no prefix or wildcard matching, even between models that share a
# tokenizer. Wrong token IDs do not raise -- they return a well-formed, unit-norm
# vector -- so no model reaches the token path unverified. To add one:
#
#   1. Confirm the route accepts token IDs. vLLM does; Bedrock decodes them with
#      tiktoken instead, turning them into unrelated text.
#   2. Confirm the window actually served, including any --max-model-len override at
#      launch, not the one on the model card. Record the full window; the affixes and
#      _WINDOW_SAFETY_MARGIN are subtracted from it.
#   3. Run tests/test_embeddings_live.py against a real deployment (set
#      SINGLESTOREDB_EMBEDDINGS_LIVE_MODEL) and require cosine > 0.9999.
#   4. Add a unit test for the resolved budget and affixes.
_MODEL_POLICIES: Dict[str, _ModelPolicy] = {
    'qwen/qwen3-embedding-0.6b': _ModelPolicy(
        max_input_tokens=32768,
        send_token_ids=True,
    ),
}


@lru_cache(maxsize=None)
def _load_tokenizer(tokenizer_name: str) -> Any:
    """Load a HuggingFace tokenizer, once per name per process.

    Memoized because parsing Qwen3's ~11 MB tokenizer.json is slow, and the serving
    image bakes in no tokenizer cache, so the first load fetches from huggingface.co.
    """
    from transformers import AutoTokenizer  # type: ignore[import-not-found]
    return AutoTokenizer.from_pretrained(tokenizer_name)


def _derive_special_affixes(tokenizer: Any) -> Tuple[List[int], List[int]]:
    """Return the token IDs a tokenizer adds before and after content.

    Measured by diffing a bare encode against a wrapped one instead of hardcoded, so
    it covers BOS-style models too and follows any change in the model revision or in
    ``transformers``.

    Raises:
        ValueError: if the tokenizer does not wrap content in a fixed prefix and
            suffix. Assuming no affixes here would cause the exact mispooling this
            function prevents, so the caller must fall back instead.
    """
    bare = list(tokenizer.encode(_AFFIX_PROBE, add_special_tokens=False))
    wrapped = list(tokenizer.encode(_AFFIX_PROBE, add_special_tokens=True))
    if not bare:
        raise ValueError(
            f'tokenizer encoded the probe {_AFFIX_PROBE!r} to no tokens, so its '
            'special-token affixes cannot be derived',
        )
    for start in range(len(wrapped) - len(bare) + 1):
        if wrapped[start:start + len(bare)] == bare:
            return wrapped[:start], wrapped[start + len(bare):]
    raise ValueError(
        f'could not locate the bare probe encoding {bare} inside its wrapped form '
        f'{wrapped}, so this tokenizer does not simply surround content with a fixed '
        'prefix and suffix; per-chunk special tokens cannot be reproduced safely',
    )


@dataclass(frozen=True)
class _TokenChunker:
    """Splits text into token ID chunks that fit the model's context window."""

    tokenizer: Any
    max_input_tokens: int
    prefix: List[int]
    suffix: List[int]

    def __post_init__(self) -> None:
        if self.budget < 1:
            raise ValueError(
                f'max_input_tokens={self.max_input_tokens} leaves no room for content '
                f'after {len(self.prefix) + len(self.suffix)} special token(s) and a '
                f'{_WINDOW_SAFETY_MARGIN}-token safety margin',
            )

    @property
    def budget(self) -> int:
        """Content tokens per chunk, after the affixes and the safety margin.

        Not clamped on purpose: a window too small for the affixes means the caller or
        the registry is wrong, and clamping would emit chunks larger than the window
        it was asked to respect.
        """
        return (
            self.max_input_tokens
            - len(self.prefix)
            - len(self.suffix)
            - _WINDOW_SAFETY_MARGIN
        )

    def chunks(self, text: str) -> List[List[int]]:
        """Encode ``text`` into wrapped, in-budget token ID chunks."""
        content = list(self.tokenizer.encode(text, add_special_tokens=False))
        budget = self.budget
        # Wrap each chunk on its own. Slicing an already-wrapped encoding would leave
        # the suffix on the last chunk only, so every earlier chunk pools at the wrong
        # position.
        return [
            self.prefix + content[i:i + budget] + self.suffix
            for i in range(0, max(len(content), 1), budget)
        ]


def _resolve_max_input_tokens(
    policy: _ModelPolicy,
    info: Any,
    override: Optional[int],
) -> int:
    """Resolve the context window: caller override first, then server, then policy.

    ``info`` is read defensively so the registry constant gives way automatically if
    the inference API ever starts reporting the window, with no new SDK release.
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

    Returns None if the model is not registered, if its route does not accept token
    IDs, or if the tokenizer cannot be loaded or inspected. All three warn, since none
    of them is visible in the embeddings themselves.
    """
    key = model_name.strip().lower()
    policy = _MODEL_POLICIES.get(key)
    if policy is None or not policy.send_token_ids:
        _warn_fallback(
            model_name,
            f'no tokenization policy is registered under {key!r}. If this is a '
            'deployment alias rather than a HuggingFace repo id, the registry cannot '
            'match it',
        )
        return None
    if hosting_platform not in policy.token_id_platforms:
        _warn_fallback(
            model_name,
            f'hosting platform {hosting_platform!r} is not known to accept token IDs '
            f'for this model (allowed: {sorted(policy.token_id_platforms)})',
        )
        return None

    tokenizer_name = policy.tokenizer_name or model_name
    try:
        tokenizer = _load_tokenizer(tokenizer_name)
        prefix, suffix = _derive_special_affixes(tokenizer)
        return _TokenChunker(
            tokenizer=tokenizer,
            max_input_tokens=_resolve_max_input_tokens(policy, info, max_input_tokens),
            prefix=prefix,
            suffix=suffix,
        )
    except Exception as exc:
        # transformers missing, blocked egress, hub outage, renamed repo, unreadable
        # special tokens, unusable window: all degrade to character chunking with text
        # on the wire, which is correct but coarser.
        _warn_fallback(
            model_name,
            f'tokenizer {tokenizer_name!r} could not be prepared '
            f'({type(exc).__name__}: {exc})',
        )
        return None


_Chunk = Union[str, List[int]]


class _ChunkedOpenAIEmbeddings(OpenAIEmbeddings):
    """OpenAIEmbeddings for non-OpenAI models behind an OpenAI-compatible endpoint.

    tiktoken is the wrong tokenizer for these models (e.g. Qwen on the 'Nova'
    platforms), so ``check_embedding_ctx_length`` should be False to stop langchain
    encoding with it. That also turns off langchain's own long-input handling, so this
    class always chunks inputs itself, embeds each chunk, and weighted-averages them
    back into one vector per input. Otherwise the server rejects, or silently
    truncates, anything over its context window.

    With a ``token_chunker`` it chunks by real tokens and sends token IDs. Without one
    it splits on characters and sends text for the server to tokenize.
    """

    max_chunk_chars: int = 24000
    """Characters per chunk when ``token_chunker`` is None.

    A coarse stand-in for a token count, since the client has no tokenizer here.
    Override per model if the deployment's context window is known.
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
        """How much this chunk counts for in the average, in units of content."""
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
        """Chunk every input, tracking which input each chunk came from."""
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
        # langchain passes batch elements through untouched when
        # check_embedding_ctx_length is False, so token ID lists reach the wire as-is
        # despite the str signature.
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
        # Real OpenAI models: tiktoken is the right tokenizer, and the model name passed
        # above picks the right encoding. Keep langchain's own tokenization and
        # long-input chunking, which are both correct here.
        kwargs.setdefault('check_embedding_ctx_length', True)
        return OpenAIEmbeddings(
            **openai_kwargs,
            **kwargs,
        )

    # Non-OpenAI models (e.g. Qwen on 'Nova'): tiktoken would send OpenAI token IDs the
    # model cannot read, giving meaningless embeddings. So either encode with the
    # model's own tokenizer, or send raw text for the server to tokenize. Either way we
    # chunk long inputs here, since the server rejects or silently truncates anything
    # over its window.
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
