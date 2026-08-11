import os
from typing import Any
from typing import Callable
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


class _ChunkedOpenAIEmbeddings(OpenAIEmbeddings):
    """OpenAIEmbeddings for non-OpenAI models behind an OpenAI-compatible endpoint.

    These models (e.g. Qwen served on the 'Nova' platform) tokenize server-side with
    their own tokenizer, so inputs are sent as raw text (``check_embedding_ctx_length``
    should be False). Because the server rejects (or silently truncates) inputs longer
    than its context window, this class splits long inputs into character-bounded chunks
    itself, embeds each chunk, and length-weighted-averages them back into a single
    vector per input -- irrespective of the flag -- so long texts never hit the server's
    hard limit.
    """

    max_chunk_chars: int = 24000
    """Maximum characters per chunk. This is a coarse character-based guard for
    models whose exact tokenizer/context metadata is not yet available to the client.
    Override per model if the deployment's context window is known to be smaller or
    larger."""

    def _chunks(self, text: str) -> List[str]:
        n = max(1, self.max_chunk_chars)
        if len(text) <= n:
            return [text]
        return [text[i:i + n] for i in range(0, len(text), n)]

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

    def _plan(self, texts: List[str]) -> Tuple[List[str], List[int]]:
        flat: List[str] = []
        owner: List[int] = []
        for i, text in enumerate(texts):
            for chunk in self._chunks(text):
                flat.append(chunk)
                owner.append(i)
        return flat, owner

    def _reduce(
        self,
        num_texts: int,
        owner: List[int],
        flat: List[str],
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
                        [max(1, len(flat[j])) for j in idxs],
                    ),
                )
        return out

    def embed_documents(
        self, texts: List[str], chunk_size: Optional[int] = None, **kwargs: Any,
    ) -> List[List[float]]:
        flat, owner = self._plan(texts)
        embeddings = super().embed_documents(flat, chunk_size=chunk_size, **kwargs)
        return self._reduce(len(texts), owner, flat, embeddings)

    async def aembed_documents(
        self, texts: List[str], chunk_size: Optional[int] = None, **kwargs: Any,
    ) -> List[List[float]]:
        flat, owner = self._plan(texts)
        embeddings = await super().aembed_documents(flat, chunk_size=chunk_size, **kwargs)
        return self._reduce(len(texts), owner, flat, embeddings)


def SingleStoreEmbeddingsFactory(
    model_name: str,
    api_key: Optional[str] = None,
    http_client: Optional[httpx.Client] = None,
    obo_token_getter: Optional[Callable[[], Optional[str]]] = None,
    base_url: Optional[str] = None,
    hosting_platform: Optional[str] = None,
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
    # model can't interpret -> nonsensical embeddings. Send raw text so the server
    # tokenizes with the model's own tokenizer, and chunk long inputs ourselves (the
    # server otherwise rejects or silently truncates over-context input).
    kwargs.setdefault('check_embedding_ctx_length', False)
    return _ChunkedOpenAIEmbeddings(
        **openai_kwargs,
        **kwargs,
    )
