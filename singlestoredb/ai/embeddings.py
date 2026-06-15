import os
from typing import Any
from typing import Callable
from typing import Optional
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


def SingleStoreEmbeddingsFactory(
    model_name: str,
    api_key: Optional[str] = None,
    http_client: Optional[httpx.Client] = None,
    http_async_client: Optional[httpx.AsyncClient] = None,
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

    if http_async_client is None:
        # Explicit timeouts: without these, httpx falls back to its 5s
        # default at the client level, but the OpenAI SDK overrides that
        # with a per-request 600s read timeout, so a stalled response can
        # sit on the socket for ~10 minutes before httpx notices. We use a
        # tighter read timeout so a dead/half-open connection fails fast
        # instead of waiting for the application-level defensive timeout
        # (e.g. EMBED_TEXT's asyncio.wait_for) to fire.
        client_timeout = httpx.Timeout(
            connect=float(
                os.environ.get(
                    'SINGLESTOREDB_EMBEDDINGS_CONNECT_TIMEOUT', '10',
                ),
            ),
            read=float(
                os.environ.get(
                    'SINGLESTOREDB_EMBEDDINGS_READ_TIMEOUT', '60',
                ),
            ),
            write=float(
                os.environ.get(
                    'SINGLESTOREDB_EMBEDDINGS_WRITE_TIMEOUT', '30',
                ),
            ),
            pool=float(
                os.environ.get(
                    'SINGLESTOREDB_EMBEDDINGS_POOL_TIMEOUT', '10',
                ),
            ),
        )
        # Allow connection reuse. The previous configuration
        # (max_keepalive_connections=0) forced a fresh TCP+TLS handshake
        # for every request, which under heavy concurrency churns sockets
        # and occasionally yields one connection that the upstream accepts
        # but never finishes responding on.
        client_limits = httpx.Limits(
            max_connections=int(
                os.environ.get(
                    'SINGLESTOREDB_EMBEDDINGS_MAX_CONNECTIONS', '64',
                ),
            ),
            max_keepalive_connections=int(
                os.environ.get(
                    'SINGLESTOREDB_EMBEDDINGS_MAX_KEEPALIVE', '16',
                ),
            ),
            keepalive_expiry=float(
                os.environ.get(
                    'SINGLESTOREDB_EMBEDDINGS_KEEPALIVE_EXPIRY', '30',
                ),
            ),
        )
        http_async_client = httpx.AsyncClient(
            timeout=client_timeout,
            limits=client_limits,
        )
    openai_kwargs['http_async_client'] = http_async_client

    return OpenAIEmbeddings(
        **openai_kwargs,
        **kwargs,
    )
