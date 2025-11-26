import os
from collections.abc import Generator
from typing import Any
from typing import Callable
from typing import Optional
from typing import Union

import httpx

from singlestoredb import manage_workspaces
from singlestoredb.management.inference_api import InferenceAPIInfo

try:
    from langchain_openai import ChatOpenAI
except ImportError:
    raise ImportError(
        'Could not import langchain_openai python package. '
        'Please install it with `pip install langchain_openai`.',
    )

try:
    from langchain_aws import ChatBedrockConverse
except ImportError:
    raise ImportError(
        'Could not import langchain-aws python package. '
        'Please install it with `pip install langchain-aws`.',
    )

import boto3
from botocore import UNSIGNED
from botocore.config import Config


def SingleStoreChatFactory(
    model_name: str,
    api_key: Optional[Union[Optional[str], Callable[[], Optional[str]]]] = None,
    streaming: bool = True,
    http_client: Optional[httpx.Client] = None,
    obo_token: Optional[Union[Optional[str], Callable[[], Optional[str]]]] = None,
    base_url: Optional[str] = None,
    hosting_platform: Optional[str] = None,
    **kwargs: Any,
) -> Union[ChatOpenAI, ChatBedrockConverse]:
    """Return a chat model instance (ChatOpenAI or ChatBedrockConverse).
    """
    # Handle api_key and obo_token as callable functions
    if callable(api_key):
        api_key_getter = api_key
    else:
        def api_key_getter() -> Optional[str]:
            if api_key is None:
                return os.environ.get('SINGLESTOREDB_USER_TOKEN')
            return api_key

    if callable(obo_token):
        obo_token_getter = obo_token
    else:
        def obo_token_getter() -> Optional[str]:
            return obo_token

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
    else:
        info = InferenceAPIInfo(
            service_id='',
            model_name=model_name,
            name='',
            connection_url=base_url,
            project_id='',
            hosting_platform=hosting_platform,
        )
    if base_url is not None:
        info.connection_url = base_url
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
            endpoint_url=info.connection_url,
            region_name='us-east-1',
            aws_access_key_id='placeholder',
            aws_secret_access_key='placeholder',
            config=cfg,
        )

        def _inject_headers(request: Any, **_ignored: Any) -> None:
            """Inject dynamic auth/OBO headers prior to Bedrock sending."""
            if api_key_getter is not None:
                token_val = api_key_getter()
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
            'before-send.bedrock-runtime.Converse',
            _inject_headers,
        )
        emitter.register_first(
            'before-send.bedrock-runtime.ConverseStream',
            _inject_headers,
        )
        emitter.register_first(
            'before-send.bedrock-runtime.InvokeModel',
            _inject_headers,
        )
        emitter.register_first(
            'before-send.bedrock-runtime.InvokeModelWithResponseStream',
            _inject_headers,
        )

        return ChatBedrockConverse(
            model_id=model_name,
            endpoint_url=info.connection_url,
            region_name='us-east-1',
            aws_access_key_id='placeholder',
            aws_secret_access_key='placeholder',
            disable_streaming=not streaming,
            client=client,
            **kwargs,
        )

    class OpenAIAuth(httpx.Auth):
        def auth_flow(
            self, request: httpx.Request,
        ) -> Generator[httpx.Request, None, None]:
            if api_key_getter is not None:
                token_val = api_key_getter()
                if token_val:
                    request.headers['Authorization'] = f'Bearer {token_val}'
            if obo_token_getter is not None:
                obo_val = obo_token_getter()
                if obo_val:
                    request.headers['X-S2-OBO'] = obo_val
            yield request

    # Build timeout configuration
    if connect_timeout is not None and read_timeout is not None:
        t = httpx.Timeout(connect=connect_timeout, read=read_timeout)
    elif connect_timeout is not None:
        t = httpx.Timeout(connect=connect_timeout)
    elif read_timeout is not None:
        t = httpx.Timeout(read=read_timeout)
    else:
        t = 60.0  # default OpenAI client timeout

    http_client = httpx.Client(
        timeout=t,
        auth=OpenAIAuth(),
    )

    # OpenAI / Azure OpenAI path
    openai_kwargs = dict(
        base_url=info.connection_url,
        api_key='placeholder',
        model=model_name,
        streaming=streaming,
    )
    openai_kwargs['http_client'] = http_client
    return ChatOpenAI(
        **openai_kwargs,
        **kwargs,
    )
