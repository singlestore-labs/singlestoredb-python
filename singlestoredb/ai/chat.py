import os
from typing import Any
from typing import Callable
from typing import Optional

import httpx

from singlestoredb.fusion.handlers.utils import get_workspace_manager

try:
    from langchain_openai import ChatOpenAI
except ImportError:
    raise ImportError(
        'Could not import langchain_openai python package. '
        'Please install it with `pip install langchain-openai`.',
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


class SingleStoreChatOpenAI(ChatOpenAI):
    def __init__(self, model_name: str, api_key: Optional[str] = None, **kwargs: Any):
        inference_api_manger = (
            get_workspace_manager().organizations.current.inference_apis
        )
        info = inference_api_manger.get(model_name=model_name)
        token = (
            api_key
            if api_key is not None
            else os.environ.get('SINGLESTOREDB_USER_TOKEN')
        )
        super().__init__(
            base_url=info.connection_url,
            api_key=token,
            model=model_name,
            **kwargs,
        )


class SingleStoreChat(ChatOpenAI):
    def __init__(self, model_name: str, api_key: Optional[str] = None, **kwargs: Any):
        inference_api_manger = (
            get_workspace_manager().organizations.current.inference_apis
        )
        info = inference_api_manger.get(model_name=model_name)
        token = (
            api_key
            if api_key is not None
            else os.environ.get('SINGLESTOREDB_USER_TOKEN')
        )
        super().__init__(
            base_url=info.connection_url,
            api_key=token,
            model=model_name,
            **kwargs,
        )


def SingleStoreChatFactory(
    model_name: str,
    api_key: Optional[str] = None,
    streaming: bool = True,
    http_client: Optional[httpx.Client] = None,
    obo_token_getter: Optional[Callable[[], Optional[str]]] = None,
    **kwargs: Any,
) -> ChatOpenAI | ChatBedrockConverse:
    """Return a chat model instance (ChatOpenAI or ChatBedrockConverse) based on prefix.

    The fully-qualified model name is expected to contain a prefix followed by
    a delimiter (one of '.', ':', '/'). Supported prefixes:
      * aura -> OpenAI style (ChatOpenAI backend)
      * aura-azr -> Azure OpenAI style (still ChatOpenAI backend)
      * aura-amz -> Amazon Bedrock (ChatBedrockConverse backend)

    If no supported prefix is detected the entire value is treated as an
    OpenAI-style model routed through the SingleStore Fusion gateway.
    """
    # Parse identifier
    prefix = 'aura'
    actual_model = model_name
    for sep in ('.', ':', '/'):
        if sep in model_name:
            head, tail = model_name.split(sep, 1)
            candidate = head.strip().lower()
            if candidate in {'aura', 'aura-azr', 'aura-amz'}:
                prefix = candidate
                actual_model = tail.strip()
            else:
                # Unsupported prefix; treat whole string as model for OpenAI path
                actual_model = model_name
            break

    inference_api_manager = (
        get_workspace_manager().organizations.current.inference_apis
    )
    info = inference_api_manager.get(model_name=actual_model)
    token_env = os.environ.get('SINGLESTOREDB_USER_TOKEN')
    token = api_key if api_key is not None else token_env

    if prefix == 'aura-amz':
        # Instantiate Bedrock client
        cfg = Config(
            signature_version=UNSIGNED,
            retries={
                'max_attempts': 1,
                'mode': 'standard',
            },
        )
        if http_client is not None and http_client.timeout is not None:
            cfg.timeout = http_client.timeout
            cfg.connect_timeout = http_client.timeout
        client = boto3.client(
            'bedrock-runtime',
            endpoint_url=info.connection_url,  # redirect requests to UMG
            region_name='us-east-1',  # dummy value; UMG does not use this
            aws_access_key_id='placeholder',  # dummy value; UMG does not use this
            aws_secret_access_key='placeholder',  # dummy value; UMG does not use this
            config=cfg,
        )
        if obo_token_getter is not None:
            def _inject_headers(request: Any, **_ignored: Any) -> None:
                """Inject dynamic auth/OBO headers prior to Bedrock signing."""
                obo_val = obo_token_getter()
                if obo_val:
                    request.headers['X-S2-OBO'] = obo_val
                if token:
                    request.headers['Authorization'] = f'Bearer {token}'
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
            model=actual_model,
            endpoint_url=info.connection_url,  # redirect requests to UMG
            region_name='us-east-1',  # dummy value; UMG does not use this
            aws_access_key_id='placeholder',  # dummy value; UMG does not use this
            aws_secret_access_key='placeholder',  # dummy value; UMG does not use this
            disable_streaming=not streaming,
            client=client,
            **kwargs,
        )

    # OpenAI / Azure OpenAI path
    openai_kwargs = dict(
        base_url=info.connection_url,
        api_key=token,
        model=actual_model,
        streaming=streaming,
    )
    if http_client is not None:
        openai_kwargs['http_client'] = http_client
    return ChatOpenAI(
        **openai_kwargs,
        **kwargs,
    )
