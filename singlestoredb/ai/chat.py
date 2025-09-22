import os
from typing import Any
from typing import Callable
from typing import Optional
from typing import Union

import httpx

from singlestoredb import manage_workspaces

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


class SingleStoreChatOpenAI(ChatOpenAI):
    def __init__(self, model_name: str, api_key: Optional[str] = None, **kwargs: Any):
        inference_api_manger = (
            manage_workspaces().organizations.current.inference_apis
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
            manage_workspaces().organizations.current.inference_apis
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
) -> Union[ChatOpenAI, ChatBedrockConverse]:
    """Return a chat model instance (ChatOpenAI or ChatBedrockConverse).
    """
    inference_api_manager = (
        manage_workspaces().organizations.current.inference_apis
    )
    info = inference_api_manager.get(model_name=model_name)
    token_env = os.environ.get('SINGLESTOREDB_USER_TOKEN')
    token = api_key if api_key is not None else token_env

    if info.hosting_platform == 'Amazon':
        # Instantiate Bedrock client
        cfg_kwargs = {
            'signature_version': UNSIGNED,
            'retries': {'max_attempts': 1, 'mode': 'standard'},
        }
        if http_client is not None and http_client.timeout is not None:
            cfg_kwargs['read_timeout'] = http_client.timeout
            cfg_kwargs['connect_timeout'] = http_client.timeout

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
            if obo_token_getter is not None:
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
            model_id=model_name,
            endpoint_url=info.connection_url,
            region_name='us-east-1',
            aws_access_key_id='placeholder',
            aws_secret_access_key='placeholder',
            disable_streaming=not streaming,
            client=client,
            **kwargs,
        )

    # OpenAI / Azure OpenAI path
    openai_kwargs = dict(
        base_url=info.connection_url,
        api_key=token,
        model=model_name,
        streaming=streaming,
    )
    if http_client is not None:
        openai_kwargs['http_client'] = http_client
    return ChatOpenAI(
        **openai_kwargs,
        **kwargs,
    )
