import os
from typing import Any

from singlestoredb.fusion.handlers.utils import get_workspace_manager

try:
    from langchain_openai import ChatOpenAI
except ImportError:
    raise ImportError(
        'Could not import langchain_openai python package. '
        'Please install it with `pip install langchain_openai`.',
    )


class SingleStoreChatOpenAI(ChatOpenAI):
    def __init__(self, model_name: str, **kwargs: Any):
        inference_api_manger = (
            get_workspace_manager().organizations.current.inference_apis
        )
        info = inference_api_manger.get(model_name=model_name)
        super().__init__(
            base_url=info.connection_url,
            api_key=os.environ.get('SINGLESTOREDB_USER_TOKEN'),
            model=model_name,
            **kwargs,
        )


class SingleStoreChat(ChatOpenAI):
    def __init__(self, model_name: str, **kwargs: Any):
        inference_api_manger = (
            get_workspace_manager().organizations.current.inference_apis
        )
        info = inference_api_manger.get(model_name=model_name)
        super().__init__(
            base_url=info.connection_url,
            api_key=os.environ.get('SINGLESTOREDB_USER_TOKEN'),
            model=model_name,
            **kwargs,
        )
