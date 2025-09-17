import os
from typing import Any
from typing import AsyncIterator
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


class SingleStoreExperimentalChat:
    """Experimental unified chat interface (prefix-based two-part identifier).

    Input model name MUST (for dynamic selection) be of the form:
        <prefix>.<actual_model>
    where <prefix> is one of:
        * ``aura``      -> OpenAI style (ChatOpenAI backend)
        * ``aura-azr``  -> Azure OpenAI style (still ChatOpenAI backend)
        * ``aura-amz``  -> Amazon Bedrock (ChatBedrockConverse backend)

    If no delimiter (".", ":" or "/") is present, or prefix is unrecognized,
    the entire string is treated as an OpenAI-style model (ChatOpenAI).

    Only the prefix ``aura-amz`` triggers Bedrock usage; in that case the
    *second* component (after the first delimiter) is passed as the model
    name to the Bedrock client. For other prefixes the second component is
    passed to ChatOpenAI with the SingleStore Fusion-provided base_url.

    This class uses composition and delegates attribute access to the chosen
    backend client for near drop-in behavior.
    """

    _VALID_PREFIXES = {'aura', 'aura-azr', 'aura-amz'}

    def __init__(
        self,
        model_name: str,
        http_client: Optional[httpx.Client] = None,
        api_key: Optional[str] = None,
        obo_token_getter: Optional[Callable[[], Optional[str]]] = None,
        streaming: bool = False,
        **kwargs: Any,
    ) -> None:
        prefix, actual_model = self._parse_identifier(model_name)

        inference_api_manager = (
            get_workspace_manager().organizations.current.inference_apis
        )
        # Use the raw identifier for Fusion lookup (keeps gateway mapping
        # logic server-side).
        info = inference_api_manager.get(model_name=actual_model)
        if prefix == 'aura-amz':
            backend_type = 'bedrock'
        elif prefix == 'aura-azr':
            backend_type = 'azure-openai'
        else:
            backend_type = 'openai'

        # Extract headers from provided http_client (if any) for possible reuse.
        provided_headers: dict[str, str] = {}
        if http_client is not None and hasattr(http_client, 'headers'):
            try:
                provided_headers = dict(http_client.headers)  # make a copy
            except Exception:
                provided_headers = {}

        if backend_type == 'bedrock':
            self._removed_aws_env: dict[str, str] = {}
            for _v in (
                'AWS_ACCESS_KEY_ID',
                'AWS_SECRET_ACCESS_KEY',
                'AWS_SESSION_TOKEN',
            ):
                if _v in os.environ:
                    self._removed_aws_env[_v] = os.environ.pop(_v)

            token_env = os.environ.get('SINGLESTOREDB_USER_TOKEN')
            token = api_key if api_key is not None else token_env
            self._client = ChatBedrockConverse(
                base_url=info.connection_url,
                model=actual_model,
                streaming=streaming,
                **kwargs,
            )

            # Attempt to inject Authorization header for downstream HTTP layers.
            # Not all implementations expose a direct header map; we add a
            # lightweight wrapper if needed.
            self._auth_header = None
            merged_headers: dict[str, str] = {}
            if provided_headers:
                merged_headers.update({k: v for k, v in provided_headers.items()})
            if token:
                merged_headers.setdefault('Authorization', f'Bearer {token}')
            # Add Bedrock converse headers based on streaming flag
            if streaming:
                merged_headers.setdefault('X-BEDROCK-CONVERSE-STREAMING', 'true')
            else:
                merged_headers.setdefault('X-BEDROCK-CONVERSE', 'true')
            if merged_headers:
                # Try to set directly if backend exposes default_headers
                if (
                    hasattr(self._client, 'default_headers')
                    and isinstance(
                        getattr(self._client, 'default_headers'),
                        dict,
                    )
                ):
                    getattr(self._client, 'default_headers').update(
                        {
                            k: v
                            for k, v in merged_headers.items()
                            if k
                            not in getattr(
                                self._client, 'default_headers',
                            )
                        },
                    )
                else:
                    self._auth_header = merged_headers  # fallback for invoke/stream
        else:
            # Pass through http_client if ChatOpenAI supports it; if not,
            # include in kwargs only when present.
            token_env = os.environ.get('SINGLESTOREDB_USER_TOKEN')
            token = api_key if api_key is not None else token_env
            openai_kwargs = dict(
                base_url=info.connection_url,
                api_key=token,
                model=actual_model,
                streaming=streaming,
            )
            if http_client is not None:
                # Some versions accept 'http_client' parameter for custom transport.
                openai_kwargs['http_client'] = http_client
            self._client = ChatOpenAI(
                **openai_kwargs,
                **kwargs,
            )

        self._backend_type = backend_type
        self.model_name = model_name         # external identifier provided by caller
        self.actual_model = actual_model     # model portion after prefix
        self.prefix = prefix                 # normalized prefix
        self.connection_url = info.connection_url
        # Optional callable returning a fresh OBO token each request (Bedrock only).
        # If supplied, a new token will be fetched and injected into the
        # 'X-S2-OBO' header for every Bedrock request made via this wrapper.
        self._obo_token_getter = obo_token_getter
        self._streaming = streaming

    @classmethod
    def _parse_identifier(cls, identifier: str) -> tuple[str, str]:
        for sep in ('.', ':', '/'):
            if sep in identifier:
                head, tail = identifier.split(sep, 1)
                prefix = head.strip().lower()
                model = tail.strip()
                if prefix in cls._VALID_PREFIXES:
                    return prefix, model
                return 'aura', identifier.strip()  # treat whole string as model
        return 'aura', identifier.strip()

    # ---------------------------------------------------------------------
    # Delegation layer
    # ---------------------------------------------------------------------
    def __getattr__(self, item: str) -> Any:
        return getattr(self._client, item)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _maybe_inject_headers(self, kwargs: dict[str, Any]) -> None:
        """Inject Bedrock auth headers into kwargs if we only have a fallback.

        If the Bedrock client accepted headers via its own internal
        `default_headers` we don't need to do anything here. When we had
        to stash headers into `_auth_header` we add them for each outbound
        call that allows a `headers` kwarg and has not already provided
        its own.
        """
        if self._backend_type != 'bedrock':
            return

    # Start from existing headers in the call.
    # Copy to avoid mutating caller-provided dict in-place.
        call_headers: dict[str, str] = {}
        if 'headers' in kwargs and isinstance(kwargs['headers'], dict):
            call_headers = dict(kwargs['headers'])
        elif (
            hasattr(self, '_auth_header')
            and getattr(self, '_auth_header')
            and 'headers' not in kwargs
        ):
            # Use fallback auth header if user did not pass any.
            call_headers = dict(getattr(self, '_auth_header'))

        # Dynamic OBO token injection (always fresh per request if getter provided)
        getter = getattr(self, '_obo_token_getter', None)
        if getter is not None:
            try:
                obo_token = getter()
            except Exception:
                obo_token = None
            if obo_token:
                # Overwrite any stale value.
                call_headers['X-S2-OBO'] = obo_token

        if call_headers:
            kwargs['headers'] = call_headers

    def as_base(self) -> Any:
        """Return the underlying backend client instance.

        This gives callers direct access to provider specific methods or
        configuration that aren't surfaced by the experimental wrapper.
        """
        return self._client

    def invoke(self, *args: Any, **kwargs: Any) -> Any:
        self._maybe_inject_headers(kwargs)
        return self._client.invoke(*args, **kwargs)

    async def ainvoke(self, *args: Any, **kwargs: Any) -> Any:
        self._maybe_inject_headers(kwargs)
        return await self._client.ainvoke(*args, **kwargs)

    def stream(self, *args: Any, **kwargs: Any) -> Any:
        self._maybe_inject_headers(kwargs)
        return self._client.stream(*args, **kwargs)

    async def astream(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> AsyncIterator[Any]:
        self._maybe_inject_headers(kwargs)
        async for chunk in self._client.astream(*args, **kwargs):
            yield chunk

    # ------------------------------------------------------------------
    # Extended delegation for additional common chat model surface area.
    # Each method simply injects headers (if needed) then forwards.
    # ------------------------------------------------------------------
    def generate(self, *args: Any, **kwargs: Any) -> Any:
        self._maybe_inject_headers(kwargs)
        return self._client.generate(*args, **kwargs)

    async def agenerate(self, *args: Any, **kwargs: Any) -> Any:
        self._maybe_inject_headers(kwargs)
        return await self._client.agenerate(*args, **kwargs)

    def predict(self, *args: Any, **kwargs: Any) -> Any:
        self._maybe_inject_headers(kwargs)
        return self._client.predict(*args, **kwargs)

    async def apredict(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        self._maybe_inject_headers(kwargs)
        return await self._client.apredict(*args, **kwargs)

    def predict_messages(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        self._maybe_inject_headers(kwargs)
        return self._client.predict_messages(*args, **kwargs)

    async def apredict_messages(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        self._maybe_inject_headers(kwargs)
        return await self._client.apredict_messages(*args, **kwargs)

    def batch(self, *args: Any, **kwargs: Any) -> Any:
        self._maybe_inject_headers(kwargs)
        return self._client.batch(*args, **kwargs)

    async def abatch(self, *args: Any, **kwargs: Any) -> Any:
        self._maybe_inject_headers(kwargs)
        return await self._client.abatch(*args, **kwargs)

    def apply(self, *args: Any, **kwargs: Any) -> Any:
        self._maybe_inject_headers(kwargs)
        return self._client.apply(*args, **kwargs)

    async def aapply(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        self._maybe_inject_headers(kwargs)
        return await self._client.aapply(*args, **kwargs)

    def __repr__(self) -> str:
        return (
            'SingleStoreExperimentalChat('
            f'identifier={self.model_name!r}, '
            f'actual_model={self.actual_model!r}, '
            f'prefix={self.prefix}, backend={self._backend_type})'
        )
