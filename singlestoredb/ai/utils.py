from collections.abc import Generator
from typing import Callable
from typing import Optional

import httpx


class SingleStoreOpenAIAuth(httpx.Auth):
    def __init__(
        self,
        api_key_getter: Optional[Callable[[], Optional[str]]] = None,
        obo_token_getter: Optional[Callable[[], Optional[str]]] = None,
    ) -> None:
        self.api_key_getter = api_key_getter
        self.obo_token_getter = obo_token_getter

    def auth_flow(
        self, request: httpx.Request,
    ) -> Generator[httpx.Request, None, None]:
        print(f'[DEBUG] auth_flow called for {request.method} {request.url}')
        if self.api_key_getter is not None:
            token_val = self.api_key_getter()
            print(f"[DEBUG] api_key_getter: {token_val if token_val else 'None'}...")
            if token_val:
                request.headers['Authorization'] = f'Bearer {token_val}'
        if self.obo_token_getter is not None:
            obo_val = self.obo_token_getter()
            print(f"[DEBUG] obo_token_getter: {obo_val if obo_val else 'None'}...")
            if obo_val:
                request.headers['X-S2-OBO'] = obo_val
        print(f'[DEBUG] Final headers: {dict(request.headers)}')
        yield request
