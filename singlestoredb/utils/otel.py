"""OpenTelemetry trace-context propagation for outbound HTTP requests."""
from typing import Any
from typing import Union

import httpx


def inject_otel_headers(headers: Any) -> None:
    """Add the current W3C trace context to ``headers``, if OpenTelemetry is installed."""
    try:
        from opentelemetry.propagate import inject as otel_inject
    except ImportError:
        return
    try:
        otel_inject(headers)
    except Exception:
        return


def httpx_inject_otel(request: httpx.Request) -> None:
    inject_otel_headers(request.headers)


async def httpx_inject_otel_async(request: httpx.Request) -> None:
    inject_otel_headers(request.headers)


def attach_otel_request_hook(
    client: Union[httpx.Client, httpx.AsyncClient],
) -> None:
    """Inject the trace context into every request sent by ``client``."""
    # httpx.AsyncClient awaits its event hooks, so it needs a coroutine hook.
    hook: Any = (
        httpx_inject_otel_async
        if isinstance(client, httpx.AsyncClient)
        else httpx_inject_otel
    )
    hooks = client.event_hooks.setdefault('request', [])
    if hook not in hooks:
        hooks.append(hook)
