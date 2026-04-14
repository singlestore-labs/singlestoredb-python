"""
Control signal dispatch for @@health, @@functions, @@register.

Matches the Rust wasm-udf-server's dispatch_control_signal behavior.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from .registry import describe_functions_json

if TYPE_CHECKING:
    from .server import SharedRegistry

logger = logging.getLogger('plugin.control')


@dataclass
class ControlResult:
    """Result of a control signal dispatch."""
    ok: bool
    data: str  # JSON response on success, error message on failure


def dispatch_control_signal(
    signal_name: str,
    request_data: bytes,
    shared_registry: SharedRegistry,
    pipe_write_fd: int | None = None,
) -> ControlResult:
    """Dispatch a control signal to the appropriate handler."""
    try:
        if signal_name == '@@health':
            return _handle_health()
        elif signal_name == '@@functions':
            return _handle_functions(shared_registry)
        elif signal_name == '@@register':
            return _handle_register(
                request_data, shared_registry, pipe_write_fd,
            )
        else:
            return ControlResult(
                ok=False,
                data=f'Unknown control signal: {signal_name}',
            )
    except Exception as e:
        return ControlResult(ok=False, data=str(e))


def _handle_health() -> ControlResult:
    """Handle @@health: return status ok."""
    return ControlResult(ok=True, data='{"status":"ok"}')


def _handle_functions(shared_registry: SharedRegistry) -> ControlResult:
    """Handle @@functions: return function descriptions."""
    registry = shared_registry.get_thread_local_registry()
    json_str = describe_functions_json(registry)
    return ControlResult(ok=True, data=f'{{"functions":{json_str}}}')


def _handle_register(
    request_data: bytes,
    shared_registry: SharedRegistry,
    pipe_write_fd: int | None = None,
) -> ControlResult:
    """Handle @@register: register a new function dynamically.

    If ``pipe_write_fd`` is not None (process mode), the registration
    payload is written to the pipe so the main process can update its
    own registry and re-fork all workers.
    """
    if not request_data:
        return ControlResult(ok=False, data='Missing registration payload')

    try:
        body = json.loads(request_data)
    except json.JSONDecodeError as e:
        return ControlResult(ok=False, data=f'Invalid JSON: {e}')

    function_name = body.get('function_name')
    if not function_name:
        return ControlResult(
            ok=False, data='Missing required field: function_name',
        )

    args = body.get('args')
    if not isinstance(args, list):
        return ControlResult(
            ok=False, data='Missing required field: args (must be an array)',
        )

    returns = body.get('returns')
    if not isinstance(returns, list):
        return ControlResult(
            ok=False,
            data='Missing required field: returns (must be an array)',
        )

    func_body = body.get('body')
    if not func_body:
        return ControlResult(
            ok=False, data='Missing required field: body',
        )

    replace = body.get('replace', False)

    # Build signature JSON matching describe-functions schema
    signature = json.dumps({
        'name': function_name,
        'args': args,
        'returns': returns,
    })

    try:
        shared_registry.create_function(signature, func_body, replace)
    except Exception as e:
        return ControlResult(ok=False, data=str(e))

    # Notify main process so it can re-fork workers with updated state
    if pipe_write_fd is not None:
        from .server import _write_pipe_message
        payload = json.dumps({
            'signature_json': signature,
            'code': func_body,
            'replace': replace,
        }).encode()
        _write_pipe_message(pipe_write_fd, payload)

    logger.info(f"@@register: added function '{function_name}'")
    return ControlResult(ok=True, data='{"status":"ok"}')
