"""
Control signal dispatch for @@health, @@functions, @@delete.

Matches the Rust wasm-udf-server's dispatch_control_signal behavior.
Registration happens in-band via the v3 handshake envelope; there is no
separate @@register control signal.
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
        elif signal_name == '@@delete':
            return _handle_delete(
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


def _handle_delete(
    request_data: bytes,
    shared_registry: SharedRegistry,
    pipe_write_fd: int | None = None,
) -> ControlResult:
    """Handle @@delete: delete a dynamically registered function by id."""
    if not request_data:
        return ControlResult(ok=False, data='Missing deletion payload')

    try:
        body = json.loads(request_data)
    except json.JSONDecodeError as e:
        return ControlResult(ok=False, data=f'Invalid JSON: {e}')

    id = body.get('id')
    if not id:
        return ControlResult(
            ok=False, data='Missing required field: id',
        )

    try:
        shared_registry.delete_function(id)
    except ValueError as e:
        return ControlResult(ok=False, data=str(e))

    # Notify main process so it can re-fork workers with updated state
    if pipe_write_fd is not None:
        from .server import _write_pipe_message
        payload = json.dumps({
            'action': 'delete',
            'id': id,
        }).encode()
        _write_pipe_message(pipe_write_fd, payload)

    logger.info(f'@@delete: removed function id={id!r}')
    return ControlResult(ok=True, data='{"status":"ok"}')
