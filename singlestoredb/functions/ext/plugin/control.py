"""
Control signal dispatch for @@health, @@functions, @@register, @@delete.

Matches the Rust wasm-udf-server's dispatch_control_signal behavior, including
the structured-error-code shape from ADR 0001
(``{"message": "...", "code": "SCREAMING_SNAKE"}`` on errors). The authoritative
catalog of codes lives in ADR 0001 in the ``wasm-udf-server`` repo; the codes
emitted from this module are:

- ``UNKNOWN_SIGNAL`` — unrecognized ``@@``-prefixed signal name.
- ``INTERNAL_ERROR`` — cross-cutting fallback for unexpected handler exceptions.
- ``REGISTER_MISSING_PAYLOAD`` — ``@@register`` called with an empty body.
- ``REGISTER_INVALID_PAYLOAD`` — ``@@register`` body failed JSON parsing or
  field validation.
- ``REGISTER_FUNC_EXISTS`` — function with the same name is already registered
  and ``replace`` was not requested.
- ``REGISTER_FUNC_NOT_DYNAMIC`` — ``replace`` requested for a function that was
  not dynamically registered (e.g., a built-in).
- ``DELETE_MISSING_PAYLOAD`` — ``@@delete`` called with an empty body.
- ``DELETE_INVALID_PAYLOAD`` — ``@@delete`` body failed JSON parsing or field
  validation.
- ``DELETE_FUNC_NOT_REGISTERED`` — target function exists but was not
  dynamically registered, so it cannot be deleted.
- ``DELETE_FUNC_NOT_FOUND`` — target function does not exist.

The ``REGISTER_DISABLED`` and ``DELETE_DISABLED`` codes from that catalog have
no call site here because this server has no registration enable/disable flag.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from .registry import describe_functions_json
from .registry import FunctionExistsError
from .registry import FunctionNotDynamicError
from .registry import FunctionNotFoundError

if TYPE_CHECKING:
    from .server import SharedRegistry

logger = logging.getLogger('plugin.control')


@dataclass
class ControlResult:
    """Result of a control signal dispatch."""
    ok: bool
    # JSON response. On success (``ok=True``) this is a handler-specific
    # document such as ``{"status":"ok"}`` or ``{"functions":[...]}``. On
    # failure (``ok=False``) this is the ADR 0001 error shape
    # ``{"message":"...","code":"..."}``.
    data: str


def _err(message: str, code: str) -> ControlResult:
    """Build an error ControlResult with the ADR 0001 JSON shape."""
    return ControlResult(
        ok=False,
        data=json.dumps({'message': message, 'code': code}),
    )


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
        elif signal_name == '@@delete':
            return _handle_delete(
                request_data, shared_registry, pipe_write_fd,
            )
        else:
            return _err(
                f'Unknown control signal: {signal_name}',
                'UNKNOWN_SIGNAL',
            )
    except Exception as e:
        return _err(str(e), 'INTERNAL_ERROR')


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

    Security: the function body is passed to ``exec()`` via
    ``FunctionRegistry.create_function``. The security boundary is
    the Unix socket permission (0o600) set in ``Server._bind_socket``,
    which restricts access to the socket owner.

    If ``pipe_write_fd`` is not None (process mode), the registration
    payload is written to the pipe so the main process can update its
    own registry and re-fork all workers.
    """
    if not request_data:
        return _err('Missing registration payload', 'REGISTER_MISSING_PAYLOAD')

    try:
        body = json.loads(request_data)
    except json.JSONDecodeError as e:
        return _err(f'Invalid JSON: {e}', 'REGISTER_INVALID_PAYLOAD')

    function_name = body.get('name')
    if not function_name:
        return _err(
            'Missing required field: name', 'REGISTER_INVALID_PAYLOAD',
        )

    args = body.get('args')
    if not isinstance(args, list):
        return _err(
            'Missing required field: args (must be an array)',
            'REGISTER_INVALID_PAYLOAD',
        )

    returns = body.get('returns')
    if not isinstance(returns, list):
        return _err(
            'Missing required field: returns (must be an array)',
            'REGISTER_INVALID_PAYLOAD',
        )

    func_body = body.get('body')
    if not func_body:
        return _err(
            'Missing required field: body', 'REGISTER_INVALID_PAYLOAD',
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
    except FunctionExistsError as e:
        return _err(str(e), 'REGISTER_FUNC_EXISTS')
    except FunctionNotDynamicError as e:
        return _err(str(e), 'REGISTER_FUNC_NOT_DYNAMIC')
    except Exception as e:
        return _err(str(e), 'REGISTER_INVALID_PAYLOAD')

    # Notify main process so it can re-fork workers with updated state
    if pipe_write_fd is not None:
        from .server import _write_pipe_message
        payload = json.dumps({
            'action': 'register',
            'signature_json': signature,
            'code': func_body,
            'replace': replace,
        }).encode()
        _write_pipe_message(pipe_write_fd, payload)

    logger.info(f"@@register: added function '{function_name}'")
    return ControlResult(ok=True, data='{"status":"ok"}')


def _handle_delete(
    request_data: bytes,
    shared_registry: SharedRegistry,
    pipe_write_fd: int | None = None,
) -> ControlResult:
    """Handle @@delete: delete a dynamically registered function."""
    if not request_data:
        return _err('Missing deletion payload', 'DELETE_MISSING_PAYLOAD')

    try:
        body = json.loads(request_data)
    except json.JSONDecodeError as e:
        return _err(f'Invalid JSON: {e}', 'DELETE_INVALID_PAYLOAD')

    function_name = body.get('name')
    if not function_name:
        return _err(
            'Missing required field: name', 'DELETE_INVALID_PAYLOAD',
        )

    try:
        shared_registry.delete_function(function_name)
    except FunctionNotDynamicError as e:
        return _err(str(e), 'DELETE_FUNC_NOT_REGISTERED')
    except FunctionNotFoundError as e:
        return _err(str(e), 'DELETE_FUNC_NOT_FOUND')
    except ValueError as e:
        return _err(str(e), 'DELETE_INVALID_PAYLOAD')

    # Notify main process so it can re-fork workers with updated state
    if pipe_write_fd is not None:
        from .server import _write_pipe_message
        payload = json.dumps({
            'action': 'delete',
            'function_name': function_name,
        }).encode()
        _write_pipe_message(pipe_write_fd, payload)

    logger.info(f"@@delete: removed function '{function_name}'")
    return ControlResult(ok=True, data='{"status":"ok"}')
