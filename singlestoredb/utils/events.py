#!/usr/bin/env python
from typing import Any
from typing import Callable
from typing import Dict
from typing import Set


_subscribers: Set[Callable[[Dict[str, Any]], None]] = set()
_initialized = False


def _ensure_initialized() -> None:
    """Lazily detect IPython and register the control handler."""
    global _initialized
    if _initialized:
        return
    _initialized = True

    try:
        from IPython import get_ipython
        _handlers = get_ipython().kernel.control_handlers
        _handlers['singlestore_portal_request'] = _event_handler
    except (ImportError, OSError, AttributeError):
        pass


def subscribe(func: Callable[[Dict[str, Any]], None]) -> None:
    """
    Subscribe to SingleStore portal events.

    Parameters
    ----------
    func : Callable
        The function to call when an event is received

    """
    _ensure_initialized()
    _subscribers.add(func)


def unsubscribe(func: Callable[[Dict[str, Any]], None]) -> None:
    """
    Unsubscribe from SingleStore portal events.

    Parameters
    ----------
    func : Callable
        The function to call when an event is received

    """
    try:
        _subscribers.remove(func)
    except KeyError:
        pass


def _event_handler(stream: Any, ident: Any, msg: Dict[str, Any]) -> None:
    """Handle request on the control stream."""
    if not _subscribers or not isinstance(msg, dict):
        return

    content = msg.get('content', {})
    if content.get('type', '') != 'event':
        return

    for func in _subscribers:
        func(content)
