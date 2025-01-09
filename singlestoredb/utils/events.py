#!/usr/bin/env python
from typing import Any
from typing import Callable
from typing import Dict
from typing import Set

try:
    from IPython import get_ipython
    has_ipython = True
except ImportError:
    has_ipython = False


_subscribers: Set[Callable[[Dict[str, Any]], None]] = set()


def subscribe(func: Callable[[Dict[str, Any]], None]) -> None:
    """
    Subscribe to SingleStore portal events.

    Parameters
    ----------
    func : Callable
        The function to call when an event is received

    """
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


# Inject a control handler to receive SingleStore events
if has_ipython:
    try:
        _handlers = get_ipython().kernel.control_handlers
        _handlers['singlestore_portal_request'] = _event_handler
    except AttributeError:
        pass
