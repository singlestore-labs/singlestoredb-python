import sys
from typing import Any

from ..config import get_option


def log_query(query: str, args: Any = None) -> None:
    """Log the query and parameters."""
    if get_option('debug.queries'):
        if args is None:
            print('[QUERY]', query, file=sys.stderr)
        else:
            print('[QUERY]', query, args, file=sys.stderr)
