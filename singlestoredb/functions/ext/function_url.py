"""Parse and classify external function service URLs from SHOW CREATE."""
from __future__ import annotations

import re
from typing import Optional

from ...mysql.constants import ER


# Managed Python UDFs emit: AS MANAGED SERVICE "https://..."
# Older / remote forms may use SERVICE 'https://...'
_SERVICE_URL_RE = re.compile(
    r'(?:MANAGED\s+)?SERVICE\s+[\'"]([^\'"]+)[\'"]',
    re.IGNORECASE,
)


def extract_service_url(create_sql: str) -> Optional[str]:
    """Return the MANAGED/REMOTE SERVICE URL from SHOW CREATE FUNCTION text."""
    if not create_sql:
        return None
    match = _SERVICE_URL_RE.search(create_sql)
    if match is None:
        return None
    return match.group(1)


def normalize_service_url(url: str) -> str:
    return url.rstrip('/')


def urls_equal(left: Optional[str], right: Optional[str]) -> bool:
    if not left or not right:
        return False
    return normalize_service_url(left) == normalize_service_url(right)


def is_interactive_service_url(url: Optional[str]) -> bool:
    if not url:
        return False
    return '/interactive' in normalize_service_url(url).lower()


def classify_interactive_registration(
    existing_url: Optional[str],
    this_session_url: str,
) -> str:
    """Return 'create', 'replace', or raise ValueError if the name is not ours.

    Interactive registration may only create a missing name or replace a
    function that already points at this notebook session's /interactive/ URL.
    """
    if not existing_url:
        return 'create'
    if urls_equal(existing_url, this_session_url):
        return 'replace'
    raise ValueError(
        f'Cannot register over existing function pointing at {existing_url} '
        f'(this session is {this_session_url}). '
        'Interactive registration will not replace a published or '
        'other-session function.',
    )


def is_function_not_defined(exc: BaseException) -> bool:
    errno = getattr(exc, 'errno', None)
    if errno in (ER.FUNCTION_NOT_DEFINED, ER.SP_DOES_NOT_EXIST):
        return True
    args = getattr(exc, 'args', ())
    if args and args[0] in (ER.FUNCTION_NOT_DEFINED, ER.SP_DOES_NOT_EXIST):
        return True
    return False
