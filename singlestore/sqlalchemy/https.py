#!/usr/bin/env python
"""SingleStore SQLAlchemy HTTPS API driver."""
from __future__ import annotations

from .http import SingleStoreDialect_http


class SingleStoreDialect_https(SingleStoreDialect_http):
    """SingleStore SQLAlchemy HTTPS API dialect."""

    driver = 'https'


dialect = SingleStoreDialect_https
