from __future__ import annotations

from .http import SingleStoreDialect_http


class SingleStoreDialect_https(SingleStoreDialect_http):
    driver = 'https'


dialect = SingleStoreDialect_https
