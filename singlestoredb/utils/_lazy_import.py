#!/usr/bin/env python3
"""Lazy import utilities for heavy optional dependencies."""
import importlib
from functools import lru_cache
from typing import Any
from typing import Optional


@lru_cache(maxsize=None)
def get_numpy() -> Optional[Any]:
    """Return numpy module or None if not installed."""
    try:
        return importlib.import_module('numpy')
    except ImportError:
        return None


@lru_cache(maxsize=None)
def get_pandas() -> Optional[Any]:
    """Return pandas module or None if not installed."""
    try:
        return importlib.import_module('pandas')
    except ImportError:
        return None


@lru_cache(maxsize=None)
def get_polars() -> Optional[Any]:
    """Return polars module or None if not installed."""
    try:
        return importlib.import_module('polars')
    except ImportError:
        return None


@lru_cache(maxsize=None)
def get_pyarrow() -> Optional[Any]:
    """Return pyarrow module or None if not installed."""
    try:
        return importlib.import_module('pyarrow')
    except ImportError:
        return None
