#!/usr/bin/env python3
import importlib
import os

from .registry import execute
from .registry import get_handler

# Load all files in handlers directory
for f in os.listdir(os.path.join(os.path.dirname(__file__), 'handlers')):
    if f.endswith('.py') and not f.startswith('_'):
        importlib.import_module(f'singlestoredb.fusion.handlers.{f[:-3]}')
