#!/usr/bin/env python
"""Configure Pytest"""

# Load the pytest plugin so that the fixtures are available
from singlestoredb import pytest

# Module is not directly used
del pytest
