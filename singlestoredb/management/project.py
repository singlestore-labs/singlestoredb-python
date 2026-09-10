#!/usr/bin/env python
"""
SingleStoreDB Project Management.

Projects are only addressed by the v2 wrappers -- ``POST /v2/clusters``
requires a ``projectID`` where the v1 workspace group route assigned one
implicitly -- so the implementation lives in
:mod:`singlestoredb.management.v2.project` and this module is a stable import
path for it, the same arrangement :mod:`singlestoredb.management.cluster` uses.
"""
from .v2.project import Project as Project
