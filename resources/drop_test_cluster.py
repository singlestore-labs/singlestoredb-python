#!/usr/bin/env python
# type: ignore
from __future__ import annotations

import sys
from optparse import OptionParser

import singlestoredb as s2


# Handle command-line options
usage = 'usage: %prog [options] cluster-id'
parser = OptionParser(usage=usage)
parser.add_option(
    '-t', '--token',
    help='API key for the management API',
)
(options, args) = parser.parse_args()

if len(args) != 1:
    parser.print_help()
    sys.exit(1)


# Pin v2 explicitly rather than following the ambient management.version
# option: clusters only exist in v2.
mgr = s2.manage_clusters(options.token or None, version='v2')

# force=True so a cluster with connections still open goes away; this only
# ever runs against clusters this repo's CI created.
mgr.get_cluster(args[0]).terminate(force=True, wait_on_terminated=True)
