#!/usr/bin/env python
# type: ignore
from __future__ import annotations

import sys
from optparse import OptionParser

import singlestore as s2


# Handle command-line options
usage = 'usage: %prog [options] cluster-id'
parser = OptionParser(usage=usage)
parser.add_option(
    '-t', '--token',
    help='API key for the cluster management API',
)
(options, args) = parser.parse_args()

if len(args) != 1:
    parser.print_help()
    sys.exit(1)


# Connect to cluster
cm = s2.manage_cluster(options.token or None)

# Create cluster
clus = cm.get_cluster(args[0])
clus.terminate()
