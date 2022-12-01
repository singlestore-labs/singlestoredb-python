#!/usr/bin/env python
# type: ignore
from __future__ import annotations

import re
import sys
from optparse import OptionParser

import singlestoredb as s2


# Handle command-line options
usage = 'usage: %prog [options] workspace-id'
parser = OptionParser(usage=usage)
parser.add_option(
    '-t', '--token',
    help='API key for the workspace management API',
)
(options, args) = parser.parse_args()

if len(args) != 1:
    parser.print_help()
    sys.exit(1)


# Connect to workspace
wm = s2.manage_workspaces(options.token or None)

wg_name = 'Python Client Testing'

wgs = [x for x in wm.workspace_groups if x.name == wg_name]
if len(wgs) > 1:
    print('ERROR: There is more than one workspace group with the specified name.')
    sys.exit(1)
elif len(wgs) == 0:
    print('ERROR: There is no workspace group with the specified name.')
    sys.exit(1)
wg = wgs[0]

ws_name = re.sub(r'^-|-$', r'', re.sub(r'-+', r'-', re.sub(r'\s+', '-', args[0].lower())))

wss = [x for x in wg.workspaces if x.name == ws_name]
if len(wss) > 1:
    print('ERROR: There is more than one workspace with the specified name.')
    sys.exit(1)
elif len(wss) == 0:
    print('ERROR: There is no workspace with the specified name.')
    sys.exit(1)
ws = wss[0]

# Terminate workspace
ws.terminate()
