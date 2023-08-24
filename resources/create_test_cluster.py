#!/usr/bin/env python
# type: ignore
from __future__ import annotations

import os
import random
import re
import secrets
import subprocess
import sys
import time
import uuid
from optparse import OptionParser

import singlestoredb as s2


# Handle command-line options
usage = 'usage: %prog [options] workspace-name'
parser = OptionParser(usage=usage)
parser.add_option(
    '-r', '--region',
    default='AWS::*US East 1*',
    help='region pattern or ID',
)
parser.add_option(
    '-p', '--password',
    default=secrets.token_urlsafe(20) + '-x',
    help='admin password',
)
parser.add_option(
    '-e', '--expires',
    default='4h',
    help='timestamp when workspace should expire (4h)',
)
parser.add_option(
    '-s', '--size',
    default='S-00',
    help='size of the workspace (S-00)',
)
parser.add_option(
    '-t', '--token',
    help='API key for the workspace management API',
)
parser.add_option(
    '--http-port', type='int',
    help='enable HTTP API on given port',
)
parser.add_option(
    '-i', '--init-sql',
    help='initialize database with given SQL file',
)
parser.add_option(
    '-o', '--output',
    default='env', choices=['env', 'github', 'json'],
    help='report workspace information in the requested format: github, env, json',
)
parser.add_option(
    '-d', '--database',
    help='database name to create',
)

(options, args) = parser.parse_args()

if len(args) != 1:
    parser.print_help()
    sys.exit(1)

if options.init_sql and not os.path.isfile(options.init_sql):
    print('ERROR: Could not locate SQL file: {options.init_sql}', file=sys.stderr)
    sys.exit(1)


# Connect to workspace
wm = s2.manage_workspaces(options.token or None)

# Find matching region
if '::' in options.region:
    pattern = options.region.replace('*', '.*')
    regions = wm.regions
    for item in random.sample(regions, k=len(regions)):
        region_name = '{}::{}'.format(item.provider, item.name)
        if re.match(pattern, region_name):
            options.region = item.id
            break

if '::' in options.region:
    print(
        'ERROR: Could not find a region mating the pattern: '
        '{options.region}', file=sys.stderr,
    )
    sys.exit(1)


# Create workspace group
wg_name = 'Python Client Testing'

wgs = [x for x in wm.workspace_groups if x.name == wg_name]
if len(wgs) > 1:
    print('ERROR: There is more than one workspace group with the specified name.')
    sys.exit(1)
elif len(wgs) == 1:
    wg = wgs[0]
else:
    wg = wm.create_workspace_group(
        wg_name,
        region=options.region,
        admin_password=options.password,
        # firewall_ranges=requests.get('https://api.github.com/meta').json()['actions'],
        firewall_ranges=['0.0.0.0/0'],
    )

# Make sure the workspace group exists before continuing
timeout = 300
while timeout > 0 and not [x for x in wm.workspace_groups if x.name == wg_name]:
    time.sleep(10)
    timeout -= 10

ws_name = re.sub(r'^-|-$', r'', re.sub(r'-+', r'-', re.sub(r'\s+', '-', args[0].lower())))

ws = wg.create_workspace(
    ws_name,
    size=options.size,
    wait_on_active=True,
)

# Make sure the endpoint exists before continuing
timeout = 300
while timeout > 0 and not ws.endpoint:
    time.sleep(10)
    ws.refresh()
    timeout -= 10

if not ws.endpoint:
    print('ERROR: Endpoint was never activated.')
    sys.exit(1)


# Extra pause for server to become available
time.sleep(10)

database = options.database
if not database:
    database = 'TEMP_{}'.format(uuid.uuid4()).replace('-', '_')

host = ws.endpoint
if ':' in host:
    host, port = host.split(':', 1)
    port = int(port)
else:
    port = 3306

# Print workspace information
if options.output == 'env':
    print(f'CLUSTER_ID={ws.id}')
    print(f'CLUSTER_HOST={host}')
    print(f'CLUSTER_PORT={port}')
    print(f'CLUSTER_DATABASE={database}')
elif options.output == 'github':
    with open(os.environ['GITHUB_OUTPUT'], 'a') as output:
        print(f'cluster-id={ws.id}', file=output)
        print(f'cluster-host={host}', file=output)
        print(f'cluster-port={port}', file=output)
        print(f'cluster-database={database}', file=output)
elif options.output == 'json':
    print('{')
    print(f'  "cluster-id": "{ws.id}",')
    print(f'  "cluster-host": "{host}",')
    print(f'  "cluster-port": {port}')
    print(f'  "cluster-database": {database}')
    print('}')

# Initialize the database
if options.init_sql:
    init_db = [
        os.path.join(os.path.dirname(__file__), 'init_db.py'),
        '--host', str(host), '--port', str(port),
        '--user', 'admin', '--password', options.password,
        '--database', database,
    ]

    if options.http_port:
        init_db += ['--http-port', str(options.http_port)]

    init_db.append(options.init_sql)

    subprocess.check_call(init_db)
