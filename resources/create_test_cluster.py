#!/usr/bin/env python
# type: ignore
from __future__ import annotations

import os
import random
import re
import secrets
import subprocess
import sys
from optparse import OptionParser

import singlestore as s2


# Handle command-line options
usage = 'usage: %prog [options] cluster-name'
parser = OptionParser(usage=usage)
parser.add_option(
    '-r', '--region',
    default='AWS::*US East 1*',
    help='region pattern or ID',
)
parser.add_option(
    '-p', '--password',
    default=secrets.token_urlsafe(20),
    help='admin password',
)
parser.add_option(
    '-e', '--expires',
    default='4h',
    help='timestamp when cluster should expire (4h)',
)
parser.add_option(
    '-s', '--size',
    default='S-00',
    help='size of the cluster (S-00)',
)
parser.add_option(
    '-t', '--token',
    help='API key for the cluster management API',
)
parser.add_option(
    '--http-port',
    help='enable HTTP API on given port',
)
parser.add_option(
    '-i', '--init-sql',
    help='initialize database with given SQL file',
)

(options, args) = parser.parse_args()

if len(args) != 1:
    parser.print_help()
    sys.exit(1)

if options.init_sql and not os.path.isfile(options.init_sql):
    print('ERROR: Could not locate SQL file: {options.init_sql}', file=sys.stderr)
    sys.exit(1)


# Connect to cluster
cm = s2.manage_cluster(options.token or None)

# Find matching region
if '::' in options.region:
    pattern = options.region.replace('*', '.*')
    regions = cm.regions
    for item in random.sample(regions, k=len(regions)):
        region_name = '{}::{}'.format(item.provider, item.region)
        if re.match(pattern, region_name):
            options.region = item.id
            break

if '::' in options.region:
    print(
        'ERROR: Could not find a region mating the pattern: '
        '{options.region}', file=sys.stderr,
    )
    sys.exit(1)

# Create cluster
clus = cm.create_cluster(
    args[0],
    region_id=options.region,
    admin_password=options.password,
    # firewall_ranges=requests.get('https://api.github.com/meta').json()['actions'],
    firewall_ranges=['0.0.0.0/0'],
    expires_at=options.expires,
    size=options.size,
    plan='poc',
    wait_on_active=True,
)

# TODO: When we can discover the hostname, change this.
host = f'svc-{clus.id}-ddl.aws-virginia-2.svc.singlestore.com'
port = 3306

# Initialize the database
if options.init_sql:
    init_db = [
        os.path.join(__file__, 'init_db.py'),
        '--host', host, '--port', port,
        '--user', 'admin', '--password', options.password,
    ]

    if options.http_port:
        init_db += ['--http-port', options.http_port]

    subprocess.check_call(init_db)

print(clus.id, host)
