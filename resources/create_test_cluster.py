#!/usr/bin/env python
# type: ignore
from __future__ import annotations

import os
import random
import re
import subprocess
import sys
import uuid
from optparse import OptionParser

import singlestoredb as s2


# Handle command-line options
usage = 'usage: %prog [options] cluster-name'
parser = OptionParser(usage=usage)
parser.add_option(
    '-r', '--region',
    default='AWS::*US East 1*',
    help='region pattern to deploy into, as provider::name '
         '(AWS::*US East 1*); * is a wildcard',
)
parser.add_option(
    '-e', '--expires',
    default='4h',
    help='when the cluster should expire, as a timestamp or a '
         'duration such as 4h (4h)',
)
parser.add_option(
    '-s', '--size',
    default='S-00',
    help='size of the cluster (S-00)',
)
parser.add_option(
    '-p', '--password',
    help='password to give the admin user once the cluster is up; required, '
         'because the password the API generates cannot be reported to a '
         'caller that masks it',
)
parser.add_option(
    '-t', '--token',
    help='API key for the management API',
)
parser.add_option(
    '--project',
    help='ID or name of the project to deploy into; defaults to the '
         'organization\'s STANDARD-edition project',
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
    help='report cluster information in the requested format: github, env, json',
)
parser.add_option(
    '-d', '--database',
    help='database name to create',
)

(options, args) = parser.parse_args()

if len(args) != 1:
    parser.print_help()
    sys.exit(1)

if not options.password:
    print('ERROR: --password is required', file=sys.stderr)
    sys.exit(1)

if options.init_sql and not os.path.isfile(options.init_sql):
    print(f'ERROR: Could not locate SQL file: {options.init_sql}', file=sys.stderr)
    sys.exit(1)


# Pin v2 explicitly rather than following the ambient management.version
# option: this script provisions clusters, which only exist in v2.
mgr = s2.manage_clusters(options.token or None, version='v2')


# Find a matching region. A v2 region is identified by the
# (provider, region_name) pair rather than an ID, so the matched Region object
# itself is handed to create_cluster. Candidates are shuffled to spread
# deployments across whichever regions match, and the pattern is tried against
# both the display name and the provider region name -- 'US East 1' and
# 'us-east-1' -- since either may land in Region.name.
pattern = options.region.replace('*', '.*')
regions = list(mgr.regions)


def candidates(item):
    """Return the names ``item`` can be matched by, most specific first."""
    for label in (item.name, item.region_name):
        if label:
            yield f'{item.provider}::{label}' if '::' in options.region else label


region = None
for item in random.sample(regions, k=len(regions)):
    if any(re.match(pattern, x) for x in candidates(item)):
        region = item
        break

if region is None:
    print(
        'ERROR: Could not find a region matching the pattern '
        f'{options.region}; the API reports: ' +
        ', '.join(sorted(f'{x.provider}::{x.name}' for x in regions)),
        file=sys.stderr,
    )
    sys.exit(1)


# Choose a project. projectID is required by POST /v2/clusters and only
# auto-resolves for an organization with a single project, so pick the
# STANDARD-edition one when it was not named explicitly.
if options.project:
    project_id = options.project
else:
    projects = list(mgr.projects)
    standard = [x for x in projects if x.edition == 'STANDARD']
    if not standard:
        print(
            'ERROR: No STANDARD-edition project in this organization; pass '
            '--project with one of: ' +
            ', '.join(f'{x.name} ({x.id}, {x.edition})' for x in projects),
            file=sys.stderr,
        )
        sys.exit(1)
    project_id = standard[0].id


# A cluster name must match [a-z0-9]([a-z0-9-]*[a-z0-9])? and be 1-32
# characters: fold everything outside that alphabet to a hyphen, truncate, and
# trim any hyphen the cut exposes.
name = re.sub(r'[^a-z0-9]+', '-', args[0].lower()).strip('-')[:32].rstrip('-')
if not name:
    print(f'ERROR: Cluster name is empty after cleaning: {args[0]}', file=sys.stderr)
    sys.exit(1)


# wait_on_active covers ACTIVE, then the endpoint, then the firewall, so the
# cluster is actually reachable by the time this returns.
cluster = mgr.create_cluster(
    name,
    region=region,
    size=options.size,
    firewall_ranges=['0.0.0.0/0'],
    expires_at=options.expires,
    project=project_id,
    wait_on_active=True,
    wait_timeout=1200,
)

host = cluster.endpoint
if ':' in host:
    host, port = host.split(':', 1)
    port = int(port)
else:
    port = 3306

database = options.database
if not database:
    database = 'TEMP_{}'.format(uuid.uuid4()).replace('-', '_')

# Report before touching the cluster any further. Everything below can fail
# against a cluster that is already billing, and the ID reported here is the
# caller's only handle on it -- a CI teardown job with an empty cluster-id output
# would DELETE /v2/clusters/ and leak the cluster it meant to remove.
#
# No password is reported: the caller passed it in, so it already has it.
if options.output == 'env':
    print(f'CLUSTER_ID={cluster.id}')
    print(f'CLUSTER_HOST={host}')
    print(f'CLUSTER_PORT={port}')
    print(f'CLUSTER_DATABASE={database}')
elif options.output == 'github':
    with open(os.environ['GITHUB_OUTPUT'], 'a') as output:
        print(f'cluster-id={cluster.id}', file=output)
        print(f'cluster-host={host}', file=output)
        print(f'cluster-port={port}', file=output)
        print(f'cluster-database={database}', file=output)
elif options.output == 'json':
    print('{')
    print(f'  "cluster-id": "{cluster.id}",')
    print(f'  "cluster-host": "{host}",')
    print(f'  "cluster-port": {port},')
    print(f'  "cluster-database": "{database}"')
    print('}')

# The API generates the admin password and reports it only on the create
# response: no route hands it back later, and it is None after any refresh().
# It is read back rather than set because the API accepts an adminPassword on
# both POST and PATCH and ignores both -- item 9 of
# docs/management-api-audit.md.
generated = cluster.admin_password
if not generated:
    print(
        'ERROR: cluster was created without a readable admin password',
        file=sys.stderr,
    )
    sys.exit(1)

# Trade the generated password for the caller's, because the generated one
# cannot leave this process: a GitHub Actions runner drops any output whose value
# is masked -- "Skip output 'cluster-password' since it may contain secret" -- so
# masking it and passing it to another job are mutually exclusive.
#
# ALTER USER, not SET PASSWORD, which wants a pre-hashed value and rejects a
# literal with '1372: Password hash should be a 41-digit hexadecimal number'.
password = options.password
escaped = password.replace('\\', '\\\\').replace("'", "\\'")

with s2.connect(
    host=host, port=port, user='admin',
    password=generated, connect_timeout=30,
) as conn:
    with conn.cursor() as cur:
        cur.execute(f"ALTER USER 'admin'@'%' IDENTIFIED BY '{escaped}'")

# Initialize the database
if options.init_sql:
    init_db = [
        os.path.join(os.path.dirname(__file__), 'init_db.py'),
        '--host', str(host), '--port', str(port),
        '--user', 'admin', '--password', password,
        '--database', database,
    ]

    if options.http_port:
        init_db += ['--http-port', str(options.http_port)]

    init_db.append(options.init_sql)

    subprocess.check_call(init_db)
