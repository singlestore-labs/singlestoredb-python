#!/usr/bin/env python3
"""
Manage SingleStoreDB Docker container for documentation generation.

Usage:
    singlestoredb-docker.py start  - Start container and output connection URL
    singlestoredb-docker.py stop   - Stop and remove container
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time


CONTAINER_INFO_FILE = '.singlestore_docs_container.txt'


def start_container() -> int:
    """Start SingleStoreDB Docker container and return connection URL."""
    try:
        from singlestoredb.server import docker
    except ImportError:
        print('Error: singlestoredb package not installed', file=sys.stderr)
        print('Please install it with: pip install singlestoredb', file=sys.stderr)
        return 1

    print('Starting SingleStoreDB Docker container for documentation...', file=sys.stderr)

    # Start the container
    server = docker.start()

    # Get the connection URL
    conn_url = server.connection_url

    # Save the container info for stopping later
    with open(CONTAINER_INFO_FILE, 'w') as f:
        f.write(f'{server.container.name}\n')
        f.write(f'{conn_url}\n')

    # Wait for the container to be ready
    print('Waiting for SingleStoreDB to be ready...', file=sys.stderr)
    max_retries = 30

    for attempt in range(max_retries):
        try:
            test_conn = server.connect()
            test_conn.close()
            print(f'SingleStoreDB is ready! (took {attempt + 1} seconds)', file=sys.stderr)
            break
        except Exception as e:
            if attempt == max_retries - 1:
                print(f'Error: Container failed to start after {max_retries} seconds', file=sys.stderr)
                print(f'Last error: {e}', file=sys.stderr)
                return 1
            time.sleep(1)

    # Output the connection URL (this is what the Makefile will capture)
    print(conn_url)
    return 0


def stop_container() -> int:
    """Stop and remove the SingleStoreDB Docker container."""
    if not os.path.exists(CONTAINER_INFO_FILE):
        print('No container info file found. Nothing to stop.', file=sys.stderr)
        return 0

    try:
        with open(CONTAINER_INFO_FILE, 'r') as f:
            lines = f.readlines()
            if not lines:
                print('Container info file is empty. Nothing to stop.', file=sys.stderr)
                return 0

            container_name = lines[0].strip()

        print(f'Stopping SingleStoreDB container: {container_name}', file=sys.stderr)

        # Stop the container
        subprocess.run(
            ['docker', 'stop', container_name],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        # Remove the container
        subprocess.run(
            ['docker', 'rm', container_name],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        # Remove the info file
        os.remove(CONTAINER_INFO_FILE)

        print('SingleStoreDB container stopped and removed.', file=sys.stderr)
        return 0

    except Exception as e:
        print(f'Error stopping container: {e}', file=sys.stderr)
        # Still try to remove the info file
        if os.path.exists(CONTAINER_INFO_FILE):
            os.remove(CONTAINER_INFO_FILE)
        return 1


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Manage SingleStoreDB Docker container for documentation generation',
    )
    parser.add_argument(
        'command',
        choices=['start', 'stop'],
        help='Command to execute',
    )

    args = parser.parse_args()

    if args.command == 'start':
        return start_container()
    elif args.command == 'stop':
        return stop_container()
    else:
        parser.print_help()
        return 1


if __name__ == '__main__':
    sys.exit(main())
