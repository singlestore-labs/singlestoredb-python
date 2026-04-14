"""
CLI entry point for the plugin Python UDF server.

Usage::

    python -m singlestoredb.functions.ext.plugin \\
        --plugin-name myfuncs \\
        --search-path /home/user/libs \\
        --socket /tmp/my-udf.sock

Arguments match the Rust wasm-udf-server CLI for drop-in compatibility.
"""
import argparse
import logging
import os
import secrets
import sys
import tempfile
from typing import Any

from .registry import setup_logging
from .server import Server

logger = logging.getLogger('plugin')


def main(argv: Any = None) -> None:
    parser = argparse.ArgumentParser(
        prog='python -m singlestoredb.functions.ext.plugin',
        description='High-performance plugin Python UDF server',
    )
    parser.add_argument(
        '--plugin-name',
        default=os.environ.get('PLUGIN_NAME', ''),
        help=(
            'Python module to import (e.g. myfuncs). '
            'Env: PLUGIN_NAME'
        ),
    )
    parser.add_argument(
        '--search-path',
        default=os.environ.get('PLUGIN_SEARCH_PATH', ''),
        help=(
            'Colon-separated search dirs for the module. '
            'Env: PLUGIN_SEARCH_PATH'
        ),
    )
    parser.add_argument(
        '--socket',
        default=os.environ.get(
            'PLUGIN_SOCKET_PATH',
            os.path.join(
                tempfile.gettempdir(),
                f'singlestore-udf-{os.getpid()}-{secrets.token_hex(4)}.sock',
            ),
        ),
        help=(
            'Unix socket path. '
            'Env: PLUGIN_SOCKET_PATH'
        ),
    )
    parser.add_argument(
        '--n-workers',
        type=int,
        default=int(os.environ.get('PLUGIN_N_WORKERS', '0')),
        help=(
            'Worker threads (0 = CPU count). '
            'Env: PLUGIN_N_WORKERS'
        ),
    )
    parser.add_argument(
        '--max-connections',
        type=int,
        default=int(os.environ.get('PLUGIN_MAX_CONNECTIONS', '32')),
        help=(
            'Socket backlog. '
            'Env: PLUGIN_MAX_CONNECTIONS'
        ),
    )
    parser.add_argument(
        '--log-level',
        default=os.environ.get('PLUGIN_LOG_LEVEL', 'info'),
        choices=['debug', 'info', 'warning', 'error'],
        help=(
            'Logging level. '
            'Env: PLUGIN_LOG_LEVEL'
        ),
    )
    parser.add_argument(
        '--process-mode',
        default=os.environ.get('PLUGIN_PROCESS_MODE', 'process'),
        choices=['thread', 'process'],
        help=(
            'Concurrency mode: "thread" uses a thread pool, '
            '"process" uses pre-fork workers for true CPU '
            'parallelism. Env: PLUGIN_PROCESS_MODE'
        ),
    )

    args = parser.parse_args(argv)

    if not args.plugin_name:
        parser.error(
            '--plugin-name is required '
            '(or set PLUGIN_NAME env var)',
        )

    # Setup logging
    level = getattr(logging, args.log_level.upper())
    setup_logging(level)

    config = {
        'plugin_name': args.plugin_name,
        'search_path': args.search_path,
        'socket': args.socket,
        'n_workers': args.n_workers,
        'max_connections': args.max_connections,
        'process_mode': args.process_mode,
    }

    server = Server(config)
    try:
        server.run()
    except RuntimeError as exc:
        logger.error(str(exc))
        sys.exit(1)
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()
