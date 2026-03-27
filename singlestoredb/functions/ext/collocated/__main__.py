"""
CLI entry point for the collocated Python UDF server.

Usage::

    python -m singlestoredb.functions.ext.collocated \\
        --extension myfuncs \\
        --extension-path /home/user/libs \\
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

logger = logging.getLogger('collocated')


def main(argv: Any = None) -> None:
    parser = argparse.ArgumentParser(
        prog='python -m singlestoredb.functions.ext.collocated',
        description='High-performance collocated Python UDF server',
    )
    parser.add_argument(
        '--extension',
        default=os.environ.get('EXTERNAL_UDF_EXTENSION', ''),
        help=(
            'Python module to import (e.g. myfuncs). '
            'Env: EXTERNAL_UDF_EXTENSION'
        ),
    )
    parser.add_argument(
        '--extension-path',
        default=os.environ.get('EXTERNAL_UDF_EXTENSION_PATH', ''),
        help=(
            'Colon-separated search dirs for the module. '
            'Env: EXTERNAL_UDF_EXTENSION_PATH'
        ),
    )
    parser.add_argument(
        '--socket',
        default=os.environ.get(
            'EXTERNAL_UDF_SOCKET_PATH',
            os.path.join(
                tempfile.gettempdir(),
                f'singlestore-udf-{os.getpid()}-{secrets.token_hex(4)}.sock',
            ),
        ),
        help=(
            'Unix socket path. '
            'Env: EXTERNAL_UDF_SOCKET_PATH'
        ),
    )
    parser.add_argument(
        '--n-workers',
        type=int,
        default=int(os.environ.get('EXTERNAL_UDF_N_WORKERS', '0')),
        help=(
            'Worker threads (0 = CPU count). '
            'Env: EXTERNAL_UDF_N_WORKERS'
        ),
    )
    parser.add_argument(
        '--max-connections',
        type=int,
        default=int(os.environ.get('EXTERNAL_UDF_MAX_CONNECTIONS', '32')),
        help=(
            'Socket backlog. '
            'Env: EXTERNAL_UDF_MAX_CONNECTIONS'
        ),
    )
    parser.add_argument(
        '--log-level',
        default=os.environ.get('EXTERNAL_UDF_LOG_LEVEL', 'info'),
        choices=['debug', 'info', 'warning', 'error'],
        help=(
            'Logging level. '
            'Env: EXTERNAL_UDF_LOG_LEVEL'
        ),
    )
    parser.add_argument(
        '--process-mode',
        default=os.environ.get('EXTERNAL_UDF_PROCESS_MODE', 'process'),
        choices=['thread', 'process'],
        help=(
            'Concurrency mode: "thread" uses a thread pool, '
            '"process" uses pre-fork workers for true CPU '
            'parallelism. Env: EXTERNAL_UDF_PROCESS_MODE'
        ),
    )

    args = parser.parse_args(argv)

    if not args.extension:
        parser.error(
            '--extension is required '
            '(or set EXTERNAL_UDF_EXTENSION env var)',
        )

    # Setup logging
    level = getattr(logging, args.log_level.upper())
    setup_logging(level)

    config = {
        'extension': args.extension,
        'extension_path': args.extension_path,
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
