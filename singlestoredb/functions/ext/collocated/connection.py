"""
Connection handler: protocol, mmap I/O, request loop.

Implements the binary socket protocol matching the Rust wasm-udf-server:
handshake, control signal dispatch, and UDF request loop with mmap I/O.
"""
from __future__ import annotations

import array
import logging
import mmap
import os
import select
import socket
import struct
import threading
import traceback
from typing import TYPE_CHECKING

from .control import dispatch_control_signal
from .registry import call_function

if TYPE_CHECKING:
    from .server import SharedRegistry

logger = logging.getLogger('collocated.connection')

# Protocol constants
PROTOCOL_VERSION = 1
STATUS_OK = 200
STATUS_BAD_REQUEST = 400
STATUS_ERROR = 500

# Minimum output mmap size to avoid repeated ftruncate
_MIN_OUTPUT_SIZE = 128 * 1024


def handle_connection(
    conn: socket.socket,
    shared_registry: SharedRegistry,
    shutdown_event: threading.Event,
) -> None:
    """Handle a single client connection (runs in a thread pool worker)."""
    try:
        _handle_connection_inner(conn, shared_registry, shutdown_event)
    except Exception:
        logger.error(f'Connection error:\n{traceback.format_exc()}')
    finally:
        try:
            conn.close()
        except OSError:
            pass


def _handle_connection_inner(
    conn: socket.socket,
    shared_registry: SharedRegistry,
    shutdown_event: threading.Event,
) -> None:
    """Inner connection handler (may raise)."""
    # --- Handshake ---
    # Receive 16 bytes: [version: u64 LE][namelen: u64 LE]
    header = _recv_exact(conn, 16)
    if header is None:
        return
    version, namelen = struct.unpack('<QQ', header)

    if version != PROTOCOL_VERSION:
        logger.warning(f'Unsupported protocol version: {version}')
        return

    # Receive function name + 2 FDs via SCM_RIGHTS
    fd_model = array.array('i', [0, 0])
    msg, ancdata, flags, addr = conn.recvmsg(
        namelen,
        socket.CMSG_LEN(2 * fd_model.itemsize),
    )
    if len(ancdata) != 1:
        logger.warning(f'Expected 1 ancdata, got {len(ancdata)}')
        return

    function_name = msg.decode('utf8')
    input_fd, output_fd = struct.unpack('<ii', ancdata[0][2])

    # --- Control signal path ---
    if function_name.startswith('@@'):
        logger.info(f"Received control signal '{function_name}'")
        _handle_control_signal(
            conn, function_name, input_fd, output_fd, shared_registry,
        )
        return

    # --- UDF request loop ---
    logger.info(f"Received request for function '{function_name}'")
    _handle_udf_loop(
        conn, function_name, input_fd, output_fd,
        shared_registry, shutdown_event,
    )


def _handle_control_signal(
    conn: socket.socket,
    signal_name: str,
    input_fd: int,
    output_fd: int,
    shared_registry: SharedRegistry,
) -> None:
    """Handle a @@-prefixed control signal (one-shot request-response)."""
    try:
        # Read 8-byte request length
        len_buf = _recv_exact(conn, 8)
        if len_buf is None:
            return
        length = struct.unpack('<Q', len_buf)[0]

        # Read input data from mmap (if any)
        request_data = b''
        if length > 0:
            mem = mmap.mmap(
                input_fd, length, mmap.MAP_SHARED, mmap.PROT_READ,
            )
            try:
                request_data = mem[:length]
            finally:
                mem.close()

        # Dispatch
        result = dispatch_control_signal(
            signal_name, request_data, shared_registry,
        )

        if result.ok:
            # Write response to output mmap
            response_bytes = result.data.encode('utf8')
            response_size = len(response_bytes)
            os.ftruncate(output_fd, max(_MIN_OUTPUT_SIZE, response_size))
            os.lseek(output_fd, 0, os.SEEK_SET)
            os.write(output_fd, response_bytes)

            # Send [status=200, size]
            conn.sendall(struct.pack('<QQ', STATUS_OK, response_size))
            logger.debug(
                f"Control signal '{signal_name}' succeeded, "
                f'{response_size} bytes',
            )
        else:
            # Send [status=400, len, message]
            err_bytes = result.data.encode('utf8')
            conn.sendall(
                struct.pack(
                    f'<QQ{len(err_bytes)}s',
                    STATUS_BAD_REQUEST, len(err_bytes), err_bytes,
                ),
            )
            logger.warning(
                f"Control signal '{signal_name}' failed: {result.data}",
            )
    finally:
        os.close(input_fd)
        os.close(output_fd)


def _handle_udf_loop(
    conn: socket.socket,
    function_name: str,
    input_fd: int,
    output_fd: int,
    shared_registry: SharedRegistry,
    shutdown_event: threading.Event,
) -> None:
    """Handle the UDF request loop for a single function."""
    # Track output mmap size to avoid repeated ftruncate
    current_output_size = 0

    try:
        # Get thread-local registry
        registry = shared_registry.get_thread_local_registry()

        while not shutdown_event.is_set():
            # Select-based recv with 100ms timeout for shutdown checks
            readable, _, _ = select.select([conn], [], [], 0.1)
            if not readable:
                continue

            # Read 8-byte request length
            len_buf = _recv_exact(conn, 8)
            if len_buf is None:
                break
            length = struct.unpack('<Q', len_buf)[0]
            if length == 0:
                break

            # Read input from mmap
            mem = mmap.mmap(
                input_fd, length, mmap.MAP_SHARED, mmap.PROT_READ,
            )
            try:
                input_data = bytes(mem[:length])
            finally:
                mem.close()

            # Refresh registry if generation changed
            registry = shared_registry.get_thread_local_registry()

            # Call function
            try:
                output_data = call_function(registry, function_name, input_data)

                # Write result to output mmap
                response_size = len(output_data)
                needed = max(_MIN_OUTPUT_SIZE, response_size)
                if needed > current_output_size:
                    os.ftruncate(output_fd, needed)
                    current_output_size = needed
                os.lseek(output_fd, 0, os.SEEK_SET)
                os.write(output_fd, output_data)

                # Send [status=200, size]
                conn.sendall(struct.pack('<QQ', STATUS_OK, response_size))

            except Exception as e:
                error_msg = (
                    f"error in function '{function_name}': {e}"
                )
                logger.error(error_msg)
                for line in traceback.format_exc().splitlines():
                    logger.error(line)
                err_bytes = error_msg.encode('utf8')
                conn.sendall(
                    struct.pack(
                        f'<QQ{len(err_bytes)}s',
                        STATUS_ERROR, len(err_bytes), err_bytes,
                    ),
                )
                break

    finally:
        os.close(input_fd)
        os.close(output_fd)


def _recv_exact(sock: socket.socket, n: int) -> bytes | None:
    """Receive exactly n bytes, or return None on EOF."""
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf.extend(chunk)
    return bytes(buf)
