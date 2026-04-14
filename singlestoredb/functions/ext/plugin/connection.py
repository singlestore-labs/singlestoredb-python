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
import socket
import struct
import threading
import time
import traceback
from typing import TYPE_CHECKING

from .control import dispatch_control_signal
from .registry import _has_accel
from .registry import _mmap_read
from .registry import _mmap_write
from .registry import _recv_exact as _c_recv_exact
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

# Pre-pack the status OK header prefix to avoid per-request struct.pack
_STATUS_OK_PREFIX = struct.pack('<Q', STATUS_OK)

# Maximum function name length to prevent resource exhaustion
_MAX_FUNCTION_NAME_LEN = 4096

# Enable per-request timing via environment variable
_PROFILE = os.environ.get('SINGLESTOREDB_UDF_PROFILE', '') == '1'


def handle_connection(
    conn: socket.socket,
    shared_registry: SharedRegistry,
    shutdown_event: threading.Event,
    pipe_write_fd: int | None = None,
) -> None:
    """Handle a single client connection (runs in a thread pool worker)."""
    try:
        _handle_connection_inner(
            conn, shared_registry, shutdown_event, pipe_write_fd,
        )
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
    pipe_write_fd: int | None = None,
) -> None:
    """Inner connection handler (may raise)."""
    # --- Handshake ---
    # Receive 16 bytes: [version: u64 LE][namelen: u64 LE]
    header = _recv_exact_py(conn, 16)
    if header is None:
        return
    version, namelen = struct.unpack('<QQ', header)

    if version != PROTOCOL_VERSION:
        logger.warning(f'Unsupported protocol version: {version}')
        return

    if namelen > _MAX_FUNCTION_NAME_LEN:
        logger.warning(f'Function name too long: {namelen}')
        return

    # Receive function name + 2 FDs via SCM_RIGHTS
    fd_model = array.array('i', [0, 0])
    msg, ancdata, flags, addr = conn.recvmsg(
        namelen,
        socket.CMSG_LEN(2 * fd_model.itemsize),
    )

    # Validate ancdata and extract FDs
    received_fds: list[int] = []
    try:
        if len(ancdata) != 1:
            logger.warning(f'Expected 1 ancdata, got {len(ancdata)}')
            return

        level, type_, fd_data = ancdata[0]
        if level != socket.SOL_SOCKET or type_ != socket.SCM_RIGHTS:
            logger.warning(
                f'Unexpected ancdata level={level} type={type_}',
            )
            return

        if flags & getattr(socket, 'MSG_CTRUNC', 0):
            logger.warning('Ancillary data was truncated (MSG_CTRUNC)')
            return

        fd_array = array.array('i')
        fd_array.frombytes(fd_data)
        received_fds = list(fd_array)

        if len(received_fds) != 2:
            logger.warning(
                f'Expected 2 FDs, got {len(received_fds)}',
            )
            return

        function_name = msg.decode('utf8')
        input_fd, output_fd = received_fds[0], received_fds[1]
        # Clear so finally doesn't close FDs we're handing off
        received_fds = []
    finally:
        # Close any received FDs if we're returning early
        for fd in received_fds:
            try:
                os.close(fd)
            except OSError:
                pass

    # --- Control signal path ---
    if function_name.startswith('@@'):
        logger.info(f"Received control signal '{function_name}'")
        _handle_control_signal(
            conn, function_name, input_fd, output_fd, shared_registry,
            pipe_write_fd,
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
    pipe_write_fd: int | None = None,
) -> None:
    """Handle a @@-prefixed control signal (one-shot request-response)."""
    try:
        # Read 8-byte request length
        len_buf = _recv_exact_py(conn, 8)
        if len_buf is None:
            return
        length = struct.unpack('<Q', len_buf)[0]

        # Read input data from mmap (if any)
        request_data = b''
        if length > 0:
            if _has_accel:
                request_data = _mmap_read(input_fd, length)
            else:
                mem = mmap.mmap(
                    input_fd, length, mmap.MAP_SHARED, mmap.PROT_READ,
                )
                try:
                    request_data = bytes(mem[:length])
                finally:
                    mem.close()

        # Dispatch
        result = dispatch_control_signal(
            signal_name, request_data, shared_registry, pipe_write_fd,
        )

        if result.ok:
            # Write response to output mmap
            response_bytes = result.data.encode('utf8')
            response_size = len(response_bytes)
            if _has_accel:
                _mmap_write(
                    output_fd, response_bytes,
                    max(_MIN_OUTPUT_SIZE, response_size),
                )
            else:
                os.ftruncate(output_fd, max(_MIN_OUTPUT_SIZE, response_size))
                os.lseek(output_fd, 0, os.SEEK_SET)
                _write_all_fd(output_fd, response_bytes)

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

    # Choose recv implementation: C accel or Python fallback
    use_accel = _has_accel
    sock_fd = conn.fileno()

    if use_accel:
        # Keep the fd in blocking mode. The C recv_exact uses poll()
        # internally with a timeout, avoiding the interaction between
        # Python's settimeout() (which sets O_NONBLOCK on the fd) and
        # direct fd-level recv() in the C code.
        pass
    else:
        # Python fallback: settimeout makes recv_into raise
        # socket.timeout (alias for TimeoutError) when no data arrives.
        conn.settimeout(0.1)

    # Profiling accumulators
    profile = _PROFILE
    if profile:
        n_requests = 0
        t_recv = 0.0
        t_mmap_read = 0.0
        t_call = 0.0
        t_mmap_write = 0.0
        t_send = 0.0

    try:
        # Get thread-local registry
        registry = shared_registry.get_thread_local_registry()

        while not shutdown_event.is_set():
            # Read 8-byte request length (with timeout for shutdown checks)
            try:
                if use_accel:
                    if profile:
                        t0 = time.monotonic()
                    len_buf = _c_recv_exact(sock_fd, 8, 100)
                    if profile:
                        t_recv += time.monotonic() - t0
                else:
                    if profile:
                        t0 = time.monotonic()
                    len_buf = _recv_exact_py(conn, 8)
                    if profile:
                        t_recv += time.monotonic() - t0
            except TimeoutError:
                continue
            except OSError:
                break

            if len_buf is None:
                break
            length = struct.unpack('<Q', len_buf)[0]
            if length == 0:
                break

            # Read input from mmap
            if profile:
                t0 = time.monotonic()
            if use_accel:
                input_data = _mmap_read(input_fd, length)
            else:
                mem = mmap.mmap(
                    input_fd, length, mmap.MAP_SHARED, mmap.PROT_READ,
                )
                try:
                    input_data = bytes(mem[:length])
                finally:
                    mem.close()
            if profile:
                t_mmap_read += time.monotonic() - t0

            # Refresh registry if generation changed
            registry = shared_registry.get_thread_local_registry()

            # Call function
            try:
                if profile:
                    t0 = time.monotonic()
                output_data = call_function(
                    registry, function_name, input_data,
                )
                if profile:
                    t_call += time.monotonic() - t0

                # Write result to output mmap
                response_size = len(output_data)
                if profile:
                    t0 = time.monotonic()
                if use_accel:
                    needed = max(_MIN_OUTPUT_SIZE, response_size)
                    if needed > current_output_size:
                        _mmap_write(output_fd, output_data, needed)
                        current_output_size = needed
                    else:
                        _mmap_write(output_fd, output_data, 0)
                else:
                    needed = max(_MIN_OUTPUT_SIZE, response_size)
                    if needed > current_output_size:
                        os.ftruncate(output_fd, needed)
                        current_output_size = needed
                    os.lseek(output_fd, 0, os.SEEK_SET)
                    _write_all_fd(output_fd, output_data)
                if profile:
                    t_mmap_write += time.monotonic() - t0

                # Send [status=200, size]
                if profile:
                    t0 = time.monotonic()
                conn.sendall(
                    _STATUS_OK_PREFIX + struct.pack('<Q', response_size),
                )
                if profile:
                    t_send += time.monotonic() - t0
                    n_requests += 1

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

        if profile and n_requests > 0:
            t_total = (
                t_recv + t_mmap_read + t_call + t_mmap_write + t_send
            ) / n_requests * 1e6
            logger.info(
                f"PROFILE '{function_name}' "
                f'n={n_requests} '
                f'recv={t_recv / n_requests * 1e6:.1f}us '
                f'mmap_read={t_mmap_read / n_requests * 1e6:.1f}us '
                f'call={t_call / n_requests * 1e6:.1f}us '
                f'mmap_write={t_mmap_write / n_requests * 1e6:.1f}us '
                f'send={t_send / n_requests * 1e6:.1f}us '
                f'total={t_total:.1f}us',
            )


def _recv_exact_py(sock: socket.socket, n: int) -> bytes | None:
    """Receive exactly n bytes, or return None on EOF."""
    buf = bytearray(n)
    view = memoryview(buf)
    pos = 0
    orig_timeout = sock.gettimeout()
    while pos < n:
        try:
            nbytes = sock.recv_into(view[pos:])
        except TimeoutError:
            if pos == 0:
                raise
            # Partial message already consumed — must finish it.
            # Remove timeout to avoid protocol desync.
            sock.settimeout(None)
            continue
        if nbytes == 0:
            if orig_timeout is not None:
                sock.settimeout(orig_timeout)
            return None
        pos += nbytes
    if orig_timeout is not None:
        sock.settimeout(orig_timeout)
    return bytes(buf)


def _write_all_fd(fd: int, data: bytes) -> None:
    """Write all bytes to a file descriptor, handling partial writes."""
    view = memoryview(data)
    written = 0
    while written < len(data):
        try:
            n = os.write(fd, view[written:])
        except InterruptedError:
            continue
        if n == 0:
            raise RuntimeError('short write to output fd')
        written += n
