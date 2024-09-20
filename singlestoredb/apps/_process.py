import os
import signal
import typing
if typing.TYPE_CHECKING:
    from psutil import Process


def kill_process_by_port(port: int) -> None:
    existing_process = _find_process_by_port(port)
    kernel_pid = os.getpid()
    # Make sure we are not killing current kernel
    if existing_process is not None and kernel_pid != existing_process.pid:
        print(f'Killing process {existing_process.pid} which is using port {port}')
        os.kill(existing_process.pid, signal.SIGKILL)


def _find_process_by_port(port: int) -> 'Process | None':
    try:
        import psutil
    except ImportError:
        raise ImportError('package psutil is required')

    for proc in psutil.process_iter(['pid']):
        try:
            connections = proc.connections()
            for conn in connections:
                if conn.laddr.port == port:
                    return proc
        except psutil.AccessDenied:
            pass

    return None
