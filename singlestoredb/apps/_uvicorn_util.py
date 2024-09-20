import asyncio
import socket
from typing import List
from typing import Optional
try:
    import uvicorn
except ImportError:
    raise ImportError('package uvicorn is required')


class AwaitableUvicornServer(uvicorn.Server):
    """
    Adds `wait_for_startup` method.
    The function (asynchronously) blocks until the server
    starts listening or throws an error.
    """

    def __init__(self, config: 'uvicorn.Config') -> None:
        super().__init__(config)
        self._startup_future = asyncio.get_event_loop().create_future()

    async def startup(self, sockets: Optional[List[socket.socket]] = None) -> None:
        try:
            result = await super().startup(sockets)
            self._startup_future.set_result(True)
            return result
        except Exception as error:
            self._startup_future.set_exception(error)
            raise error

    async def wait_for_startup(self) -> None:
        await self._startup_future
