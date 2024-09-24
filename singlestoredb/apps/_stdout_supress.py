import io
import sys
from typing import Optional


class StdoutSuppressor:
    """
    Supresses the stdout for code executed within the context.
    This should not be used for asynchronous or threaded executions.

    ```py
    with Supressor():
        print("This won't be printed")
    ```

    """

    def __enter__(self) -> None:
        self.stdout = sys.stdout
        self.buffer = io.StringIO()
        sys.stdout = self.buffer

    def __exit__(
        self,
        exc_type: Optional[object],
        exc_value: Optional[Exception],
        exc_traceback: Optional[str],
    ) -> None:
        del self.buffer
        sys.stdout = self.stdout
