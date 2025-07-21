import json
import time
from typing import Any
from typing import Dict
from typing import Optional

from . import utils

logger = utils.get_logger('singlestoredb.functions.ext.metrics')


class RoundedFloatEncoder(json.JSONEncoder):

    def encode(self, obj: Any) -> str:
        if isinstance(obj, dict):
            return '{' + ', '.join(
                f'"{k}": {self._format_value(v)}'
                for k, v in obj.items()
            ) + '}'
        return super().encode(obj)

    def _format_value(self, value: Any) -> str:
        if isinstance(value, float):
            return f'{value:.2f}'
        return json.dumps(value)


class Timer:
    """
    Timer context manager that supports nested timing using a stack.

    Example
    -------
    timer = Timer()

    with timer('total'):
        with timer('receive_data'):
            time.sleep(0.1)
        with timer('parse_input'):
            time.sleep(0.2)
        with timer('call_function'):
            with timer('inner_operation'):
                time.sleep(0.05)
            time.sleep(0.3)

    print(timer.metrics)
    # {'receive_data': 0.1, 'parse_input': 0.2, 'inner_operation': 0.05,
    #  'call_function': 0.35, 'total': 0.65}

    """

    def __init__(self, **kwargs: Any) -> None:
        self.metadata: Dict[str, Any] = kwargs
        self.metrics: Dict[str, float] = dict()
        self.entries: Dict[str, float] = dict()
        self._current_key: Optional[str] = None
        self.start_time = time.perf_counter()

    def __call__(self, key: str) -> 'Timer':
        self._current_key = key
        return self

    def __enter__(self) -> 'Timer':
        if self._current_key is None:
            raise ValueError(
                "No key specified. Use timer('key_name') as context manager.",
            )
        self.entries[self._current_key] = time.perf_counter()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        key = self._current_key
        if key and key in self.entries:
            start = self.entries.pop(key)
            elapsed = time.perf_counter() - start
            self.metrics[key] = elapsed
        self._current_key = None

    async def __aenter__(self) -> 'Timer':
        return self.__enter__()

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.__exit__(exc_type, exc_val, exc_tb)

    def reset(self) -> None:
        self.metrics.clear()
        self.entries.clear()
        self._current_key = None

    def finish(self) -> None:
        """Finish the current timing context and store the elapsed time."""
        self.metrics['total'] = time.perf_counter() - self.start_time
        self.log_metrics()

    def log_metrics(self) -> None:
        if self.metadata.get('function'):
            result = dict(type='function_metrics', **self.metadata, **self.metrics)
            logger.info(json.dumps(result, cls=RoundedFloatEncoder))
