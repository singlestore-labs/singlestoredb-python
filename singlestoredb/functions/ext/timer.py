import json
import time
from typing import Any
from typing import Dict
from typing import List

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
        """
        Initialize the Timer.

        Parameters
        ----------
        metrics : Dict[str, float]
            Dictionary to store the timing results

        """
        self.metadata: Dict[str, Any] = kwargs
        self.metrics: Dict[str, float] = dict()
        self._stack: List[Dict[str, Any]] = []
        self.start_time = time.perf_counter()

    def __call__(self, key: str) -> 'Timer':
        """
        Set the key for the next context manager usage.

        Parameters
        ----------
        key : str
            The key to store the execution time under

        Returns
        -------
        Timer
            Self, to be used as context manager

        """
        self._current_key = key
        return self

    def __enter__(self) -> 'Timer':
        """Enter the context manager and start timing."""
        if not hasattr(self, '_current_key'):
            raise ValueError(
                "No key specified. Use timer('key_name') as context manager.",
            )

        # Push current timing info onto stack
        timing_info = {
            'key': self._current_key,
            'start_time': time.perf_counter(),
        }
        self._stack.append(timing_info)

        # Clear current key for next use
        delattr(self, '_current_key')

        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit the context manager and store the elapsed time."""
        if not self._stack:
            return

        # Pop the current timing from stack
        timing_info = self._stack.pop()
        elapsed = time.perf_counter() - timing_info['start_time']
        self.metrics[timing_info['key']] = elapsed

    def finish(self) -> None:
        """Finish the current timing context and store the elapsed time."""
        if self._stack:
            raise RuntimeError(
                'finish() called without a matching __enter__(). '
                'Use the context manager instead.',
            )

        self.metrics['total'] = time.perf_counter() - self.start_time

        self.log_metrics()

    def reset(self) -> None:
        """Clear all stored times and reset the stack."""
        self.metrics.clear()
        self._stack.clear()

    def log_metrics(self) -> None:
        if self.metadata.get('function'):
            result = dict(type='function_metrics', **self.metadata, **self.metrics)
            logger.info(json.dumps(result, cls=RoundedFloatEncoder))
