"""
Thin WIT adapter over FunctionRegistry.

This module provides the FunctionHandler class that implements the
singlestore:udf/function-handler WIT interface by delegating to the
shared FunctionRegistry in registry.py.
"""
import logging
import traceback

from .registry import _has_accel
from .registry import call_function
from .registry import describe_functions_json
from .registry import FunctionRegistry

logger = logging.getLogger('udf_handler')

# Global registry instance (used by WASM component runtime)
_registry = FunctionRegistry()


class FunctionHandler:
    """Implementation of the singlestore:udf/function-handler interface."""

    def initialize(self) -> None:
        """Initialize and discover UDF functions from loaded modules."""
        if _has_accel:
            logger.info('Using accelerated C call_function_accel loop')
        else:
            logger.info('Using pure Python call_function loop')
        _registry.initialize()

    def call_function(self, name: str, input_data: bytes) -> bytes:
        """Call a function by its registered name."""
        return call_function(_registry, name, input_data)

    def describe_functions(self) -> str:
        """Describe all functions as a JSON array.

        Returns a JSON string containing an array of function
        description objects.
        """
        try:
            return describe_functions_json(_registry)
        except Exception as e:
            tb = traceback.format_exc()
            raise RuntimeError(f'{e}\n{tb}')

    def create_function(
        self,
        signature: str,
        code: str,
        replace: bool,
    ) -> None:
        """Register a function from its signature and Python source code."""
        try:
            _registry.create_function(signature, code, replace)
        except Exception as e:
            tb = traceback.format_exc()
            raise RuntimeError(f'{e}\n{tb}')
