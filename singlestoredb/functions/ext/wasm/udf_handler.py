"""
Python UDF handler implementing the WIT interface for WASM component.

This module provides a Python runtime for UDF functions. When compiled
with componentize-py, it becomes a WASM component that can be loaded by
the Rust UDF server.

Functions are discovered automatically by scanning sys.modules for
@udf-decorated functions. No _exports.py is needed — just import
FunctionHandler from this module in your UDF file and decorate
functions with @udf.
"""
import difflib  # noqa: F401
import inspect
import json
import logging
import os
import sys
import traceback
import types
from typing import Any
from typing import Callable
from typing import Dict
from typing import List


# Install numpy stub before importing singlestoredb (which tries to import numpy)
if 'numpy' not in sys.modules:
    try:
        import numpy  # noqa: F401
    except ImportError:
        from . import numpy_stub
        sys.modules['numpy'] = numpy_stub

from singlestoredb.functions.signature import get_signature
from singlestoredb.functions.ext.rowdat_1 import load as _load_rowdat_1
from singlestoredb.functions.ext.rowdat_1 import dump as _dump_rowdat_1
from singlestoredb.mysql.constants import FIELD_TYPE as ft

try:
    from _singlestoredb_accel import call_function_accel as _call_function_accel
    _has_call_accel = True
except Exception:
    _has_call_accel = False


class _TracingFormatter(logging.Formatter):
    """Match Rust tracing-subscriber's colored output format."""

    _RESET = '\033[0m'
    _DIM = '\033[2m'
    _BOLD = '\033[1m'
    _LEVEL_COLORS = {
        'DEBUG': '\033[34m',    # blue
        'INFO': '\033[32m',     # green
        'WARNING': '\033[33m',  # yellow
        'ERROR': '\033[31m',    # red
        'CRITICAL': '\033[31m',  # red
    }

    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        from datetime import datetime, timezone
        dt = datetime.fromtimestamp(record.created, tz=timezone.utc)
        return dt.strftime('%Y-%m-%dT%H:%M:%S.') + f'{dt.microsecond:06d}Z'

    def format(self, record: logging.LogRecord) -> str:
        ts = self.formatTime(record)
        color = self._LEVEL_COLORS.get(record.levelname, '')
        level = f'{color}{self._BOLD}{record.levelname:>5}{self._RESET}'
        name = f'{self._DIM}{record.name}{self._RESET}'
        msg = record.getMessage()
        return f'{self._DIM}{ts}{self._RESET} {level} {name}: {msg}'


_handler = logging.StreamHandler()
_handler.setFormatter(_TracingFormatter())
logging.basicConfig(level=logging.INFO, handlers=[_handler])
logger = logging.getLogger('udf_handler')

# Map dtype strings to rowdat_1 type codes for wire serialization.
# rowdat_1 always uses 8-byte encoding for integers and doubles for floats,
# so all int types collapse to LONGLONG and all float types to DOUBLE.
# Uses negative values for unsigned ints / binary data.
rowdat_1_type_map = {
    'bool': ft.LONGLONG,
    'int8': ft.LONGLONG,
    'int16': ft.LONGLONG,
    'int32': ft.LONGLONG,
    'int64': ft.LONGLONG,
    'uint8': -ft.LONGLONG,
    'uint16': -ft.LONGLONG,
    'uint32': -ft.LONGLONG,
    'uint64': -ft.LONGLONG,
    'float32': ft.DOUBLE,
    'float64': ft.DOUBLE,
    'str': ft.STRING,
    'bytes': -ft.STRING,
}


class FunctionRegistry:
    """Registry of discovered UDF functions."""

    def __init__(self) -> None:
        self.functions: Dict[str, Dict[str, Any]] = {}

    def initialize(self) -> None:
        """Initialize and discover UDF functions from loaded modules.

        Scans sys.modules for any module containing @udf-decorated
        functions. No _exports.py is needed — modules just need to be
        imported before initialize() is called (componentize-py captures
        them at build time).
        """
        self._discover_udf_functions()

    @staticmethod
    def _is_stdlib_or_infra(mod_name: str, mod_file: str) -> bool:
        """Check if a module is stdlib or infrastructure (not user UDF code).

        Uses the module's __file__ path to detect stdlib modules
        (under sys.prefix but not in site-packages) rather than
        maintaining a hardcoded list of names.
        """
        # Infrastructure modules that are part of this project
        _infra = frozenset({
            'udf_handler', 'numpy_stub',
        })
        if mod_name in _infra:
            return True

        # Resolve symlinks for reliable prefix comparison
        real_file = os.path.realpath(mod_file)
        real_prefix = os.path.realpath(sys.prefix)

        # Modules under sys.prefix but NOT in site-packages are stdlib
        if real_file.startswith(real_prefix + os.sep):
            if 'site-packages' not in real_file:
                return True

        return False

    def _discover_udf_functions(self) -> None:
        """Discover @udf functions by scanning sys.modules.

        Uses a two-pass approach: first, identify candidate modules
        that import FunctionHandler (the convention for UDF modules).
        Then extract @udf-decorated functions from those modules.
        Modules without a __file__ (built-in/frozen) and stdlib/
        infrastructure modules are skipped automatically.
        """
        found_modules = []
        for mod_name, mod in list(sys.modules.items()):
            if mod is None:
                continue
            if not isinstance(mod, types.ModuleType):
                continue
            mod_file = getattr(mod, '__file__', None)
            if mod_file is None:
                continue

            # Short-circuit: only scan modules that import
            # FunctionHandler (the convention for UDF modules)
            if not any(
                obj is FunctionHandler
                for obj in vars(mod).values()
            ):
                continue

            # Skip stdlib and infrastructure modules
            if self._is_stdlib_or_infra(mod_name, mod_file):
                continue

            self._extract_functions(mod)
            if any(
                hasattr(obj, '_singlestoredb_attrs')
                for _, obj in inspect.getmembers(mod)
                if inspect.isfunction(obj)
            ):
                found_modules.append(mod_name)

        if found_modules:
            logger.info(
                f'Discovered UDF functions from modules: '
                f'{", ".join(sorted(found_modules))}',
            )
        else:
            logger.warning(
                'No modules with @udf functions found in sys.modules.',
            )

    def _extract_functions(self, module: Any) -> None:
        """Extract @udf-decorated functions from a module.

        Unlike module scanning, this does not filter by __module__ —
        _exports.py may re-export functions defined in other modules.
        """
        for name, obj in inspect.getmembers(module):
            if name.startswith('_'):
                continue

            if not callable(obj):
                continue

            if not inspect.isfunction(obj):
                continue

            # Only register functions decorated with @udf
            if not hasattr(obj, '_singlestoredb_attrs'):
                continue

            try:
                sig = get_signature(obj)
                if sig and sig.get('args') is not None and sig.get('returns'):
                    self._register_function(obj, name, sig)
            except (TypeError, ValueError):
                # Skip functions that can't be introspected
                pass

    def _build_json_descriptions(
        self,
        func_names: List[str],
    ) -> List[Dict[str, Any]]:
        """Build JSON-serializable descriptions for the given function names.

        Extracts metadata from the stored signature dict for each function.
        """
        descriptions = []
        for func_name in func_names:
            func_info = self.functions[func_name]
            sig = func_info['signature']
            args = []
            for arg in sig['args']:
                args.append({
                    'name': arg['name'],
                    'dtype': arg['dtype'],
                    'sql': arg['sql'],
                })
            returns = []
            for ret in sig['returns']:
                returns.append({
                    'name': ret.get('name') or None,
                    'dtype': ret['dtype'],
                    'sql': ret['sql'],
                })
            descriptions.append({
                'name': func_name,
                'args': args,
                'returns': returns,
                'args_data_format': sig.get('args_data_format') or 'scalar',
                'returns_data_format': (
                    sig.get('returns_data_format') or 'scalar'
                ),
                'function_type': sig.get('function_type') or 'udf',
                'doc': sig.get('doc'),
            })
        return descriptions

    def create_function(
        self,
        signature_json: str,
        code: str,
        replace: bool,
    ) -> List[str]:
        """Register a function from its signature and Python source code.

        Args:
            signature_json: JSON object matching the describe-functions
                element schema (must contain a 'name' field)
            code: Python source code containing the @udf-decorated function
            replace: If False, raise an error if the function already exists

        Returns:
            List of newly registered function names

        Raises:
            SyntaxError: If the code has syntax errors
            ValueError: If no @udf-decorated functions are found or
                function already exists and replace is False
        """
        sig = json.loads(signature_json)
        func_name = sig.get('name')
        if not func_name:
            raise ValueError(
                'signature JSON must contain a "name" field',
            )

        # Check for name collision when replace is False
        if not replace and func_name in self.functions:
            raise ValueError(
                f'Function "{func_name}" already exists '
                f'(use replace=true to overwrite)',
            )

        # When replacing, remove the old entry so the new registration
        # is detected as "new" by the before/after name comparison.
        if replace and func_name in self.functions:
            del self.functions[func_name]

        # Use __main__ as the module name for dynamically submitted functions
        name = '__main__'

        # Validate syntax
        compiled = compile(code, f'<{name}>', 'exec')

        # Reuse existing module to avoid corrupting the componentize-py
        # runtime state (replacing sys.modules['__main__'] traps WASM).
        if name in sys.modules:
            module = sys.modules[name]
        else:
            module = types.ModuleType(name)
            module.__file__ = f'<{name}>'
            sys.modules[name] = module
        exec(compiled, module.__dict__)  # noqa: S102

        # Extract functions from the module
        before_names = set(self.functions.keys())
        self._extract_functions(module)
        new_names = [k for k in self.functions if k not in before_names]

        if not new_names:
            raise ValueError(
                'No @udf-decorated functions found in submitted code',
            )

        logger.info(
            f'create_function({func_name}): registered '
            f'{len(new_names)} function(s): {", ".join(new_names)}',
        )
        return new_names

    def _register_function(
        self,
        func: Callable[..., Any],
        func_name: str,
        sig: Dict[str, Any],
    ) -> None:
        """Register a function under its bare name.

        All functions are registered as top-level names (no module prefix).
        If a function with the same name already exists, the last
        registration wins.
        """
        # Use alias name from signature if available, otherwise use function name
        full_name = sig.get('name') or func_name

        # Convert args to (name, type_code) tuples
        arg_types = []
        for arg in sig['args']:
            dtype = arg['dtype'].replace('?', '')
            if dtype not in rowdat_1_type_map:
                logger.warning(f"Skipping {full_name}: unsupported arg dtype '{dtype}'")
                return
            arg_types.append((arg['name'], rowdat_1_type_map[dtype]))

        # Convert returns to type_code list
        return_types = []
        for ret in sig['returns']:
            dtype = ret['dtype'].replace('?', '')
            if dtype not in rowdat_1_type_map:
                logger.warning(f'Skipping {full_name}: no type mapping for {dtype}')
                return
            return_types.append(rowdat_1_type_map[dtype])

        self.functions[full_name] = {
            'func': func,
            'arg_types': arg_types,
            'return_types': return_types,
            'signature': sig,
        }


# Global registry instance
_registry = FunctionRegistry()


class FunctionHandler:
    """Implementation of the singlestore:udf/function-handler interface."""

    def initialize(self) -> None:
        """Initialize and discover UDF functions from loaded modules."""
        if _has_call_accel:
            logger.info('Using accelerated C call_function_accel loop')
        else:
            logger.info('Using pure Python call_function loop')
        _registry.initialize()

    def call_function(self, name: str, input_data: bytes) -> bytes:
        """Call a function by its registered name."""
        if name not in _registry.functions:
            raise ValueError(f'unknown function: {name}')

        func_info = _registry.functions[name]
        func = func_info['func']
        arg_types = func_info['arg_types']
        return_types = func_info['return_types']

        try:
            if _has_call_accel:
                return _call_function_accel(
                    colspec=arg_types,
                    returns=return_types,
                    data=input_data,
                    func=func,
                )

            # Fallback to pure Python
            row_ids, rows = _load_rowdat_1(arg_types, input_data)
            results = []
            for row in rows:
                result = func(*row)
                if not isinstance(result, tuple):
                    result = [result]
                results.append(list(result))
            return bytes(_dump_rowdat_1(return_types, row_ids, results))

        except Exception as e:
            tb = traceback.format_exc()
            raise RuntimeError(f'Error calling {name}: {e}\n{tb}')

    def describe_functions(self) -> str:
        """Describe all functions as a JSON array.

        Returns a JSON string containing an array of function description
        objects with: name, args, returns, args_data_format,
        returns_data_format, function_type, doc.

        Raises RuntimeError on failure (mapped to result Err by
        componentize-py).
        """
        try:
            func_names = list(_registry.functions.keys())
            descriptions = _registry._build_json_descriptions(func_names)
            return json.dumps(descriptions)
        except Exception as e:
            tb = traceback.format_exc()
            raise RuntimeError(f'{e}\n{tb}')

    def create_function(
        self,
        signature: str,
        code: str,
        replace: bool,
    ) -> None:
        """Register a function from its signature and Python source code.

        Returns None on success (mapped to result Ok(()) by componentize-py).
        Raises RuntimeError on failure (mapped to result Err by
        componentize-py).
        """
        try:
            _registry.create_function(signature, code, replace)
        except Exception as e:
            tb = traceback.format_exc()
            raise RuntimeError(f'{e}\n{tb}')
