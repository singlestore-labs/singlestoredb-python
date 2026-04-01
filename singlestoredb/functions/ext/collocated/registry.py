"""
Function registry for UDF discovery, registration, and invocation.

This module contains the core FunctionRegistry class (moved from
wasm/udf_handler.py) plus standalone call_function() and
describe_functions_json() helpers. Both the WASM handler and the
collocated server use these directly.
"""
import inspect
import json
import logging
import os
import sys
import traceback
import types
from datetime import datetime
from datetime import timezone
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

from singlestoredb.functions.ext.rowdat_1 import dump as _dump_rowdat_1
from singlestoredb.functions.ext.rowdat_1 import load as _load_rowdat_1
from singlestoredb.functions.signature import get_signature
from singlestoredb.mysql.constants import FIELD_TYPE as ft

_accel_error: Optional[str] = None
try:
    from _singlestoredb_accel import call_function_accel as _call_function_accel
    from _singlestoredb_accel import mmap_read as _mmap_read
    from _singlestoredb_accel import mmap_write as _mmap_write
    from _singlestoredb_accel import recv_exact as _recv_exact
    _has_accel = True
    logging.getLogger(__name__).info('_singlestoredb_accel loaded successfully')
except Exception as e:
    _has_accel = False
    _accel_error = str(e)
    _mmap_read = None
    _mmap_write = None
    _recv_exact = None
    logging.getLogger(__name__).warning(
        '_singlestoredb_accel failed to load: %s', e,
    )


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

    def formatTime(
        self,
        record: logging.LogRecord,
        datefmt: Optional[str] = None,
    ) -> str:
        dt = datetime.fromtimestamp(record.created, tz=timezone.utc)
        return dt.strftime('%Y-%m-%dT%H:%M:%S.') + f'{dt.microsecond:06d}Z'

    def format(self, record: logging.LogRecord) -> str:
        ts = self.formatTime(record)
        color = self._LEVEL_COLORS.get(record.levelname, '')
        level = f'{color}{self._BOLD}{record.levelname:>5}{self._RESET}'
        name = f'{self._DIM}{record.name}{self._RESET}'
        msg = record.getMessage()
        return f'{self._DIM}{ts}{self._RESET} {level} {name}: {msg}'


def setup_logging(level: int = logging.INFO) -> None:
    """Configure root logging with the tracing formatter."""
    handler = logging.StreamHandler()
    handler.setFormatter(_TracingFormatter())
    logging.basicConfig(level=level, handlers=[handler])


# Map dtype strings to rowdat_1 type codes for wire serialization.
# rowdat_1 always uses 8-byte encoding for integers and doubles for floats,
# so all int types collapse to LONGLONG and all float types to DOUBLE.
# Uses negative values for unsigned ints / binary data.
rowdat_1_type_map: Dict[str, int] = {
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

# Map dtype strings to Python type annotation strings for code generation.
_dtype_to_python: Dict[str, str] = {
    'bool': 'bool',
    'int8': 'int',
    'int16': 'int',
    'int32': 'int',
    'int64': 'int',
    'int': 'int',
    'uint8': 'int',
    'uint16': 'int',
    'uint32': 'int',
    'uint64': 'int',
    'float32': 'float',
    'float64': 'float',
    'float': 'float',
    'str': 'str',
    'bytes': 'bytes',
}

logger = logging.getLogger('udf_handler')


class FunctionRegistry:
    """Registry of discovered UDF functions."""

    def __init__(self) -> None:
        self.functions: Dict[str, Dict[str, Any]] = {}

    def initialize(self) -> None:
        """Initialize and discover UDF functions from loaded modules.

        Scans sys.modules for any module containing @udf-decorated
        functions. No _exports.py is needed -- modules just need to be
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
        _infra = frozenset({
            'udf_handler',
        })
        if mod_name in _infra:
            return True

        real_file = os.path.realpath(mod_file)
        real_prefix = os.path.realpath(sys.prefix)

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
        # Import here to avoid circular dependency at module level
        from .wasm import FunctionHandler

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
        """Extract @udf-decorated functions from a module."""
        for name, obj in inspect.getmembers(module):
            if name.startswith('_'):
                continue

            if not callable(obj):
                continue

            if not inspect.isfunction(obj):
                continue

            if not hasattr(obj, '_singlestoredb_attrs'):
                continue

            try:
                sig = get_signature(obj)
                if sig and sig.get('args') is not None and sig.get('returns'):
                    self._register_function(obj, name, sig)
            except (TypeError, ValueError):
                pass

    def _build_json_descriptions(
        self,
        func_names: List[str],
    ) -> List[Dict[str, Any]]:
        """Build JSON-serializable descriptions for the given function names."""
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

    @staticmethod
    def _python_type_annotation(dtype: str) -> str:
        """Convert a dtype string to a Python type annotation."""
        nullable = dtype.endswith('?')
        base = dtype.rstrip('?')
        py_type = _dtype_to_python.get(base)
        if py_type is None:
            raise ValueError(f'Unsupported dtype: {dtype!r}')
        if nullable:
            return f'Optional[{py_type}]'
        return py_type

    @staticmethod
    def _build_python_code(
        sig: Dict[str, Any],
        body: str,
    ) -> str:
        """Build a complete @udf-decorated Python function from sig + body."""
        func_name = sig['name']
        args = sig.get('args', [])
        returns = sig.get('returns', [])

        params = []
        for arg in args:
            ann = FunctionRegistry._python_type_annotation(arg['dtype'])
            params.append(f'{arg["name"]}: {ann}')
        params_str = ', '.join(params)

        if len(returns) == 0:
            ret_ann = 'None'
        elif len(returns) == 1:
            ret_ann = FunctionRegistry._python_type_annotation(
                returns[0]['dtype'],
            )
        else:
            parts = [
                FunctionRegistry._python_type_annotation(r['dtype'])
                for r in returns
            ]
            ret_ann = f'Tuple[{", ".join(parts)}]'

        indented_body = '\n'.join(
            f'    {line}' for line in body.splitlines()
        )

        return (
            'from singlestoredb.functions import udf\n'
            'from typing import Optional, Tuple\n'
            '\n'
            '@udf\n'
            f'def {func_name}({params_str}) -> {ret_ann}:\n'
            f'{indented_body}\n'
        )

    def create_function(
        self,
        signature_json: str,
        code: str,
        replace: bool,
    ) -> List[str]:
        """Register a function from its signature and function body.

        Args:
            signature_json: JSON object matching the describe-functions
                element schema (must contain a 'name' field)
            code: Function body (e.g. "return x * 3"), not full source
            replace: If False, raise an error if the function already exists

        Returns:
            List of newly registered function names
        """
        sig = json.loads(signature_json)
        func_name = sig.get('name')
        if not func_name:
            raise ValueError(
                'signature JSON must contain a "name" field',
            )

        if not replace and func_name in self.functions:
            raise ValueError(
                f'Function "{func_name}" already exists '
                f'(use replace=true to overwrite)',
            )

        if replace and func_name in self.functions:
            del self.functions[func_name]

        full_code = self._build_python_code(sig, code)

        name = '__main__'
        compiled = compile(full_code, f'<{name}>', 'exec')

        if name in sys.modules:
            module = sys.modules[name]
        else:
            module = types.ModuleType(name)
            module.__file__ = f'<{name}>'
            sys.modules[name] = module
        exec(compiled, module.__dict__)  # noqa: S102

        before_names = set(self.functions.keys())
        self._extract_functions(module)
        new_names = [k for k in self.functions if k not in before_names]

        if not new_names:
            raise ValueError(
                f'Function "{func_name}" was not registered. '
                f'Check that the signature dtypes are supported.',
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
        """Register a function under its bare name."""
        full_name = sig.get('name') or func_name

        arg_types: List[Tuple[str, int]] = []
        for arg in sig['args']:
            dtype = arg['dtype'].replace('?', '')
            if dtype not in rowdat_1_type_map:
                logger.warning(
                    f"Skipping {full_name}: unsupported arg dtype '{dtype}'",
                )
                return
            arg_types.append((arg['name'], rowdat_1_type_map[dtype]))

        return_types: List[int] = []
        for ret in sig['returns']:
            dtype = ret['dtype'].replace('?', '')
            if dtype not in rowdat_1_type_map:
                logger.warning(
                    f'Skipping {full_name}: no type mapping for {dtype}',
                )
                return
            return_types.append(rowdat_1_type_map[dtype])

        self.functions[full_name] = {
            'func': func,
            'arg_types': arg_types,
            'return_types': return_types,
            'signature': sig,
        }


def call_function(
    registry: FunctionRegistry,
    name: str,
    input_data: bytes,
) -> bytes:
    """Call a registered UDF by name using the C accelerator or fallback.

    This is the hot-path function used by both the WASM handler and
    the collocated server.
    """
    if name not in registry.functions:
        raise ValueError(f'unknown function: {name}')

    func_info = registry.functions[name]
    func = func_info['func']
    arg_types = func_info['arg_types']
    return_types = func_info['return_types']

    try:
        if _has_accel:
            return _call_function_accel(
                colspec=arg_types,
                returns=return_types,
                data=input_data,
                func=func,
            )

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


def describe_functions_json(registry: FunctionRegistry) -> str:
    """Serialize all function descriptions as a JSON array string."""
    func_names = list(registry.functions.keys())
    descriptions = registry._build_json_descriptions(func_names)
    return json.dumps(descriptions)
