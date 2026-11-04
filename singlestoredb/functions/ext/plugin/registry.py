"""
Function registry for UDF discovery, registration, and invocation.

This module contains the core FunctionRegistry class (moved from
wasm/udf_handler.py) plus standalone call_function() and
describe_functions_json() helpers. Both the WASM handler and the
plugin server use these directly.
"""
import builtins
import datetime as _datetime
import decimal as _decimal
import inspect
import itertools
import json
import logging
import os
import sys
import traceback
import types
import typing
import weakref
from datetime import datetime
from datetime import timezone
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

from . import overload as _overload
from singlestoredb.functions import udf
from singlestoredb.functions.ext import rowdat_1 as _rowdat_1
from singlestoredb.functions.ext.rowdat_1 import dump as _dump_rowdat_1
from singlestoredb.functions.ext.rowdat_1 import load as _load_rowdat_1
from singlestoredb.functions.signature import get_signature
from singlestoredb.functions.typing import Masked
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
    'datetime': ft.DATETIME,
    'datetime6': ft.DATETIME,
    'date': ft.DATE,
    'time': ft.TIME,
    'time6': ft.TIME,
    'decimal': ft.NEWDECIMAL,
}

# Map dtype strings to the Python types used to annotate dynamic functions.
_dtype_to_python: Dict[str, Any] = {
    'bool': bool,
    'int8': int,
    'int16': int,
    'int32': int,
    'int64': int,
    'int': int,
    'uint8': int,
    'uint16': int,
    'uint32': int,
    'uint64': int,
    'float32': float,
    'float64': float,
    'float': float,
    'str': str,
    'bytes': bytes,
    'datetime': _datetime.datetime,
    'datetime6': _datetime.datetime,
    'date': _datetime.date,
    'time': _datetime.timedelta,
    'time6': _datetime.timedelta,
    'decimal': _decimal.Decimal,
}

# Names every dynamic function body can use without importing them. Bound
# once per registry; a dynamic function of the same name replaces its entry.
_DYNAMIC_PRELUDE: Dict[str, Any] = {
    'datetime': _datetime,
    'decimal': _decimal,
    'Decimal': _decimal.Decimal,
    'udf': udf,
    'Optional': Optional,
    'Tuple': Tuple,
}

logger = logging.getLogger('udf_handler')

_DYNAMIC_MODULE_NAME = 'singlestoredb.functions.ext.plugin._dynamic'

# Suffixes that give each registry's dynamic module a unique name.
_dynamic_module_ids = itertools.count()


def _dynamic_factory_name(func_name: str) -> str:
    """Name of the factory that defines a dynamic function's variant."""
    return f'__s2_make_{func_name}'


def _dynamic_binder_name(func_name: str) -> str:
    """Name of the factory parameter that decorates a dynamic function."""
    return f'__s2_bind_{func_name}'


def _restore_declared_sql_types(
    compiled_sig: Dict[str, Any],
    declared_sig: Dict[str, Any],
) -> None:
    """Copy the caller's SQL types over the ones re-derived from Python.

    A dynamically created function is compiled from a Python annotation, so
    its signature comes back with whatever SQL type that annotation maps to:
    a declared ``INT`` returns as ``BIGINT``, ``VARCHAR(50)`` as ``TEXT``,
    ``FLOAT`` as ``DOUBLE``. The declaration is the authority — it decides
    which overload slot the variant occupies and it is what we must report
    from ``describe_functions``. Modifies ``compiled_sig`` in place, leaving
    dtypes (which drive the rowdat_1 encoding) alone.
    """
    for key in ('args', 'returns'):
        compiled = compiled_sig.get(key) or []
        declared = declared_sig.get(key) or []
        if len(compiled) != len(declared):
            continue
        for c, d in zip(compiled, declared):
            sql = d.get('sql') if isinstance(d, dict) else None
            if isinstance(sql, str) and sql.strip():
                c['sql'] = sql


class FunctionRegistry:
    """Registry of discovered UDF functions.

    A name may map to multiple variants when SingleStoreDB-style overloads
    are registered (same name, different parameter signatures). Each entry
    in ``self.functions`` is therefore a *list* of variant dicts. Use
    ``lookup_variant(name, param_types)`` to pick the right one for a call.
    """

    def __init__(self) -> None:
        # name -> list of variant dicts. Each variant carries `func`,
        # `arg_types`, `return_types`, `signature`, and `param_sql_types`
        # (normalized SQL keys used for overload resolution).
        self.functions: Dict[str, List[Dict[str, Any]]] = {}
        self._base_function_names: set[str] = set()
        # Maps server-supplied id -> (name, signature_key) for the variant
        # this id registered. Consulted by delete_function.
        self._id_to_variant: Dict[str, Tuple[str, str]] = {}
        # Globals shared by every dynamic function in this registry, so a
        # body can call another dynamic function by name. Each name is bound
        # to its most recently registered variant. They are the namespace of
        # a module of the registry's own, so each SharedRegistry replay
        # starts clean. The module is in sys.modules, because code such as
        # dataclasses looks a class's __module__ up there, and leaves it
        # when the registry is collected.
        module_name = f'{_DYNAMIC_MODULE_NAME}_{next(_dynamic_module_ids)}'
        module = types.ModuleType(module_name)
        module.__dict__.update({
            '__file__': f'<{_DYNAMIC_MODULE_NAME}>',
            '__builtins__': builtins,
            **_DYNAMIC_PRELUDE,
        })
        sys.modules[module_name] = module
        weakref.finalize(self, sys.modules.pop, module_name, None)
        self._dynamic_globals: Dict[str, Any] = module.__dict__

    def initialize(self, plugin_module: Any = None) -> None:
        """Initialize and discover UDF functions from loaded modules.

        If plugin_module is provided, only that module is scanned.
        Otherwise scans sys.modules for any module containing @udf-decorated
        functions. No _exports.py is needed -- modules just need to be
        imported before initialize() is called (componentize-py captures
        them at build time).
        """
        if plugin_module is not None:
            self._extract_functions(plugin_module)
            if self.functions:
                logger.info(
                    f'Discovered UDF functions from module: '
                    f'{plugin_module.__name__}',
                )
            else:
                logger.warning(
                    f'No @udf functions found in module: '
                    f'{plugin_module.__name__}',
                )
        else:
            self._discover_udf_functions()

        self._base_function_names = set(self.functions.keys())

    @staticmethod
    def _is_stdlib_or_infra(mod_name: str, mod_file: str) -> bool:
        """Check if a module is stdlib or infrastructure (not user UDF code).

        Uses the module's __file__ path to detect stdlib modules
        (under sys.prefix but not in site-packages) rather than
        maintaining a hardcoded list of names.
        """
        _infra = frozenset({
            'udf_handler',
            'singlestoredb',
        })
        if mod_name in _infra or mod_name.startswith('singlestoredb.'):
            return True

        real_file = os.path.realpath(mod_file)
        real_prefix = os.path.realpath(sys.prefix)

        if real_file.startswith(real_prefix + os.sep):
            if 'site-packages' not in real_file:
                return True

        return False

    def _discover_udf_functions(self) -> None:
        """Discover @udf functions by scanning sys.modules.

        Scans all non-stdlib, non-infrastructure modules for objects
        bearing the ``_singlestoredb_attrs`` marker set by the ``@udf``
        decorator, then extracts and registers matching functions.
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

            if self._is_stdlib_or_infra(mod_name, mod_file):
                continue

            def _has_udf_marker(obj: object) -> bool:
                try:
                    return hasattr(obj, '_singlestoredb_attrs')
                except (TypeError, Exception):
                    return False

            try:
                mod_vars = list(vars(mod).values())
            except RuntimeError:
                continue

            if not any(_has_udf_marker(obj) for obj in mod_vars):
                continue

            self._extract_functions(mod)
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
            except (TypeError, ValueError) as exc:
                # ValueError here covers duplicate-variant detection, so a
                # second @udf with the same normalized signature is dropped
                # with a warning rather than killing initialization.
                logger.warning(
                    f'Skipping {name}: {exc}',
                )

    def _build_json_descriptions(
        self,
        func_names: List[str],
    ) -> List[Dict[str, Any]]:
        """Build JSON-serializable descriptions for the given function names.

        Emits one entry per variant — overloaded names appear multiple times
        in the returned list, each with its own argument/return signature.
        """
        descriptions = []
        for func_name in func_names:
            for variant in self.functions[func_name]:
                sig = variant['signature']
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
                    'args_data_format': (
                        sig.get('args_data_format') or 'scalar'
                    ),
                    'returns_data_format': (
                        sig.get('returns_data_format') or 'scalar'
                    ),
                    'function_type': sig.get('function_type') or 'udf',
                    'doc': sig.get('doc'),
                })
        return descriptions

    @staticmethod
    def _python_type_annotation(dtype: str) -> Any:
        """Convert a dtype string to a Python type annotation."""
        nullable = dtype.endswith('?')
        base = dtype.rstrip('?')
        py_type = _dtype_to_python.get(base)
        if py_type is None:
            raise ValueError(f'Unsupported dtype: {dtype!r}')
        if nullable:
            return Optional[py_type]
        return py_type

    @staticmethod
    def _build_annotations(sig: Dict[str, Any]) -> Dict[str, Any]:
        """Build the ``__annotations__`` of a dynamic function from sig."""
        annotations = {
            arg['name']: FunctionRegistry._python_type_annotation(arg['dtype'])
            for arg in sig.get('args', [])
        }
        returns = sig.get('returns', [])
        if len(returns) == 0:
            annotations['return'] = None
        elif len(returns) == 1:
            annotations['return'] = FunctionRegistry._python_type_annotation(
                returns[0]['dtype'],
            )
        else:
            parts = tuple(
                FunctionRegistry._python_type_annotation(r['dtype'])
                for r in returns
            )
            annotations['return'] = Tuple[parts]
        return annotations

    @staticmethod
    def _build_python_code(
        sig: Dict[str, Any],
        body: str,
    ) -> str:
        """Build the source of a factory that defines a function from body.

        The factory, ``_dynamic_factory_name``, takes a binder that
        annotates and ``@udf``-decorates the function, and returns the
        result. Overloads share a bare name, so if the body looked its own
        name up in the shared globals, a recursive ``f(n)`` would call
        whichever variant of ``f`` was registered last. Defined inside the
        factory, the name is a closure variable, rebound to the decorated
        variant; every other name still resolves through the globals at
        call time.

        The source names nothing but the function, its parameters, and the
        binder, whose name is longer than the function's. A function named
        like the decorator or an annotation type (``udf``, ``int``) would
        otherwise be read in the factory before its own ``def`` binds it.
        The binder's name is also lengthened, doubling the added
        underscores each time, until the body does not contain it, so a
        body calling a dynamic function that happens to share it reaches
        that function rather than the enclosing binder.
        """
        func_name = sig['name']
        params_str = ', '.join(arg['name'] for arg in sig.get('args', []))
        binder = _dynamic_binder_name(func_name)
        suffix = '_'
        while binder in body:
            binder += suffix
            suffix += suffix

        indented_body = '\n'.join(
            f'        {line}' for line in body.splitlines()
        )

        return (
            f'def {_dynamic_factory_name(func_name)}({binder}):\n'
            f'    def {func_name}({params_str}):\n'
            f'{indented_body}\n'
            f'    {func_name} = {binder}({func_name})\n'
            f'    return {func_name}\n'
        )

    def create_function(
        self,
        id: str,
        signature_json: str,
        code: str,
        replace: bool,
    ) -> List[str]:
        """Register a function variant from its signature and body.

        Multiple variants may share a name as long as their normalized
        parameter signatures differ — see :mod:`overload`. ``replace=True``
        swaps the *matching* variant (same name and signature key); it does
        not wipe out other variants registered under the same name.

        Args:
            id: Server-supplied identifier recorded so the variant can
                later be removed via delete_function
            signature_json: JSON object matching the describe-functions
                element schema (must contain a 'name' field)
            code: Function body (e.g. "return x * 3"), not full source
            replace: If False, raise an error if a variant with the same
                signature already exists

        Returns:
            List of newly registered function names (always a single
            element today, but kept as a list for backwards compatibility).
        """
        sig = json.loads(signature_json)
        func_name = sig.get('name')
        if not func_name:
            raise ValueError(
                'signature JSON must contain a "name" field',
            )
        if func_name.startswith('__') and func_name.endswith('__'):
            # The dynamic globals are a module namespace; a dunder name would
            # replace one of its attributes, e.g. __builtins__ for every
            # function compiled after it.
            raise ValueError(f'Cannot register {func_name!r}: reserved name')

        # Compute the canonical signature key from the incoming sig so we
        # can detect collisions before compiling.
        try:
            incoming_sql_types = [
                _overload.normalize_sql_type(a['sql']) for a in sig.get('args', [])
            ]
            incoming_key = _overload.signature_key(incoming_sql_types)
        except KeyError as e:
            raise ValueError(
                f'signature arg missing "sql" field: {e}',
            )

        if func_name in self._base_function_names:
            # Built-in @udf functions are immutable from the wire.
            raise ValueError(
                f"Cannot replace '{func_name}': "
                f'not a dynamically registered function',
            )

        existing_variants = self.functions.get(func_name, [])
        collision = next(
            (
                v for v in existing_variants
                if _overload.signature_key(v['param_sql_types']) == incoming_key
            ),
            None,
        )
        if collision is not None and not replace:
            raise ValueError(
                f'Function "{func_name}({incoming_key})" already exists '
                f'(use replace=true to overwrite)',
            )

        full_code = self._build_python_code(sig, code)

        compiled = compile(full_code, f'<{_DYNAMIC_MODULE_NAME}>', 'exec')

        namespace = self._dynamic_globals
        factory_name = _dynamic_factory_name(func_name)
        # The factory lands in its own locals, not the shared globals, where
        # it would overwrite a dynamic function of the same name. The
        # functions it defines still take namespace as their globals.
        factory_locals: Dict[str, Any] = {}
        exec(compiled, namespace, factory_locals)  # noqa: S102
        factory = factory_locals.get(factory_name)
        annotations = self._build_annotations(sig)

        def bind(func: Callable[..., Any]) -> Callable[..., Any]:
            # Before udf: it copies __annotations__ onto its wrapper, and
            # get_signature reads them from there.
            func.__annotations__ = annotations
            return udf(func)

        compiled_func = factory(bind) if callable(factory) else None
        if not callable(compiled_func):
            raise ValueError(
                f'Function "{func_name}" was not registered. '
                f'Check that the signature dtypes are supported.',
            )

        try:
            variant_sig = get_signature(compiled_func)
        except Exception as exc:
            raise ValueError(
                f'Failed to read signature of compiled function: {exc}',
            )
        if not variant_sig or variant_sig.get('args') is None:
            raise ValueError(
                f'Compiled function "{func_name}" has no usable signature',
            )

        # Restore the declared SQL types. Going through a Python annotation
        # is lossy — a declared INT becomes `int` becomes BIGINT, VARCHAR(50)
        # becomes `str` becomes TEXT — and the caller's declaration is the
        # authority for both overload dispatch and what we report back from
        # describe_functions.
        _restore_declared_sql_types(variant_sig, sig)

        # Nothing above touched the registry, so a body that fails to
        # compile or bind leaves the variant it would replace in place.
        prior_variants = list(existing_variants)
        prior_ids = dict(self._id_to_variant)
        if collision is not None and replace:
            existing_variants.remove(collision)
            if not existing_variants:
                self.functions.pop(func_name, None)
            else:
                self.functions[func_name] = existing_variants
            # Drop any prior id that pointed at the colliding variant.
            for prior_id, (prior_name, prior_key) in list(
                self._id_to_variant.items(),
            ):
                if prior_name == func_name and prior_key == incoming_key:
                    del self._id_to_variant[prior_id]
            self._rebind_dynamic_name(func_name)

        try:
            self._register_function(compiled_func, func_name, variant_sig)
        except Exception:
            if prior_variants:
                self.functions[func_name] = prior_variants
            else:
                self.functions.pop(func_name, None)
            self._id_to_variant.clear()
            self._id_to_variant.update(prior_ids)
            self._rebind_dynamic_name(func_name)
            raise
        self._id_to_variant[id] = (func_name, incoming_key)
        self._dynamic_globals[func_name] = compiled_func

        logger.info(
            f'create_function({func_name}({incoming_key}), id={id}): '
            f'registered',
        )
        return [func_name]

    def delete_function(self, id: str) -> None:
        """Delete a previously registered variant by its id.

        Args:
            id: Server-supplied identifier previously passed to
                create_function.

        Raises ValueError if the id is unknown.
        """
        entry = self._id_to_variant.pop(id, None)
        if entry is None:
            raise ValueError(f"No registered function with id '{id}'")
        name, sig_key = entry
        if name in self._base_function_names:
            raise ValueError(
                f"Cannot delete '{name}': not a dynamically registered function",
            )
        variants = self.functions.get(name, [])
        variants = [
            v for v in variants
            if _overload.signature_key(v['param_sql_types']) != sig_key
        ]
        if variants:
            self.functions[name] = variants
        else:
            self.functions.pop(name, None)
        self._rebind_dynamic_name(name)
        logger.info(f'delete_function: removed {name}({sig_key}) (id={id})')

    def _rebind_dynamic_name(self, name: str) -> None:
        """Point ``name`` in the dynamic globals at its last variant.

        Called after a variant is removed, so other dynamic functions stop
        calling it. With no variants left, the name is unbound, or bound
        back to the prelude entry it shadowed.
        """
        variants = self.functions.get(name)
        if variants:
            self._dynamic_globals[name] = variants[-1]['func']
        elif name in _DYNAMIC_PRELUDE:
            self._dynamic_globals[name] = _DYNAMIC_PRELUDE[name]
        else:
            self._dynamic_globals.pop(name, None)

    def _register_function(
        self,
        func: Callable[..., Any],
        func_name: str,
        sig: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Register a function variant under its bare name.

        Dispatch keys come from ``sig``'s SQL types, so callers that compile
        a function from a wire definition must restore the declared types
        first (see ``_restore_declared_sql_types``) — the trip through a
        Python annotation turns a declared ``INT`` into ``BIGINT``, which
        would collide the variant with a real ``BIGINT`` sibling.

        Returns the variant dict that was appended. Raises ValueError if a
        variant with an equivalent normalized signature is already present
        (per SingleStoreDB's type-equivalence rules) — callers that want
        replace semantics must remove the colliding variant first.
        """
        full_name = sig.get('name') or func_name

        arg_types: List[Tuple[str, int]] = []
        param_sql_types: List[str] = []
        for arg in sig['args']:
            dtype = arg['dtype'].replace('?', '')
            if dtype not in rowdat_1_type_map:
                raise TypeError(
                    f"unsupported arg dtype '{dtype}' for function '{full_name}'",
                )
            arg_types.append((arg['name'], rowdat_1_type_map[dtype]))
            param_sql_types.append(_overload.normalize_sql_type(arg['sql']))

        return_types: List[int] = []
        for ret in sig['returns']:
            dtype = ret['dtype'].replace('?', '')
            if dtype not in rowdat_1_type_map:
                raise TypeError(
                    f"unsupported return dtype '{dtype}' for function '{full_name}'",
                )
            return_types.append(rowdat_1_type_map[dtype])

        sig_key = _overload.signature_key(param_sql_types)
        existing = self.functions.get(full_name, [])
        for v in existing:
            if _overload.signature_key(v['param_sql_types']) == sig_key:
                raise ValueError(
                    f"function '{full_name}' already has a variant with "
                    f'signature ({sig_key})',
                )

        variant: Dict[str, Any] = {
            'func': func,
            'arg_types': arg_types,
            'return_types': return_types,
            'signature': sig,
            'param_sql_types': param_sql_types,
        }
        existing.append(variant)
        self.functions[full_name] = existing
        return variant

    def lookup_variant(
        self,
        name: str,
        param_types: Optional[str],
    ) -> Dict[str, Any]:
        """Resolve a call to a single variant.

        ``param_types`` is the semicolon-separated SQL parameter type list
        from the wire (v3 envelope ``param_types`` or the v2 segment).

        The engine always sends the list for v2/v3 and uses the *empty
        string* for a zero-arg call, so ``''`` means "zero arguments" — it
        must not be conflated with "no type info". Only ``None`` means no
        type info (v1, which carries no type list at all); resolution then
        falls back to the sole variant or raises if ambiguous.
        """
        variants = self.functions.get(name)
        if not variants:
            raise ValueError(f'unknown function: {name}')
        req = (
            None if param_types is None
            else _overload.parse_param_types(param_types)
        )
        return _overload.resolve(name, variants, req)


def _get_masked_params(func: Callable[..., Any]) -> List[bool]:
    """Determine which parameters expect (data, mask) tuples vs just data."""
    params = inspect.signature(func).parameters
    return [typing.get_origin(x.annotation) is Masked for x in params.values()]


def _get_vector_loader(fmt: str) -> Callable[..., Any]:
    """Return the appropriate rowdat_1 loader for the given data format."""
    loaders: Dict[str, str] = {
        'numpy': 'load_numpy',
        'pandas': 'load_pandas',
        'polars': 'load_polars',
        'arrow': 'load_arrow',
        'list': 'load_list',
    }
    attr = loaders.get(fmt)
    if attr is None:
        raise ValueError(f'unsupported vector data format: {fmt!r}')
    return getattr(_rowdat_1, attr)


def _get_vector_dumper(fmt: str) -> Callable[..., Any]:
    """Return the appropriate rowdat_1 dumper for the given data format."""
    dumpers: Dict[str, str] = {
        'numpy': 'dump_numpy',
        'pandas': 'dump_pandas',
        'polars': 'dump_polars',
        'arrow': 'dump_arrow',
        'list': 'dump_list',
    }
    attr = dumpers.get(fmt)
    if attr is None:
        raise ValueError(f'unsupported vector data format: {fmt!r}')
    return getattr(_rowdat_1, attr)


def _normalize_vector_output(
    out: Any,
    num_returns: int,
) -> List[Tuple[Any, Any]]:
    """Normalize vectorized UDF output to List[(data, mask_or_None)]."""
    if num_returns == 1:
        if isinstance(out, tuple) and len(out) == 2:
            # Could be a Masked (data, mask) or a 2-element tuple of columns
            # Check if it looks like Masked: second element is a boolean mask
            mask_candidate = out[1]
            if hasattr(mask_candidate, 'dtype'):
                try:
                    import numpy as np
                    if mask_candidate.dtype == np.bool_:
                        return [out]
                except ImportError:
                    pass
        return [(out, None)]

    # Multiple return columns
    if not isinstance(out, (tuple, list)):
        raise TypeError(
            f'vectorized UDF with {num_returns} return columns must '
            f'return a tuple or list, got {type(out).__name__}',
        )
    result_cols = []
    for x in out:
        if isinstance(x, tuple) and len(x) == 2:
            result_cols.append(x)
        else:
            result_cols.append((x, None))
    return result_cols


def _call_function_vector(
    func: Callable[..., Any],
    arg_types: List[Tuple[str, int]],
    return_types: List[int],
    input_data: bytes,
    args_data_format: str,
    returns_data_format: str,
    masks: List[bool],
) -> bytes:
    """Call a vectorized UDF with columnar data."""
    loader = _get_vector_loader(args_data_format)
    dumper = _get_vector_dumper(returns_data_format)

    row_ids, cols = loader(arg_types, input_data)

    # Masked params get the full (data, mask) tuple, others get just data
    func_args = [col if m else col[0] for col, m in zip(cols, masks)]

    out = func(*func_args)

    result_cols = _normalize_vector_output(out, len(return_types))

    return bytes(dumper(return_types, row_ids, result_cols))


def call_function(
    registry: FunctionRegistry,
    name: str,
    input_data: bytes,
    param_types: Optional[str] = None,
) -> bytes:
    """Call a registered UDF by name using the C accelerator or fallback.

    This is the hot-path function used by both the WASM handler and
    the plugin server. ``param_types`` is the semicolon-separated SQL type
    list from the wire and is required when a name has multiple variants.
    ``''`` denotes a zero-arg call; ``None`` denotes no type info at all.
    """
    func_info = registry.lookup_variant(name, param_types)
    func = func_info['func']
    arg_types = func_info['arg_types']
    return_types = func_info['return_types']
    sig = func_info['signature']

    args_data_format = sig.get('args_data_format') or 'scalar'
    returns_data_format = sig.get('returns_data_format') or 'scalar'

    try:
        # Vector path: columnar processing
        if args_data_format not in ('scalar',):
            masks = func_info.get('_masks')
            if masks is None:
                masks = _get_masked_params(func)
                func_info['_masks'] = masks
            return _call_function_vector(
                func, arg_types, return_types, input_data,
                args_data_format, returns_data_format, masks,
            )

        # Scalar path: row-by-row processing
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
            if not isinstance(result, (tuple, list)):
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
