"""
Minimal numpy stub for WASM environment.

This provides just enough of numpy's interface for singlestoredb's
get_signature() function to work without actual numpy.
"""


class _DtypeMeta(type):
    """Metaclass for dtype to support typing.get_origin checks."""
    pass


class dtype(metaclass=_DtypeMeta):
    """Stub dtype class."""
    def __init__(self, spec=None):
        self.spec = spec

    def __repr__(self):
        return f'dtype({self.spec!r})'


class ndarray:
    """Stub ndarray class."""
    pass


# Stub type classes - these just need to exist for isinstance/issubclass checks
class bool_:
    pass


class integer:
    pass


class int_(integer):
    pass


class int8(integer):
    pass


class int16(integer):
    pass


class int32(integer):
    pass


class int64(integer):
    pass


class uint(integer):
    pass


class unsignedinteger(integer):
    pass


class uint8(unsignedinteger):
    pass


class uint16(unsignedinteger):
    pass


class uint32(unsignedinteger):
    pass


class uint64(unsignedinteger):
    pass


class longlong(integer):
    pass


class ulonglong(unsignedinteger):
    pass


class str_:
    pass


class bytes_:
    pass


class float16:
    pass


class float32:
    pass


class float64:
    pass


class double(float64):
    pass


class float_(float64):
    pass


class single(float32):
    pass


class unicode_(str_):
    pass


# Common aliases
float = float64
int = int64
