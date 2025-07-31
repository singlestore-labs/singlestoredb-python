"""Parse docstrings as per Sphinx notation."""
from .common import Docstring
from .common import DocstringDeprecated
from .common import DocstringMeta
from .common import DocstringParam
from .common import DocstringRaises
from .common import DocstringReturns
from .common import DocstringStyle
from .common import ParseError
from .common import RenderingStyle
from .parser import compose
from .parser import parse
from .parser import parse_from_object
from .util import combine_docstrings

Style = DocstringStyle  # backwards compatibility

__all__ = [
    'parse',
    'parse_from_object',
    'combine_docstrings',
    'compose',
    'ParseError',
    'Docstring',
    'DocstringMeta',
    'DocstringParam',
    'DocstringRaises',
    'DocstringReturns',
    'DocstringDeprecated',
    'DocstringStyle',
    'RenderingStyle',
    'Style',
]
