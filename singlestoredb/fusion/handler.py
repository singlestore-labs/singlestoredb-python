#!/usr/bin/env python3
import abc
import functools
import os
import re
import sys
import textwrap
from typing import Any
from typing import Callable
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple

from parsimonious import Grammar
from parsimonious import ParseError
from parsimonious.nodes import Node
from parsimonious.nodes import NodeVisitor

from . import result
from ..connection import Connection

CORE_GRAMMAR = r'''
    ws = ~r"(\s+|(\s*/\*.*\*/\s*)+)"
    qs = ~r"\"([^\"]*)\"|'([^\']*)'|([A-Za-z0-9_\-\.]+)|`([^\`]+)`" ws*
    number = ~r"[-+]?(\d*\.)?\d+(e[-+]?\d+)?"i ws*
    integer = ~r"-?\d+" ws*
    comma = ws* "," ws*
    eq = ws* "=" ws*
    open_paren = ws* "(" ws*
    close_paren = ws* ")" ws*
    open_repeats = ws* ~r"[\(\[\{]" ws*
    close_repeats = ws* ~r"[\)\]\}]" ws*
    statement = ~r"[\s\S]*" ws*
    table = ~r"(?:([A-Za-z0-9_\-]+)|`([^\`]+)`)(?:\.(?:([A-Za-z0-9_\-]+)|`([^\`]+)`))?" ws*
    column = ~r"(?:([A-Za-z0-9_\-]+)|`([^\`]+)`)(?:\.(?:([A-Za-z0-9_\-]+)|`([^\`]+)`))?" ws*
    link_name = ~r"(?:([A-Za-z0-9_\-]+)|`([^\`]+)`)(?:\.(?:([A-Za-z0-9_\-]+)|`([^\`]+)`))?" ws*
    catalog_name = ~r"(?:([A-Za-z0-9_\-]+)|`([^\`]+)`)(?:\.(?:([A-Za-z0-9_\-]+)|`([^\`]+)`))?" ws*

    json = ws* json_object ws*
    json_object = ~r"{\s*" json_members? ~r"\s*}"
    json_members = json_mapping (~r"\s*,\s*" json_mapping)*
    json_mapping = json_string ~r"\s*:\s*" json_value
    json_array = ~r"\[\s*" json_items? ~r"\s*\]"
    json_items = json_value (~r"\s*,\s*" json_value)*
    json_value = json_object / json_array / json_string / json_true_val / json_false_val / json_null_val / json_number
    json_true_val = "true"
    json_false_val = "false"
    json_null_val = "null"
    json_string = ~r"\"[ !#-\[\]-\U0010ffff]*(?:\\(?:[\"\\/bfnrt]|u[0-9A-Fa-f]{4})[ !#-\[\]-\U0010ffff]*)*\""
    json_number = ~r"-?(0|[1-9][0-9]*)(\.\d*)?([eE][-+]?\d+)?"
'''  # noqa: E501

BUILTINS = {
    '<order-by>': r'''
    order_by = ORDER BY order_by_key_,...
    order_by_key_ = '<key>' [ ASC | DESC ]
    ''',
    '<like>': r'''
    like = LIKE '<pattern>'
    ''',
    '<extended>': r'''
    extended = EXTENDED
    ''',
    '<limit>': r'''
    limit = LIMIT <integer>
    ''',
    '<integer>': '',
    '<number>': '',
    '<json>': '',
    '<table>': '',
    '<column>': '',
    '<catalog-name>': '',
    '<link-name>': '',
    '<file-type>': r'''
    file_type = { FILE | FOLDER }
    ''',
    '<statement>': '',
}

BUILTIN_DEFAULTS = {  # type: ignore
    'order_by': {'by': []},
    'like': None,
    'extended': False,
    'limit': None,
    'json': {},
}

_json_unesc_re = re.compile(r'\\(["/\\bfnrt]|u[0-9A-Fa-f])')
_json_unesc_map = {
    '"': '"',
    '/': '/',
    '\\': '\\',
    'b': '\b',
    'f': '\f',
    'n': '\n',
    'r': '\r',
    't': '\t',
}


def _json_unescape(m: Any) -> str:
    c = m.group(1)
    if c[0] == 'u':
        return chr(int(c[1:], 16))
    c2 = _json_unesc_map.get(c)
    if not c2:
        raise ValueError(f'invalid escape sequence: {m.group(0)}')
    return c2


def json_unescape(s: str) -> str:
    return _json_unesc_re.sub(_json_unescape, s[1:-1])


def get_keywords(grammar: str) -> Tuple[str, ...]:
    """Return all all-caps words from the beginning of the line."""
    m = re.match(r'^\s*((?:[@A-Z0-9_]+)(\s+|$|;))+', grammar)
    if not m:
        return tuple()
    return tuple(re.split(r'\s+', m.group(0).replace(';', '').strip()))


def is_bool(grammar: str) -> bool:
    """Determine if the rule is a boolean."""
    return bool(re.match(r'^[@A-Z0-9_\s*]+$', grammar))


def process_optional(m: Any) -> str:
    """Create options or groups of options."""
    sql = m.group(1).strip()
    if '|' in sql:
        return f'( {sql} )*'
    return f'( {sql} )?'


def process_alternates(m: Any) -> str:
    """Make alternates mandatory groups."""
    sql = m.group(1).strip()
    if '|' in sql:
        return f'( {sql} )'
    raise ValueError(f'alternates must contain "|": {sql}')


def process_repeats(m: Any) -> str:
    """Add repeated patterns."""
    sql = m.group(1).strip()
    return f'open_repeats? {sql} ws* ( comma {sql} ws* )* close_repeats?'


def lower_and_regex(m: Any) -> str:
    """Lowercase and convert literal to regex."""
    start = m.group(1) or ''
    sql = m.group(2)
    return f'~"{start}{sql.lower()}"i'


def split_unions(grammar: str) -> str:
    """
    Convert grammar in the form '[ x ] [ y ]' to '[ x | y ]'.

    Parameters
    ----------
    grammar : str
        SQL grammar

    Returns
    -------
    str

    """
    in_alternate = False
    out = []
    for c in grammar:
        if c == '{':
            in_alternate = True
            out.append(c)
        elif c == '}':
            in_alternate = False
            out.append(c)
        elif not in_alternate and c == '|':
            out.append(']')
            out.append(' ')
            out.append('[')
        else:
            out.append(c)
    return ''.join(out)


def expand_rules(rules: Dict[str, str], m: Any) -> str:
    """
    Return expanded grammar syntax for given rule.

    Parameters
    ----------
    ops : Dict[str, str]
        Dictionary of rules in grammar

    Returns
    -------
    str

    """
    txt = m.group(1)
    if txt in rules:
        return f' {rules[txt]} '
    return f' <{txt}> '


def build_cmd(grammar: str) -> str:
    """Pre-process grammar to construct top-level command."""
    if ';' not in grammar:
        raise ValueError('a semi-colon exist at the end of the primary rule')

    # Pre-space
    m = re.match(r'^\s*', grammar)
    space = m.group(0) if m else ''

    # Split on ';' on a line by itself
    begin, end = grammar.split(';', 1)

    # Get statement keywords
    keywords = get_keywords(begin)
    cmd = '_'.join(x.lower() for x in keywords) + '_cmd'

    # Collapse multi-line to one
    begin = re.sub(r'\s+', r' ', begin)

    return f'{space}{cmd} ={begin}\n{end}'


def build_syntax(grammar: str) -> str:
    """Construct full syntax."""
    if ';' not in grammar:
        raise ValueError('a semi-colon exist at the end of the primary rule')

    # Split on ';' on a line by itself
    cmd, end = grammar.split(';', 1)

    name = ''
    rules: Dict[str, Any] = {}
    for line in end.split('\n'):
        line = line.strip()
        if line.startswith('&'):
            rules[name] += '\n' + line
            continue
        if not line:
            continue
        name, value = line.split('=', 1)
        name = name.strip()
        value = value.strip()
        rules[name] = value

    while re.search(r' [a-z0-9_]+\b', cmd):
        cmd = re.sub(r' ([a-z0-9_]+)\b', functools.partial(expand_rules, rules), cmd)

    def add_indent(m: Any) -> str:
        return '    ' + (len(m.group(1)) * '    ')

    # Indent line-continuations
    cmd = re.sub(r'^(\&+)\s*', add_indent, cmd, flags=re.M)

    cmd = textwrap.dedent(cmd).rstrip() + ';'
    cmd = re.sub(r'(\S)  +', r'\1 ', cmd)
    cmd = re.sub(r'<comma>', ',', cmd)
    cmd = re.sub(r'\s+,\s*\.\.\.', ',...', cmd)

    return cmd


def _format_examples(ex: str) -> str:
    """Convert examples into sections."""
    return re.sub(r'(^Example\s+\d+.*$)', r'### \1', ex, flags=re.M)


def _format_arguments(arg: str) -> str:
    """Format arguments as subsections."""
    out = []
    for line in arg.split('\n'):
        if line.startswith('<'):
            out.append(f'### {line.replace("<", "&lt;").replace(">", "&gt;")}')
            out.append('')
        else:
            out.append(line.strip())
    return '\n'.join(out)


def _to_markdown(txt: str) -> str:
    """Convert formatting to markdown."""
    txt = re.sub(r'`([^`]+)\s+\<([^\>]+)>`_', r'[\1](\2)', txt)
    txt = txt.replace('``', '`')

    # Format code blocks
    lines = re.split(r'\n', txt)
    out = []
    while lines:
        line = lines.pop(0)
        if line.endswith('::'):
            out.append(line[:-2] + '.')
            code = []
            while lines and (not lines[0].strip() or lines[0].startswith(' ')):
                code.append(lines.pop(0).rstrip())
            code_str = re.sub(r'^\s*\n', r'', '\n'.join(code).rstrip())
            out.extend([f'```sql\n{code_str}\n```\n'])
        else:
            out.append(line)

    return '\n'.join(out)


def build_help(syntax: str, grammar: str) -> str:
    """Build full help text."""
    cmd = re.match(r'([A-Z0-9_ ]+)', syntax.strip())
    if not cmd:
        raise ValueError(f'no command found: {syntax}')

    out = [f'# {cmd.group(1)}\n\n']

    sections: Dict[str, str] = {}
    grammar = textwrap.dedent(grammar.rstrip())
    desc_re = re.compile(r'^([A-Z][\S ]+)\s*^\-\-\-\-+\s*$', flags=re.M)
    if desc_re.search(grammar):
        _, *txt = desc_re.split(grammar)
        txt = [x.strip() for x in txt]
        sections = {}
        while txt:
            key = txt.pop(0)
            value = txt.pop(0)
            sections[key.lower()] = _to_markdown(value).strip()

    if 'description' in sections:
        out.extend([sections['description'], '\n\n'])

    out.append(f'## Syntax\n\n```sql{syntax}\n```\n\n')

    if 'arguments' in sections:
        out.extend([
            '## Arguments\n\n',
            _format_arguments(sections['arguments']),
            '\n\n',
        ])
    if 'argument' in sections:
        out.extend([
            '## Argument\n\n',
            _format_arguments(sections['argument']),
            '\n\n',
        ])

    if 'remarks' in sections:
        out.extend(['## Remarks\n\n', sections['remarks'], '\n\n'])

    if 'examples' in sections:
        out.extend(['## Examples\n\n', _format_examples(sections['examples']), '\n\n'])
    elif 'example' in sections:
        out.extend(['## Example\n\n', _format_examples(sections['example']), '\n\n'])

    if 'see also' in sections:
        out.extend(['## See Also\n\n', sections['see also'], '\n\n'])

    return ''.join(out).rstrip() + '\n'


def strip_comments(grammar: str) -> str:
    """Strip comments from grammar."""
    desc_re = re.compile(r'(^\s*Description\s*^\s*-----------\s*$)', flags=re.M)
    grammar = desc_re.split(grammar, maxsplit=1)[0]
    return re.sub(r'^\s*#.*$', r'', grammar, flags=re.M)


def get_rule_info(grammar: str) -> Dict[str, Any]:
    """Compute metadata about rule used in coallescing parsed output."""
    return dict(
        n_keywords=len(get_keywords(grammar)),
        repeats=',...' in grammar,
        default=False if is_bool(grammar) else [] if ',...' in grammar else None,
    )


def inject_builtins(grammar: str) -> str:
    """Inject complex builtin rules."""
    for k, v in BUILTINS.items():
        if re.search(k, grammar):
            grammar = re.sub(
                k,
                k.replace('<', '').replace('>', '').replace('-', '_'),
                grammar,
            )
            grammar += v
    return grammar


def process_grammar(
    grammar: str,
) -> Tuple[Grammar, Tuple[str, ...], Dict[str, Any], str, str]:
    """
    Convert SQL grammar to a Parsimonious grammar.

    Parameters
    ----------
    grammar : str
        The SQL grammar

    Returns
    -------
    (Grammar, Tuple[str, ...], Dict[str, Any], str) - Grammar is the parsimonious
    grammar object. The tuple is a series of the keywords that start the command.
    The dictionary is a set of metadata about each rule. The final string is
    a human-readable version of the grammar for documentation and errors.

    """
    out = []
    rules = {}
    rule_info = {}

    full_grammar = grammar
    grammar = strip_comments(grammar)
    grammar = inject_builtins(grammar)
    command_key = get_keywords(grammar)
    syntax_txt = build_syntax(grammar)
    help_txt = build_help(syntax_txt, full_grammar)
    grammar = build_cmd(grammar)

    # Remove line-continuations
    grammar = re.sub(r'\n\s*&+', r'', grammar)

    # Make sure grouping characters all have whitespace around them
    grammar = re.sub(r' *(\[|\{|\||\}|\]) *', r' \1 ', grammar)

    grammar = re.sub(r'\(', r' open_paren ', grammar)
    grammar = re.sub(r'\)', r' close_paren ', grammar)

    for line in grammar.split('\n'):
        if not line.strip():
            continue

        op, sql = line.split('=', 1)
        op = op.strip()
        sql = sql.strip()
        sql = split_unions(sql)

        rules[op] = sql
        rule_info[op] = get_rule_info(sql)

        # Convert consecutive optionals to a union
        sql = re.sub(r'\]\s+\[', r' | ', sql)

        # Lower-case keywords and make them case-insensitive
        sql = re.sub(r'(\b|@+)([A-Z0-9_]+)\b', lower_and_regex, sql)

        # Convert literal strings to 'qs'
        sql = re.sub(r"'[^']+'", r'qs', sql)

        # Convert special characters to literal tokens
        sql = re.sub(r'([=]) ', r' eq ', sql)

        # Convert [...] groups to (...)*
        sql = re.sub(r'\[([^\]]+)\]', process_optional, sql)

        # Convert {...} groups to (...)
        sql = re.sub(r'\{([^\}]+)\}', process_alternates, sql)

        # Convert <...> to ... (<...> is the form for core types)
        sql = re.sub(r'<([a-z0-9_]+)>', r'\1', sql)

        # Insert ws between every token to allow for whitespace and comments
        sql = ' ws '.join(re.split(r'\s+', sql)) + ' ws'

        # Remove ws in optional groupings
        sql = sql.replace('( ws', '(')
        sql = sql.replace('| ws', '|')

        # Convert | to /
        sql = sql.replace('|', '/')

        # Remove ws after operation names, all operations contain ws at the end
        sql = re.sub(r'(\s+[a-z0-9_]+)\s+ws\b', r'\1', sql)

        # Convert foo,... to foo ("," foo)*
        sql = re.sub(r'(\S+),...', process_repeats, sql)

        # Remove ws before / and )
        sql = re.sub(r'(\s*\S+\s+)ws\s+/', r'\1/', sql)
        sql = re.sub(r'(\s*\S+\s+)ws\s+\)', r'\1)', sql)

        # Make sure every operation ends with ws
        sql = re.sub(r'\s+ws\s+ws$', r' ws', sql + ' ws')
        sql = re.sub(r'(\s+ws)*\s+ws\*$', r' ws*', sql)
        sql = re.sub(r'\s+ws$', r' ws*', sql)
        sql = re.sub(r'\s+ws\s+\(', r' ws* (', sql)
        sql = re.sub(r'\)\s+ws\s+', r') ws* ', sql)
        sql = re.sub(r'\s+ws\s+', r' ws* ', sql)
        sql = re.sub(r'\?\s+ws\+', r'? ws*', sql)

        # Remove extra ws around eq
        sql = re.sub(r'ws\+\s*eq\b', r'eq', sql)

        # Remove optional groupings when mandatory groupings are specified
        sql = re.sub(r'open_paren\s+ws\*\s+open_repeats\?', r'open_paren', sql)
        sql = re.sub(r'close_repeats\?\s+ws\*\s+close_paren', r'close_paren', sql)
        sql = re.sub(r'open_paren\s+open_repeats\?', r'open_paren', sql)
        sql = re.sub(r'close_repeats\?\s+close_paren', r'close_paren', sql)

        out.append(f'{op} = {sql}')

    for k, v in list(rules.items()):
        while re.search(r' ([a-z0-9_]+) ', v):
            v = re.sub(r' ([a-z0-9_]+) ', functools.partial(expand_rules, rules), v)
        rules[k] = v

    for k, v in list(rules.items()):
        while re.search(r' <([a-z0-9_]+)> ', v):
            v = re.sub(r' <([a-z0-9_]+)> ', r' \1 ', v)
        rules[k] = v

    cmds = ' / '.join(x for x in rules if x.endswith('_cmd'))
    cmds = f'init = ws* ( {cmds} ) ws* ";"? ws*\n'

    grammar = cmds + CORE_GRAMMAR + '\n'.join(out)

    try:
        return (
            Grammar(grammar), command_key,
            rule_info, syntax_txt, help_txt,
        )
    except ParseError:
        print(grammar, file=sys.stderr)
        raise


def flatten(items: Iterable[Any]) -> List[Any]:
    """Flatten a list of iterables."""
    out = []
    for x in items:
        if isinstance(x, (str, bytes, dict)):
            out.append(x)
        elif isinstance(x, Iterable):
            for sub_x in flatten(x):
                if sub_x is not None:
                    out.append(sub_x)
        elif x is not None:
            out.append(x)
    return out


def merge_dicts(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Merge list of dictionaries together."""
    out: Dict[str, Any] = {}
    for x in items:
        if isinstance(x, dict):
            same = list(set(x.keys()).intersection(set(out.keys())))
            if same:
                raise ValueError(f"found duplicate rules for '{same[0]}'")
            out.update(x)
    return out


class SQLHandler(NodeVisitor):
    """Base class for all SQL handler classes."""

    #: Parsimonious grammar object
    grammar: Grammar = Grammar(CORE_GRAMMAR)

    #: SQL keywords that start the command
    command_key: Tuple[str, ...] = ()

    #: Metadata about the parse rules
    rule_info: Dict[str, Any] = {}

    #: Syntax string for use in error messages
    syntax: str = ''

    #: Full help for the command
    help: str = ''

    #: Rule validation functions
    validators: Dict[str, Callable[..., Any]] = {}

    _grammar: str = CORE_GRAMMAR
    _is_compiled: bool = False
    _enabled: bool = True

    def __init__(self, connection: Connection):
        self.connection = connection
        self._handled: Set[str] = set()

    @classmethod
    def compile(cls, grammar: str = '') -> None:
        """
        Compile the grammar held in the docstring.

        This method modifies attributes on the class: ``grammar``,
        ``command_key``, ``rule_info``, ``syntax``, and ``help``.

        Parameters
        ----------
        grammar : str, optional
            Grammar to use instead of docstring

        """
        if cls._is_compiled:
            return

        cls.grammar, cls.command_key, cls.rule_info, cls.syntax, cls.help = \
            process_grammar(grammar or cls.__doc__ or '')

        cls._grammar = grammar or cls.__doc__ or ''
        cls._is_compiled = True

    @classmethod
    def register(cls, overwrite: bool = False) -> None:
        """
        Register the handler class.

        Paraemeters
        -----------
        overwrite : bool, optional
            Overwrite an existing command with the same name?

        """
        if not cls._enabled and \
                os.environ.get('SINGLESTOREDB_FUSION_ENABLE_HIDDEN', '0').lower() not in \
                ['1', 't', 'true', 'y', 'yes']:
            return

        from . import registry
        cls.compile()
        registry.register_handler(cls, overwrite=overwrite)

    def create_result(self) -> result.FusionSQLResult:
        """
        Create a new result object.

        Returns
        -------
        FusionSQLResult
            A new result object for this handler

        """
        return result.FusionSQLResult()

    def execute(self, sql: str) -> result.FusionSQLResult:
        """
        Parse the SQL and invoke the handler method.

        Parameters
        ----------
        sql : str
            SQL statement to execute

        Returns
        -------
        DummySQLResult

        """
        type(self).compile()
        self._handled = set()
        try:
            params = self.visit(type(self).grammar.parse(sql))
            for k, v in params.items():
                params[k] = self.validate_rule(k, v)

            res = self.run(params)

            self._handled = set()

            if res is not None:
                res.format_results(self.connection)
                return res

            res = result.FusionSQLResult()
            res.set_rows([])
            res.format_results(self.connection)
            return res

        except ParseError as exc:
            s = str(exc)
            msg = ''
            m = re.search(r'(The non-matching portion.*$)', s)
            if m:
                msg = ' ' + m.group(1)
            m = re.search(r"(Rule) '.+?'( didn't match at.*$)", s)
            if m:
                msg = ' ' + m.group(1) + m.group(2)
            raise ValueError(
                f'Could not parse statement.{msg} '
                'Expecting:\n' + textwrap.indent(type(self).syntax, '  '),
            )

    @abc.abstractmethod
    def run(self, params: Dict[str, Any]) -> Optional[result.FusionSQLResult]:
        """
        Run the handler command.

        Parameters
        ----------
        params : Dict[str, Any]
            Values parsed from the SQL query. Each rule in the grammar
            results in a key/value pair in the ``params` dictionary.

        Returns
        -------
        SQLResult - tuple containing the column definitions and
            rows of data in the result

        """
        raise NotImplementedError

    def visit_qs(self, node: Node, visited_children: Iterable[Any]) -> Any:
        """Quoted strings."""
        if node is None:
            return None
        return flatten(visited_children)[0]

    def visit_compound(self, node: Node, visited_children: Iterable[Any]) -> Any:
        """Compound name."""
        print(visited_children)
        return flatten(visited_children)[0]

    def visit_number(self, node: Node, visited_children: Iterable[Any]) -> Any:
        """Numeric value."""
        return float(flatten(visited_children)[0])

    def visit_integer(self, node: Node, visited_children: Iterable[Any]) -> Any:
        """Integer value."""
        return int(flatten(visited_children)[0])

    def visit_ws(self, node: Node, visited_children: Iterable[Any]) -> Any:
        """Whitespace and comments."""
        return

    def visit_eq(self, node: Node, visited_children: Iterable[Any]) -> Any:
        """Equals sign."""
        return

    def visit_comma(self, node: Node, visited_children: Iterable[Any]) -> Any:
        """Single comma."""
        return

    def visit_open_paren(self, node: Node, visited_children: Iterable[Any]) -> Any:
        """Open parenthesis."""
        return

    def visit_close_paren(self, node: Node, visited_children: Iterable[Any]) -> Any:
        """Close parenthesis."""
        return

    def visit_open_repeats(self, node: Node, visited_children: Iterable[Any]) -> Any:
        """Open repeat grouping."""
        return

    def visit_close_repeats(self, node: Node, visited_children: Iterable[Any]) -> Any:
        """Close repeat grouping."""
        return

    def visit_init(self, node: Node, visited_children: Iterable[Any]) -> Any:
        """Entry point of the grammar."""
        _, out, *_ = visited_children
        return out

    def visit_statement(self, node: Node, visited_children: Iterable[Any]) -> Any:
        out = ' '.join(flatten(visited_children)).strip()
        return {'statement': out}

    def visit_order_by(self, node: Node, visited_children: Iterable[Any]) -> Any:
        """Handle ORDER BY."""
        by = []
        ascending = []
        data = [x for x in flatten(visited_children)[2:] if x]
        for item in data:
            value = item.popitem()[-1]
            if not isinstance(value, list):
                value = [value]
            value.append('A')
            by.append(value[0])
            ascending.append(value[1].upper().startswith('A'))
        return {'order_by': {'by': by, 'ascending': ascending}}

    def _delimited(self, node: Node, children: Iterable[Any]) -> Any:
        children = list(children)
        items = [children[0]]
        items.extend(item for _, item in children[1])
        return items

    def _atomic(self, node: Node, children: Iterable[Any]) -> Any:
        return list(children)[0]

    # visitors
    visit_json_value = _atomic
    visit_json_members = visit_json_items = _delimited

    def visit_json_object(self, node: Node, children: Iterable[Any]) -> Any:
        _, members, _ = children
        if isinstance(members, list):
            members = members[0]
        else:
            members = []
        members = [x for x in members if x != '']
        return dict(members)

    def visit_json_array(self, node: Node, children: Iterable[Any]) -> Any:
        _, values, _ = children
        if isinstance(values, list):
            values = values[0]
        else:
            values = []
        return values

    def visit_json_mapping(self, node: Node, children: Iterable[Any]) -> Any:
        key, _, value = children
        return key, value

    def visit_json_string(self, node: Node, children: Iterable[Any]) -> Any:
        return json_unescape(node.text)

    def visit_json_number(self, node: Node, children: Iterable[Any]) -> Any:
        if '.' in node.text:
            return float(node.text)
        return int(node.text)

    def visit_json_true_val(self, node: Node, children: Iterable[Any]) -> Any:
        return True

    def visit_json_false_val(self, node: Node, children: Iterable[Any]) -> Any:
        return False

    def visit_json_null_val(self, node: Node, children: Iterable[Any]) -> Any:
        return None

    def generic_visit(self, node: Node, visited_children: Iterable[Any]) -> Any:
        """
        Handle all undefined rules.

        This method processes all user-defined rules. Each rule results in
        a dictionary with a single key corresponding to the rule name, with
        a value corresponding to the data value following the rule keywords.

        If no value exists, the value True is used. If the rule is not a
        rule with possible repeated values, a single value is used. If the
        rule can have repeated values, a list of values is returned.

        """
        if node.expr_name.startswith('json'):
            return visited_children or node.text

        # Call a grammar rule
        if node.expr_name in type(self).rule_info:
            n_keywords = type(self).rule_info[node.expr_name]['n_keywords']
            repeats = type(self).rule_info[node.expr_name]['repeats']

            # If this is the top-level command, create the final result
            if node.expr_name.endswith('_cmd'):
                final = merge_dicts(flatten(visited_children)[n_keywords:])
                for k, v in type(self).rule_info.items():
                    if k.endswith('_cmd') or k.endswith('_') or k.startswith('_'):
                        continue
                    if k not in final and k not in self._handled:
                        final[k] = BUILTIN_DEFAULTS.get(k, v['default'])
                return final

            # Filter out stray empty strings
            out = [x for x in flatten(visited_children)[n_keywords:] if x]

            # Remove underscore prefixes from rule name
            key_name = re.sub(r'^_+', r'', node.expr_name)

            if repeats or len(out) > 1:
                self._handled.add(node.expr_name)
                # If all outputs are dicts, merge them
                if len(out) > 1 and not repeats:
                    is_dicts = [x for x in out if isinstance(x, dict)]
                    if len(is_dicts) == len(out):
                        return {key_name: merge_dicts(out)}
                return {key_name: out}

            self._handled.add(node.expr_name)
            return {key_name: out[0] if out else True}

        if hasattr(node, 'match'):
            if not visited_children and not node.match.groups():
                return node.text
            return visited_children or list(node.match.groups())

        return visited_children or node.text

    def validate_rule(self, rule: str, value: Any) -> Any:
        """
        Validate the value of the given rule.

        Paraemeters
        -----------
        rule : str
            Name of the grammar rule the value belongs to
        value : Any
            Value parsed from the query

        Returns
        -------
        Any - result of the validator function

        """
        validator = type(self).validators.get(rule)
        if validator is not None:
            return validator(value)
        return value
