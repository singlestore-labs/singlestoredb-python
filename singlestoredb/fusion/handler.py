#!/usr/bin/env python3
import abc
import functools
import re
import textwrap
from typing import Any
from typing import Callable
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional
from typing import Tuple

from parsimonious import Grammar
from parsimonious import ParseError
from parsimonious.nodes import Node
from parsimonious.nodes import NodeVisitor

from . import result
from ..connection import Connection

CORE_GRAMMAR = r'''
    ws = ~r"(\s*(/\*.*\*/)*\s*)*"
    qs = ~r"\"([^\"]*)\"|'([^\']*)'"
    number = ~r"-?\d+(\.\d+)?|-?\.d+"
    integer = ~r"-?\d+"
    comma = ws "," ws
    open_paren = ws "(" ws
    close_paren = ws ")" ws
'''


def get_keywords(grammar: str) -> Tuple[str, ...]:
    """Return all all-caps words from the beginning of the line."""
    m = re.match(r'^\s*([A-Z0-9_]+(\s+|$))+', grammar)
    if not m:
        return tuple()
    return tuple(re.split(r'\s+', m.group(0).strip()))


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
    return f'open_paren? {sql} ws ( comma {sql} ws )* close_paren?'


def lower_and_regex(m: Any) -> str:
    """Lowercase and convert literal to regex."""
    sql = m.group(1)
    return f'~"{sql.lower()}"i'


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


def build_help(grammar: str) -> str:
    """Construct full help syntax."""
    if ';' not in grammar:
        raise ValueError('a semi-colon exist at the end of the primary rule')

    # Split on ';' on a line by itself
    cmd, end = grammar.split(';', 1)

    rules = {}
    for line in end.split('\n'):
        line = line.strip()
        if not line:
            continue
        name, value = line.split('=', 1)
        name = name.strip()
        value = value.strip()
        rules[name] = value

    while re.search(r' [a-z0-9_]+ ', cmd):
        cmd = re.sub(r' ([a-z0-9_]+) ', functools.partial(expand_rules, rules), cmd)

    return textwrap.dedent(cmd).rstrip() + ';'


def strip_comments(grammar: str) -> str:
    """Strip comments from grammar."""
    return re.sub(r'^\s*#.*$', r'', grammar, flags=re.M)


def get_rule_info(grammar: str) -> Dict[str, Any]:
    """Compute metadata about rule used in coallescing parsed output."""
    return dict(
        n_keywords=len(get_keywords(grammar)),
        repeats=',...' in grammar,
    )


def process_grammar(grammar: str) -> Tuple[Grammar, Tuple[str, ...], Dict[str, Any], str]:
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

    grammar = strip_comments(grammar)
    command_key = get_keywords(grammar)
    help_txt = build_help(grammar)
    grammar = build_cmd(grammar)

    # Make sure grouping characters all have whitespace around them
    grammar = re.sub(r' *(\[|\{|\||\}|\]) *', r' \1 ', grammar)

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
        sql = re.sub(r'\b([A-Z0-9]+)\b', lower_and_regex, sql)

        # Convert literal strings to 'qs'
        sql = re.sub(r"'[^']+'", r'qs', sql)

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
    cmds = f'init = ws ( {cmds} ) ws ";"? ws\n'

    return Grammar(cmds + CORE_GRAMMAR + '\n'.join(out)), command_key, rule_info, help_txt


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

    #: Help string for use in error messages
    help: str = ''

    #: Rule validation functions
    validators: Dict[str, Callable[..., Any]] = {}

    _is_compiled: bool = False

    def __init__(self, connection: Connection):
        self.connection = connection

    @classmethod
    def compile(cls, grammar: str = '') -> None:
        """
        Compile the grammar held in the docstring.

        This method modifies attributes on the class: ``grammar``,
        ``command_key``, ``rule_info``, and ``help``.

        Parameters
        ----------
        grammar : str, optional
            Grammar to use instead of docstring

        """
        if cls._is_compiled:
            return

        cls.grammar, cls.command_key, cls.rule_info, cls.help = \
            process_grammar(grammar or cls.__doc__ or '')

        cls._is_compiled = True

    def create_result(self) -> result.FusionSQLResult:
        """Return a new result object."""
        return result.FusionSQLResult(self.connection)

    @classmethod
    def register(cls, overwrite: bool = False) -> None:
        """
        Register the handler class.

        Paraemeters
        -----------
        overwrite : bool, optional
            Overwrite an existing command with the same name?

        """
        from . import registry
        cls.compile()
        registry.register_handler(cls, overwrite=overwrite)

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
        try:
            res = self.run(self.visit(type(self).grammar.parse(sql)))
            if res is not None:
                return res
            return result.FusionSQLResult(self.connection)
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
                'Expecting:\n' + textwrap.indent(type(self).help, '  '),
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

    def create_like_func(self, like: str) -> Callable[[str], bool]:
        """
        Construct a function to apply the LIKE clause.

        Calling the resulting function will return a boolean indicating
        whether the given string matched the ``like`` pattern.

        Parameters
        ----------
        like : str
            A LIKE pattern (i.e., string with '%' as a wildcard)

        Returns
        -------
        function

        """
        if like is None:
            def is_like(x: Any) -> bool:
                return True
        else:
            regex = re.compile(
                '^{}$'.format(
                    re.sub(r'\\%', r'.*', re.sub(r'([^\w])', r'\\\1', like)),
                ), flags=re.I,
            )

            def is_like(x: Any) -> bool:
                return bool(regex.match(x))

        return is_like

    def visit_qs(self, node: Node, visited_children: Iterable[Any]) -> Any:
        """Quoted strings."""
        if node is None:
            return None
        return node.match.group(1) or node.match.group(2)

    def visit_ws(self, node: Node, visited_children: Iterable[Any]) -> Any:
        """Whitespace and comments."""
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

    def visit_init(self, node: Node, visited_children: Iterable[Any]) -> Any:
        """Entry point of the grammar."""
        _, out, *_ = visited_children
        return out

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
        # Call a grammar rule
        if node.expr_name in type(self).rule_info:
            n_keywords = type(self).rule_info[node.expr_name]['n_keywords']
            repeats = type(self).rule_info[node.expr_name]['repeats']

            # If this is the top-level command, create the final result
            if node.expr_name.endswith('_cmd'):
                return merge_dicts(flatten(visited_children)[n_keywords:])

            # Filter out stray empty strings
            out = [x for x in flatten(visited_children)[n_keywords:] if x]

            if repeats:
                return {node.expr_name: self.validate_rule(node.expr_name, out)}

            return {
                node.expr_name:
                self.validate_rule(node.expr_name, out[0]) if out else True,
            }

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
