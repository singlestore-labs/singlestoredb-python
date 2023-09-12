#!/usr/bin/env python3
from __future__ import annotations

from typing import Any
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional

from parsimonious.grammar import Grammar
from parsimonious.nodes import Node
from parsimonious.nodes import NodeVisitor


GRAMMAR = Grammar(r'''
    init = ws ( show_users / show_regions / show_workspaces / show_workspace_groups / select / create_workspace_group / create_workspace ) ws end? ws
    ws = ~"(\s*(/\*.*\*/)*\s*)*"
    like = ~"like"i ws qs ws
    extended = ~"extended"i ws
    in = ~"in"i ws qs ws
    qs = ~"\"[^\"]*\"|'[^\']*'" ws
    ident = ~"(`[^`]`\.|\w+\.)?(`[^`]+`|\w+|\*)" ws
    name = ~"[A-Z]\w*" ws
    compound_name = ident alias? ws
    comma = "," ws
    end = ";" ws
    alias = ~"as"i ws ~"(`[^`]+`|\w+)" ws
    projections = compound_name alias? (comma compound_name alias?)* ws
    table = compound_name ws
    select = ~"select"i ws projections ~"from"i ws table ws
    in_region = ~"in"i ws ~"region" ws (~"id" ws)? qs ws
    with_password = ~"with"i ws ~"password" ws qs ws
    with_firewall_ranges = ~"with"i ws ~"firewall"i ws ~"ranges"i ws qs (comma qs)* ws
    expires_at = ~"expires"i ws ~"at" ws qs ws
    in_group = ~"in"i ws ~"group" ws (~"id" ws)? qs ws
    with_size = ~"with"i ws ~"size"i ws qs ws
    show_users = ~"show"i ws ~"users"i ws like? ws
    show_workspaces = ~"show"i ws ~"workspaces"i ws in_group ( like / extended )* ws
    show_workspace_groups = ~"show"i ws ~"workspace"i ws ~"groups"i ws ( like / extended )* ws
    show_regions = ~"show"i ws ~"regions"i ws like? ws
    create_workspace_group = ~"create"i ws ~"workspace"i ws ~"group" ws qs in_region ( with_password / expires_at / with_firewall_ranges )* ws
    create_workspace = ~"create"i ws ~"workspace"i ws qs in_group with_size ws
''')


def flatten(items: Iterable[Any]) -> List[Any]:
    """Flatten a list of iterables."""
    out = []
    for x in items:
        if isinstance(x, Iterable) and not isinstance(x, (str, bytes)):
            for sub_x in flatten(x):
                if sub_x is not None:
                    out.append(sub_x)
        elif x is not None:
            out.append(x)
    return out


def strip_backticks(s: str) -> str:
    """Strip backticks from a string if they exist."""
    if s and s[0] == '`':
        return s[1:-1]
    return s


class Token:
    """Base token class."""
    expendable = False

    def __init__(self, value: Any):
        self.value = value

    def __str__(self) -> str:
        return self.value

    def __repr__(self) -> str:
        return '{}({})'.format(type(self).__name__, self.value)


class Component(Token):
    """Database component including a namespace, name, and alias."""

    def __init__(self, namespace: str, value: Any, alias: Optional[str] = None):
        super().__init__(value)
        self.namespace = namespace
        self.alias = alias or value

    def __repr__(self) -> str:
        return '{}({}.{}, alias={})'.format(
            type(self).__name__,
            repr(self.namespace),
            repr(self.value),
            repr(self.alias),
        )

    @classmethod
    def from_component(cls, obj: Component) -> Component:
        return cls(obj.namespace, obj.value, alias=obj.alias)


class Projection(Component):
    """Database projections."""


class Table(Component):
    """Database table."""


class Modifier:
    """Named options."""

    def __init__(self, name: str, value: Any, is_singleton: bool = True):
        self.name = name
        self.value = value
        self.is_singleton = is_singleton

    @classmethod
    def collect(cls, tokens: Iterable[Any]) -> Dict[str, Any]:
        """Collect Modifier tokens from an iteratable."""
        out: Dict[str, Any] = {}
        found_keys = set()
        for item in tokens:
            if isinstance(item, Modifier):
                if item.name in found_keys and item.is_singleton:
                    raise ValueError(
                        f'multiple values found for `{item.name}`: '
                        f'{item.value}, {out[item.name]}',
                    )
                out[item.name] = item.value
                found_keys.add(item.name)
        return out


class SQLParser(NodeVisitor):

    def __init__(self, handler: Any):
        super().__init__()
        self.handler = handler

    def execute(self, query: str) -> Any:
        return self.visit(GRAMMAR.parse(query))

    def visit_show_users(self, node: Node, visited_children: Iterable[Any]) -> Any:
        _, _, *modifiers = flatten(visited_children)
        return self.handler('show_users', Modifier.collect(modifiers))

    def visit_show_regions(self, node: Node, visited_children: Iterable[Any]) -> Any:
        _, _, *modifiers = flatten(visited_children)
        return self.handler('show_regions', Modifier.collect(modifiers))

    def visit_show_workspaces(self, node: Node, visited_children: Iterable[Any]) -> Any:
        _, _, *modifiers = flatten(visited_children)
        return self.handler('show_workspaces', Modifier.collect(modifiers))

    def visit_show_workspace_groups(
        self,
        node: Node,
        visited_children: Iterable[Any],
    ) -> Any:
        _, _, _, *modifiers = flatten(visited_children)
        return self.handler('show_workspace_groups', Modifier.collect(modifiers))

    def visit_create_workspace_group(
        self,
        node: Node,
        visited_children: Iterable[Any],
    ) -> Any:
        _, _, _, name, region, *modifiers = flatten(visited_children)
        return self.handler(
            'create_workspace_group',
            dict(name=name, region=region.value, **Modifier.collect(modifiers)),
        )

    def visit_create_workspace(self, node: Node, visited_children: Iterable[Any]) -> Any:
        _, _, name, *modifiers = flatten(visited_children)
        return self.handler(
            'create_workspace',
            dict(name=name, **Modifier.collect(modifiers)),
        )

    def visit_select(self, node: Node, visited_children: Iterable[Any]) -> Any:
        _, _, columns, _, _, table, *modifiers = flatten(visited_children)
        return self.handler(
            'select',
            dict(columns=columns, table=table, **Modifier.collect(modifiers)),
        )

    def visit_projections(self, node: Node, visited_children: Iterable[Any]) -> Any:
        projections = flatten(visited_children)
        return [Projection.from_component(x) for x in projections if x]

    def visit_compound_name(self, node: Node, visited_children: Iterable[Any]) -> Any:
        name, alias, _ = visited_children
        return Component(name[0], name[1], alias=alias[0] if alias else None)

    def visit_alias(self, node: Node, visited_children: Iterable[Any]) -> Any:
        _, _, name, _ = visited_children
        return name[0]

    def visit_table(self, node: Node, visited_children: Iterable[Any]) -> Any:
        table = flatten(visited_children)
        return Table.from_component(table[0])

    def visit_ident(self, node: Node, visited_children: Iterable[Any]) -> Any:
        ident, *_ = visited_children
        # Strip trailing '.'
        if ident[0]:
            ident[0] = ident[0][:-1]
        return [strip_backticks(ident[0]), strip_backticks(ident[1])]

    def visit_ws(self, node: Node, visited_children: Iterable[Any]) -> Any:
        return

    def visit_comma(self, node: Node, visited_children: Iterable[Any]) -> Any:
        return

    def visit_qs(self, node: Node, visited_children: Iterable[Any]) -> Any:
        text = node.children[0].match.group(0)
        return text[1:-1]

    def visit_in_region(self, node: Node, visited_children: Iterable[Any]) -> Any:
        _, _, is_id, value = flatten(visited_children)
        return Modifier('in_region_id' if is_id else 'in_region', value)

    def visit_with_password(self, node: Node, visited_children: Iterable[Any]) -> Any:
        return Modifier('with_password', flatten(visited_children)[-1])

    def visit_expires_at(self, node: Node, visited_children: Iterable[Any]) -> Any:
        return Modifier('expires_at', flatten(visited_children)[-1])

    def visit_with_firewall_ranges(self, node: Node, visited_children: Iterable[Any]) -> Any:
        _, _, _, *ranges = flatten(visited_children)
        return Modifier('with_firewall_ranges', ranges)

    def visit_in_group(self, node: Node, visited_children: Iterable[Any]) -> Any:
        _, _, is_id, value = flatten(visited_children)
        return Modifier('in_group_id' if is_id else 'in_group', value)

    def visit_with_size(self, node: Node, visited_children: Iterable[Any]) -> Any:
        return Modifier('with_size', flatten(visited_children)[-1].upper())

    def visit_extended(self, node: Node, visited_children: Iterable[Any]) -> Any:
        return Modifier('extended', True)

    def generic_visit(self, node: Node, visited_children: Iterable[Any]) -> Any:
        if hasattr(node, 'match'):
            if not visited_children and not node.match.groups():
                return node.text
            return visited_children or list(node.match.groups())
        return visited_children or node.text

    def visit_like(self, node: Node, visited_children: Iterable[Any]) -> Any:
        _, _, text, _ = visited_children
        return Modifier('like', text)

    def visit_in(self, node: Node, visited_children: Iterable[Any]) -> Any:
        _, _, text, _ = visited_children
        return Modifier('in', text)

    def visit_init(self, node: Node, visited_children: Iterable[Any]) -> Any:
        _, out, *_ = visited_children
        return out[0]
