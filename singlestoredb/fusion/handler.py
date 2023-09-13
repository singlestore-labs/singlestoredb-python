#!/usr/bin/env python3
import json
import re
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Tuple

from . import result
from ..connection import Connection

SQLResults = Tuple[List[Tuple[str, int]], List[Tuple[Any, ...]]]


class Handler:

    def __init__(self, connection: Connection, manager: Any):
        super().__init__()
        self.connection = connection
        self.manager = manager

    def handle_generic(self, action: str, params: Dict[str, Any]) -> SQLResults:
        print('<UNKNOWN ACTION>', action, params)
        return [], []

    def __call__(
        self,
        action: str,
        params: Dict[str, Any],
    ) -> result.DummySQLResult:
        try:
            func = getattr(self, f'handle_{action}')
        except AttributeError:
            desc, data = self.handle_generic(action, params)
        else:
            desc, data = func(params)

        out = result.DummySQLResult(self.connection)
        out.inject_data(desc, data)
        return out

    def handle_show_regions(self, params: Dict[str, Any]) -> SQLResults:
        desc = [
            ('Name', result.STRING),
            ('ID', result.STRING),
            ('Provider', result.STRING),
        ]
        is_like = self.build_like_func(params.get('like', None))
        return desc, [(x.name, x.id, x.provider)
                      for x in self.manager.regions if is_like(x.name)]

    def handle_show_workspace_groups(self, params: Dict[str, Any]) -> SQLResults:
        desc = [
            ('Name', result.STRING),
            ('ID', result.STRING),
            ('Region Name', result.STRING),
            ('Firewall Ranges', result.JSON),
        ]
        if params.get('extended'):
            desc += [
                ('Created At', result.DATETIME),
                ('Terminated At', result.DATETIME),
            ]

            def fields(x: Any) -> Any:
                return (
                    x.name, x.id, x.region.name,
                    json.dumps(x.firewall_ranges),
                    x.created_at, x.terminated_at,
                )
        else:
            def fields(x: Any) -> Any:
                return (x.name, x.id, x.region.name, x.firewall_ranges)
        is_like = self.build_like_func(params.get('like', None))
        return (
            desc,
            [fields(x) for x in self.manager.workspace_groups if is_like(x.name)],
        )

    def handle_show_workspaces(self, params: Dict[str, Any]) -> SQLResults:
        desc = [
            ('Name', result.STRING),
            ('ID', result.STRING),
            ('Size', result.STRING),
            ('State', result.STRING),
        ]

        if 'in_group' in params:
            workspace_group_id = [
                x.id for x in self.manager.workspace_groups
                if x.name == params['in_group']
            ]
            if not workspace_group_id:
                raise ValueError(
                    'no workspace group found with name "{}"'.format(params['in_group']),
                )
            workspace = self.manager.get_workspace_group(workspace_group_id[0])
        else:
            workspace = self.manager.get_workspace_group(params['in_group_id'])

        if params.get('extended'):
            desc += [
                ('Endpoint', result.STRING),
                ('Created At', result.DATETIME),
                ('Terminated At', result.DATETIME),
            ]

            def fields(x: Any) -> Any:
                return (
                    x.name, x.id, x.size, x.state,
                    x.endpoint, x.created_at, x.terminated_at,
                )
        else:
            def fields(x: Any) -> Any:
                return (x.name, x.id, x.size, x.state)

        is_like = self.build_like_func(params.get('like', None))

        return desc, [fields(x) for x in workspace.workspaces if is_like(x.name)]

    def handle_create_workspace(self, params: Dict[str, Any]) -> SQLResults:
        if 'in_group' in params:
            workspace_group_id = [
                x.id for x in self.manager.workspace_groups
                if x.name == params['in_group']
            ]
            if not workspace_group_id:
                raise ValueError(
                    'no workspace group found with name "{}"'.format(params['in_group']),
                )
            workspace_group_id = workspace_group_id[0]
        else:
            workspace_group_id = params['in_group_id']

        self.manager.create_workspace(
            params['name'], workspace_group_id, size=params.get('with_size'),
        )
        return [], []

    def build_like_func(self, like: str) -> Callable[[str], bool]:
        """Construct a function to apply the LIKE clause."""
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
