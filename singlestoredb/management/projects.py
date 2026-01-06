#!/usr/bin/env python
"""SingleStoreDB Project Management."""
from __future__ import annotations

import datetime
from typing import Any
from typing import cast
from typing import Dict
from typing import Optional
from typing import TYPE_CHECKING

from .utils import NamedList
from .utils import normalize_project_edition
from .utils import require_fields
from .utils import to_datetime
from .utils import vars_to_str

if TYPE_CHECKING:
    from .workspace import WorkspaceManager


class Project:
    """
    SingleStoreDB project definition.

    This object is not instantiated directly. It is used in the results
    of API calls on the :class:`WorkspaceManager`. Projects are accessed
    via :attr:`WorkspaceManager.projects`.

    See Also
    --------
    :attr:`WorkspaceManager.projects`

    """

    id: str
    name: str
    edition: Optional[str]
    created_at: Optional[datetime.datetime]

    def __init__(
        self,
        project_id: str,
        name: str,
        edition: Optional[str] = None,
        created_at: Optional[datetime.datetime] = None,
    ):
        #: Unique ID of the project
        self.id = project_id

        #: Name of the project
        self.name = name

        #: Edition of the project (e.g., 'STANDARD', 'ENTERPRISE', 'SHARED')
        self.edition = normalize_project_edition(edition) if edition else edition

        #: Timestamp of when the project was created
        self.created_at = created_at

        self._manager: Optional[WorkspaceManager] = None

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)

    @classmethod
    def from_dict(cls, obj: Dict[str, Any], manager: 'WorkspaceManager') -> 'Project':
        """
        Construct a Project from a dictionary of values.

        Parameters
        ----------
        obj : dict
            Dictionary of values
        manager : WorkspaceManager
            The WorkspaceManager the Project belongs to

        Returns
        -------
        :class:`Project`

        """
        require_fields(obj, 'projectID', 'name')
        out = cls(
            project_id=obj['projectID'],
            name=obj['name'],
            edition=obj.get('edition'),
            created_at=to_datetime(obj.get('createdAt')),
        )
        out._manager = manager
        return out


class ProjectsMixin:
    """Mixin class that adds project management methods to WorkspaceManager."""

    @property
    def projects(self) -> NamedList[Project]:
        """Return a list of all projects in the organization."""
        manager = cast('WorkspaceManager', self)
        res = manager._get('projects')
        return NamedList([
            Project.from_dict(item, manager)
            for item in res.json()
        ])
