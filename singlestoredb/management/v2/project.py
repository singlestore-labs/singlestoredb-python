#!/usr/bin/env python
"""
SingleStoreDB Project API v2.

``GET /v2/projects`` lists the projects in the current organization. The route
is absent from ``dev-docs/management_api.openapi`` but is live, and v2 needs it:
``POST /v2/clusters`` rejects a body without ``projectID``
(``400 projectID is required``), where ``POST /v1/workspaceGroups`` assigned one
implicitly. The identical route answers at v1, but nothing at v1 has to send a
project ID, so this stays with the version that does.
"""
from __future__ import annotations

import datetime
from typing import Any
from typing import Dict
from typing import Optional
from typing import Union

from ..manager import Manager
from ..utils import to_datetime
from ..utils import vars_to_str


class Project:
    """
    Project definition.

    This object is not directly instantiated. It is used in results of
    ``ClusterManager`` API calls.

    See Also
    --------
    :attr:`ClusterManager.projects`

    """

    def __init__(
        self,
        id: str,
        name: str,
        edition: Optional[str] = None,
        created_at: Optional[Union[str, datetime.datetime]] = None,
    ) -> None:
        """Use :attr:`ClusterManager.projects` instead."""
        #: Unique ID of the project
        self.id = id

        #: Name of the project
        self.name = name

        #: Edition of the project (SHARED | STANDARD | ENTERPRISE)
        self.edition = edition

        #: Timestamp of when the project was created
        self.created_at = to_datetime(created_at)

        self._manager: Optional[Manager] = None

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)

    @classmethod
    def from_dict(cls, obj: Dict[str, Any], manager: Manager) -> 'Project':
        """
        Convert dictionary to a ``Project`` object.

        Parameters
        ----------
        obj : dict
            Key-value pairs to retrieve project information from
        manager : ClusterManager
            The ClusterManager the Project belongs to

        Returns
        -------
        :class:`Project`

        """
        out = cls(
            id=obj['projectID'],
            name=obj['name'],
            edition=obj.get('edition'),
            created_at=obj.get('createdAt'),
        )
        out._manager = manager
        return out
