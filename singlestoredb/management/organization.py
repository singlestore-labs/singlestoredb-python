#!/usr/bin/env python
"""SingleStoreDB Cloud Organization."""
from __future__ import annotations

import datetime
from typing import Any
from typing import cast
from typing import Dict
from typing import List
from typing import Optional
from typing import TYPE_CHECKING
from typing import Union

from ..exceptions import ManagementError
from .inference_api import InferenceAPIManager
from .job import JobsManager
from .manager import Manager
from .utils import NamedList
from .utils import require_fields
from .utils import to_datetime
from .utils import vars_to_str

if TYPE_CHECKING:
    from .workspace import WorkspaceManager


def listify(x: Union[str, List[str]]) -> List[str]:
    if isinstance(x, list):
        return x
    return [x]


def stringify(x: Union[str, List[str]]) -> str:
    if isinstance(x, list):
        return x[0]
    return x


class Secret(object):
    """
    SingleStoreDB secrets definition.

    This object is not directly instantiated. It is used in results
    of API calls on the :class:`WorkspaceManager`. Secrets are created using
    :meth:`WorkspaceManager.create_secret`, or existing secrets are accessed
    by either :attr:`WorkspaceManager.secrets` or by calling
    :meth:`WorkspaceManager.get_secret_by_id`.

    See Also
    --------
    :meth:`WorkspaceManager.create_secret`
    :meth:`WorkspaceManager.get_secret_by_id`
    :attr:`WorkspaceManager.secrets`

    """

    id: str
    name: str
    value: Optional[str]
    created_by: str
    created_at: Optional[datetime.datetime]
    last_updated_by: Optional[str]
    last_updated_at: Optional[datetime.datetime]
    deleted_by: Optional[str]
    deleted_at: Optional[datetime.datetime]

    def __init__(
        self,
        id: str,
        name: str,
        created_by: str,
        created_at: Optional[datetime.datetime] = None,
        last_updated_by: Optional[str] = None,
        last_updated_at: Optional[datetime.datetime] = None,
        value: Optional[str] = None,
        deleted_by: Optional[str] = None,
        deleted_at: Optional[datetime.datetime] = None,
    ):
        #: UUID of the secret
        self.id = id

        #: Name of the secret
        self.name = name

        #: Value of the secret
        self.value = value

        #: User who created the secret
        self.created_by = created_by

        #: Time when the secret was created
        self.created_at = created_at

        #: UUID of the user who last updated the secret
        self.last_updated_by = last_updated_by

        #: Time when the secret was last updated
        self.last_updated_at = last_updated_at

        #: UUID of the user who deleted the secret
        self.deleted_by = deleted_by

        #: Time when the secret was deleted
        self.deleted_at = deleted_at

        self._manager: Optional[Manager] = None

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)

    @classmethod
    def from_dict(
        cls,
        obj: Dict[str, Any],
        manager: Manager,
    ) -> 'Secret':
        """
        Construct a Secret from a dictionary of values.

        Parameters
        ----------
        obj : dict
            Dictionary of values
        manager : Manager
            The Manager the Secret belongs to

        Returns
        -------
        :class:`Secret`

        """
        require_fields(obj, 'secretID', 'name', 'createdBy')
        out = cls(
            id=obj['secretID'],
            name=obj['name'],
            created_by=obj['createdBy'],
            created_at=to_datetime(obj.get('createdAt')),
            last_updated_by=obj.get('lastUpdatedBy'),
            last_updated_at=to_datetime(obj.get('lastUpdatedAt')),
            value=obj.get('value'),
            deleted_by=obj.get('deletedBy'),
            deleted_at=to_datetime(obj.get('deletedAt')),
        )
        out._manager = manager
        return out

    def update(
        self,
        name: Optional[str] = None,
        value: Optional[str] = None,
    ) -> None:
        """
        Update the secret.

        Parameters
        ----------
        name : str, optional
            New name for the secret
        value : str, optional
            New value for the secret

        """
        if self._manager is None:
            raise ManagementError(
                msg='No workspace manager is associated with this object.',
            )

        data: Dict[str, Any] = {}
        if name is not None:
            data['name'] = name
        if value is not None:
            data['value'] = value

        if data:
            self._manager._patch(f'secrets/{self.id}', json=data)
            # Update local state
            if name is not None:
                self.name = name
            if value is not None:
                self.value = value

    def delete(self) -> None:
        """Delete the secret."""
        if self._manager is None:
            raise ManagementError(
                msg='No workspace manager is associated with this object.',
            )
        self._manager._delete(f'secrets/{self.id}')

    def get_access_controls(self) -> Dict[str, Any]:
        """
        Get access control information for this secret.

        Returns
        -------
        dict
            Access control information including grants

        """
        if self._manager is None:
            raise ManagementError(
                msg='No workspace manager is associated with this object.',
            )
        res = self._manager._get(f'secrets/{self.id}/accessControls')
        return res.json()

    def update_access_controls(
        self,
        grants: Optional[List[Dict[str, Any]]] = None,
        revokes: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        """
        Update access controls for this secret.

        Parameters
        ----------
        grants : list of dict, optional
            List of grants to add, each with 'identity' and 'role' keys
        revokes : list of dict, optional
            List of revokes to remove, each with 'identity' and 'role' keys

        """
        if self._manager is None:
            raise ManagementError(
                msg='No workspace manager is associated with this object.',
            )

        data: Dict[str, Any] = {}
        if grants is not None:
            data['grants'] = grants
        if revokes is not None:
            data['revokes'] = revokes

        if data:
            self._manager._patch(f'secrets/{self.id}/accessControls', json=data)


class SecretsMixin:
    """Mixin class that adds secret management methods to WorkspaceManager."""

    @property
    def secrets(self) -> NamedList[Secret]:
        """Return a list of all secrets in the organization."""
        manager = cast('WorkspaceManager', self)
        res = manager._get('secrets')
        return NamedList([
            Secret.from_dict(item, manager)
            for item in res.json().get('secrets', [])
        ])

    def get_secret_by_id(self, secret_id: str) -> Secret:
        """
        Retrieve a secret by ID.

        Parameters
        ----------
        secret_id : str
            ID of the secret

        Returns
        -------
        :class:`Secret`

        """
        manager = cast('WorkspaceManager', self)
        res = manager._get(f'secrets/{secret_id}')
        return Secret.from_dict(res.json(), manager)

    def create_secret(self, name: str, value: str) -> Secret:
        """
        Create a new secret.

        Parameters
        ----------
        name : str
            Name of the secret
        value : str
            Value of the secret

        Returns
        -------
        :class:`Secret`

        """
        manager = cast('WorkspaceManager', self)
        data = {'name': name, 'value': value}
        res = manager._post('secrets', json=data)
        return self.get_secret_by_id(res.json()['secretID'])


class Organization(object):
    """
    Organization in SingleStoreDB Cloud portal.

    This object is not directly instantiated. It is used in results
    of ``WorkspaceManager`` API calls.

    See Also
    --------
    :attr:`WorkspaceManager.organization`

    """

    id: str
    name: str
    firewall_ranges: List[str]

    def __init__(self, id: str, name: str, firewall_ranges: List[str]):
        """Use :attr:`WorkspaceManager.organization` instead."""
        #: Unique ID of the organization
        self.id = id

        #: Name of the organization
        self.name = name

        #: Firewall ranges of the organization
        self.firewall_ranges = list(firewall_ranges)

        self._manager: Optional[Manager] = None

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)

    def get_secret(self, name: str) -> Secret:
        if self._manager is None:
            raise ManagementError(msg='Organization not initialized')

        res = self._manager._get('secrets', params=dict(name=name))

        secrets = [
            Secret.from_dict(item, self._manager)
            for item in res.json()['secrets']
        ]

        if len(secrets) == 0:
            raise ManagementError(msg=f'Secret {name} not found')

        if len(secrets) > 1:
            raise ManagementError(msg=f'Multiple secrets found for {name}')

        return secrets[0]

    @classmethod
    def from_dict(
        cls,
        obj: Dict[str, Union[str, List[str]]],
        manager: Manager,
    ) -> 'Organization':
        """
        Convert dictionary to an ``Organization`` object.

        Parameters
        ----------
        obj : dict
            Key-value pairs to retrieve organization information from
        manager : WorkspaceManager, optional
            The WorkspaceManager the Organization belongs to

        Returns
        -------
        :class:`Organization`

        """
        out = cls(
            id=stringify(obj['orgID']),
            name=stringify(obj.get('name', '<unknown>')),
            firewall_ranges=listify(obj.get('firewallRanges', [])),
        )
        out._manager = manager
        return out

    @property
    def jobs(self) -> JobsManager:
        """
        Retrieve a SingleStoreDB scheduled job manager.

        Parameters
        ----------
        manager : WorkspaceManager, optional
            The WorkspaceManager the JobsManager belongs to

        Returns
        -------
        :class:`JobsManager`
        """
        return JobsManager(self._manager)

    @property
    def inference_apis(self) -> InferenceAPIManager:
        """
        Retrieve a SingleStoreDB inference api manager.

        Parameters
        ----------
        manager : WorkspaceManager, optional
            The WorkspaceManager the InferenceAPIManager belongs to

        Returns
        -------
        :class:`InferenceAPIManager`
        """
        return InferenceAPIManager(self._manager)

    def get_access_controls(self) -> Dict[str, Any]:
        """
        Get access control information for this organization.

        Returns
        -------
        dict
            Access control information including grants

        """
        if self._manager is None:
            raise ManagementError(msg='Organization not initialized')
        res = self._manager._get(f'organizations/{self.id}/accessControls')
        return res.json()

    def update_access_controls(
        self,
        grants: Optional[List[Dict[str, Any]]] = None,
        revokes: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        """
        Update access controls for this organization.

        Parameters
        ----------
        grants : list of dict, optional
            List of grants to add, each with 'identity' and 'role' keys
        revokes : list of dict, optional
            List of revokes to remove, each with 'identity' and 'role' keys

        """
        if self._manager is None:
            raise ManagementError(msg='Organization not initialized')

        data: Dict[str, Any] = {}
        if grants is not None:
            data['grants'] = grants
        if revokes is not None:
            data['revokes'] = revokes

        if data:
            self._manager._patch(
                f'organizations/{self.id}/accessControls',
                json=data,
            )
