#!/usr/bin/env python
"""SingleStoreDB Cloud Organization."""
import datetime
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Type
from typing import Union

from ..exceptions import ManagementError
from ._version_import import _versioned_attr
from .job import JobsManager
from .manager import Manager
from .utils import to_datetime
from .utils import vars_to_str


def get_organization(version: Optional[str] = None) -> 'Organization':
    """
    Get the current organization.

    Parameters
    ----------
    version : str, optional
        Version of the API to use. Defaults to the ``management.version``
        option (the ``SINGLESTOREDB_MANAGEMENT_VERSION`` environment
        variable).

    Returns
    -------
    :class:`Organization`

    """
    return _versioned_attr('get_organization', version)()


def get_secret(name: str, version: Optional[str] = None) -> Optional[str]:
    """
    Get the value of a secret in the current organization.

    Parameters
    ----------
    name : str
        Name of the secret
    version : str, optional
        Version of the API to use. Defaults to the ``management.version``
        option (the ``SINGLESTOREDB_MANAGEMENT_VERSION`` environment
        variable).

    Returns
    -------
    str or None

    """
    return _versioned_attr('get_secret', version)(name)


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
    of API calls on the :class:`Organization`. See :meth:`Organization.get_secret`.
    """

    def __init__(
        self,
        id: str,
        name: str,
        created_by: str,
        created_at: Optional[Union[str, datetime.datetime]],
        last_updated_by: str,
        last_updated_at: Optional[Union[str, datetime.datetime]],
        value: Optional[str] = None,
        deleted_by: Optional[str] = None,
        deleted_at: Optional[Union[str, datetime.datetime]] = None,
    ):
        # UUID of the secret
        self.id = id

        # Name of the secret
        self.name = name

        # Value of the secret
        self.value = value

        # User who created the secret
        self.created_by = created_by

        # Time when the secret was created
        self.created_at = created_at

        # UUID of the user who last updated the secret
        self.last_updated_by = last_updated_by

        # Time when the secret was last updated
        self.last_updated_at = last_updated_at

        # UUID of the user who deleted the secret
        self.deleted_by = deleted_by

        # Time when the secret was deleted
        self.deleted_at = deleted_at

    @classmethod
    def from_dict(cls, obj: Dict[str, str]) -> 'Secret':
        """
        Construct a Secret from a dictionary of values.

        Parameters
        ----------
        obj : dict
            Dictionary of values

        Returns
        -------
        :class:`Secret`

        """
        out = cls(
            id=obj['secretID'],
            name=obj['name'],
            created_by=obj['createdBy'],
            created_at=to_datetime(obj.get('createdAt')),
            last_updated_by=obj['lastUpdatedBy'],
            last_updated_at=to_datetime(obj.get('lastUpdatedAt')),
            value=obj.get('value'),
            deleted_by=obj.get('deletedBy'),
            deleted_at=to_datetime(obj.get('deletedAt')),
        )

        return out

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)


class Organization:
    """
    Organization in SingleStoreDB Cloud portal.

    This object is not directly instantiated. It is used in results
    of ``ClusterManager`` API calls.

    See Also
    --------
    :attr:`ClusterManager.organization`

    """

    id: str
    name: str
    firewall_ranges: List[str]

    #: Sub-manager classes reached through this organization. The
    #: ``organizations/current`` and ``secrets`` routes are identical at v1 and
    #: v2, so ``Organization`` itself is version-neutral; only the managers it
    #: hands out differ. These name the current-version managers, and
    #: ``v1/organization.py`` repoints them back to the v1 classes.
    _jobs_manager_class: Type[JobsManager] = JobsManager

    #: Inference API manager class, or ``None`` if the version has no
    #: inference routes. There are none from v2 onward.
    _inference_api_manager_class: Optional[Type[Any]] = None

    def __init__(self, id: str, name: str, firewall_ranges: List[str]):
        """Use :attr:`ClusterManager.organization` instead."""
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

        secrets = [Secret.from_dict(item) for item in res.json()['secrets']]

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
        manager : ClusterManager, optional
            The ClusterManager the Organization belongs to

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
        manager : ClusterManager, optional
            The ClusterManager the JobsManager belongs to

        Returns
        -------
        :class:`JobsManager`
        """
        return self._jobs_manager_class(self._manager)

    @property
    def inference_apis(self) -> Any:
        """
        Retrieve a SingleStoreDB inference api manager.

        Returns
        -------
        :class:`InferenceAPIManager`

        Raises
        ------
        ManagementError
            If the API version has no inference routes

        """
        if self._inference_api_manager_class is None:
            raise ManagementError(
                msg='The inference API is not available in this version of '
                    'the management API. None of the inferenceapis/ routes '
                    'exist past v1.',
            )
        return self._inference_api_manager_class(self._manager)


class Organizations(object):
    """Organizations."""

    #: The ``Organization`` class this hands out. Version subclasses repoint
    #: this so the organization carries the right sub-managers.
    _organization_class: Type[Organization] = Organization

    def __init__(self, manager: Manager):
        self._manager = manager

    @property
    def current(self) -> Organization:
        """Get current organization."""
        res = self._manager._get('organizations/current').json()
        return self._organization_class.from_dict(res, self._manager)
