#!/usr/bin/env python
"""SingleStoreDB Workspace Management."""
import datetime
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

from .. import connection
from ..exceptions import ManagementError
from .manager import Manager
from .region import Region
from .utils import to_datetime
from .utils import vars_to_str


class Workspace(object):
    """
    SingleStoreDB workspace definition.

    This object is not instantiated directly. It is used in the results
    of API calls on the :class:`WorkspaceManager`. Workspaces are created using
    :meth:`WorkspaceManager.create_workspace`, or existing workspaces are
    accessed by either :attr:`WorkspaceManager.workspaces` or by calling
    :meth:`WorkspaceManager.get_workspace`.

    See Also
    --------
    :meth:`WorkspaceManager.create_workspace`
    :meth:`WorkspaceManager.get_workspace`
    :attr:`WorkspaceManager.workspaces`

    """

    def __init__(
        self, name: str, workspace_id: str,
        workspace_group: Union[str, 'WorkspaceGroup'],
        size: str, state: str,
        created_at: Union[str, datetime.datetime],
        terminated_at: Optional[Union[str, datetime.datetime]] = None,
        endpoint: Optional[str] = None,
    ):
        #: Name of the workspace
        self.name = name

        #: Unique ID of the workspace
        self.id = workspace_id

        #: Unique ID of the workspace group
        if isinstance(workspace_group, WorkspaceGroup):
            self.group_id = workspace_group.id
        else:
            self.group_id = workspace_group

        #: Size of the workspace in workspace size notation (S-00, S-1, etc.)
        self.size = size

        #: State of the workspace: PendingCreation, Transitioning, Active,
        #: Terminated, Suspended, Resuming, Failed
        self.state = state.strip()

        #: Timestamp of when the workspace was created
        self.created_at = to_datetime(created_at)

        #: Timestamp of when the workspace was terminated
        self.terminated_at = to_datetime(terminated_at)

        #: Hostname (or IP address) of the workspace database server
        self.endpoint = endpoint

        self._manager: Optional[WorkspaceManager] = None

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)

    @classmethod
    def from_dict(cls, obj: Dict[str, Any], manager: 'WorkspaceManager') -> 'Workspace':
        """
        Construct a Workspace from a dictionary of values.

        Parameters
        ----------
        obj : dict
            Dictionary of values
        manager : WorkspaceManager, optional
            The WorkspaceManager the Workspace belongs to

        Returns
        -------
        :class:`Workspace`

        """
        out = cls(
            name=obj['name'], workspace_id=obj['workspaceID'],
            workspace_group=obj['workspaceGroupID'],
            size=obj.get('size', 'Unknown'), state=obj['state'],
            created_at=obj['createdAt'], terminated_at=obj.get('terminatedAt'),
            endpoint=obj.get('endpoint'),
        )
        out._manager = manager
        return out

    def refresh(self) -> 'Workspace':
        """Update the object to the current state."""
        if self._manager is None:
            raise ManagementError(
                msg='No workspace manager is associated with this object.',
            )
        new_obj = self._manager.get_workspace(self.id)
        for name, value in vars(new_obj).items():
            setattr(self, name, value)
        return self

    def terminate(
        self,
        wait_on_terminated: bool = False,
        wait_interval: int = 10,
        wait_timeout: int = 600,
    ) -> None:
        """
        Terminate the workspace.

        Parameters
        ----------
        wait_on_terminated : bool, optional
            Wait for the workspace to go into 'Terminated' mode before returning
        wait_interval : int, optional
            Number of seconds between each server check
        wait_timeout : int, optional
            Total number of seconds to check server before giving up

        Raises
        ------
        ManagementError
            If timeout is reached

        """
        if self._manager is None:
            raise ManagementError(
                msg='No workspace manager is associated with this object.',
            )
        self._manager._delete(f'workspaces/{self.id}')
        if wait_on_terminated:
            self._manager._wait_on_state(
                self._manager.get_workspace(self.id),
                'Terminated', interval=wait_interval, timeout=wait_timeout,
            )
            self.refresh()

    def connect(self, **kwargs: Any) -> connection.Connection:
        """
        Create a connection to the database server for this workspace.

        Parameters
        ----------
        **kwargs : keyword-arguments, optional
            Parameters to the SingleStoreDB `connect` function except host
            and port which are supplied by the workspace object

        Returns
        -------
        :class:`Connection`

        """
        if not self.endpoint:
            raise ManagementError(
                msg='An endpoint has not been set in this workspace configuration',
            )
        kwargs['host'] = self.endpoint
        return connection.connect(**kwargs)


class WorkspaceGroup(object):
    """
    SingleStoreDB workspace group definition.

    This object is not instantiated directly. It is used in the results
    of API calls on the :class:`WorkspaceManager`. Workspace groups are created using
    :meth:`WorkspaceManager.create_workspace_group`, or existing workspace groups are
    accessed by either :attr:`WorkspaceManager.workspace_groups` or by calling
    :meth:`WorkspaceManager.get_workspace_group`.

    See Also
    --------
    :meth:`WorkspaceManager.create_workspace_group`
    :meth:`WorkspaceManager.get_workspace_group`
    :attr:`WorkspaceManager.workspace_groups`

    """

    def __init__(
        self, name: str, id: str,
        created_at: Union[str, datetime.datetime],
        region: Region,
        firewall_ranges: List[str],
        terminated_at: Optional[Union[str, datetime.datetime]],
    ):
        #: Name of the workspace group
        self.name = name

        #: Unique ID of the workspace group
        self.id = id

        #: Timestamp of when the workspace group was created
        self.created_at = to_datetime(created_at)

        #: Region of the cluster (see :class:`Region`)
        self.region = region

        #: List of allowed incoming IP addresses / ranges
        self.firewall_ranges = firewall_ranges

        #: Timestamp of when the workspace group was terminated
        self.terminated_at = to_datetime(terminated_at)

        self._manager: Optional[WorkspaceManager] = None

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)

    @classmethod
    def from_dict(
        cls, obj: Dict[str, Any], manager: 'WorkspaceManager',
    ) -> 'WorkspaceGroup':
        """
        Construct a WorkspaceGroup from a dictionary of values.

        Parameters
        ----------
        obj : dict
            Dictionary of values
        manager : WorkspaceManager, optional
            The WorkspaceManager the WorkspaceGroup belongs to

        Returns
        -------
        :class:`WorkspaceGroup`

        """
        out = cls(
            name=obj['name'], id=obj['workspaceGroupID'],
            created_at=obj['createdAt'],
            region=[x for x in manager.regions if x.id == obj['regionID']][0],
            firewall_ranges=obj.get('firewallRanges', []),
            terminated_at=obj.get('terminatedAt'),
        )
        out._manager = manager
        return out

    def refresh(self) -> 'WorkspaceGroup':
        """Update teh object to the current state."""
        if self._manager is None:
            raise ManagementError(
                msg='No workspace manager is associated with this object.',
            )
        new_obj = self._manager.get_workspace_group(self.id)
        for name, value in vars(new_obj).items():
            setattr(self, name, value)
        return self

    def update(
        self, name: Optional[str] = None,
        admin_password: Optional[str] = None,
        firewall_ranges: Optional[List[str]] = None,
    ) -> None:
        """
        Update the cluster definition.

        Parameters
        ----------
        name : str, optional
            Cluster name
        admim_password : str, optional
            Admin password for the cluster
        firewall_ranges : Sequence[str], optional
            List of allowed incoming IP addresses

        """
        if self._manager is None:
            raise ManagementError(
                msg='No workspace manager is associated with this object.',
            )
        data = {
            k: v for k, v in dict(
                name=name, adminPassword=admin_password,
                firewallRanges=firewall_ranges,
            ).items() if v is not None
        }
        self._manager._patch(f'workspaceGroups/{self.id}', json=data)
        self.refresh()

    def terminate(
        self, force: bool = False,
        wait_on_terminated: bool = False,
        wait_interval: int = 10,
        wait_timeout: int = 600,
    ) -> None:
        """
        Terminate the workspace group.

        Parameters
        ----------
        force : bool, optional
            Terminate a workspace group even if it has active workspaces
        wait_on_terminated : bool, optional
            Wait for the cluster to go into 'Terminated' mode before returning
        wait_interval : int, optional
            Number of seconds between each server check
        wait_timeout : int, optional
            Total number of seconds to check server before giving up

        Raises
        ------
        ManagementError
            If timeout is reached

        """
        if self._manager is None:
            raise ManagementError(
                msg='No workspace manager is associated with this object.',
            )
        self._manager._delete(f'workspaceGroups/{self.id}', params=dict(force=force))
        if wait_on_terminated:
            self._manager._wait_on_state(
                self._manager.get_workspace_group(self.id),
                'Terminated', interval=wait_interval, timeout=wait_timeout,
            )
            self.refresh()

    def create_workspace(
        self, name: str, size: Optional[str] = None,
        wait_on_active: bool = False, wait_interval: int = 10,
        wait_timeout: int = 600,
    ) -> Workspace:
        """
        Create a new workspace.

        Parameters
        ----------
        name : str
            Name of the workspace
        size : str, optional
            Workspace size in workspace size notation (S-00, S-1, etc.)
        wait_on_active : bool, optional
            Wait for the workspace to be active before returning
        wait_timeout : int, optional
            Maximum number of seconds to wait before raising an exception
            if wait=True
        wait_interval : int, optional
            Number of seconds between each polling interval

        Returns
        -------
        :class:`Workspace`

        """
        if self._manager is None:
            raise ManagementError(
                msg='No workspace manager is associated with this object.',
            )
        return self._manager.create_workspace(
            name=name, workspace_group=self, size=size, wait_on_active=wait_on_active,
            wait_interval=wait_interval, wait_timeout=wait_timeout,
        )

    @property
    def workspaces(self) -> List[Workspace]:
        """Return a list of available workspaces."""
        if self._manager is None:
            raise ManagementError(
                msg='No workspace manager is associated with this object.',
            )
        res = self._manager._get('workspaces', params=dict(workspaceGroupID=self.id))
        return [Workspace.from_dict(item, self._manager) for item in res.json()]


class WorkspaceManager(Manager):
    """
    SingleStoreDB workspace manager.

    This class should be instantiated using :func:`singlestoredb.manage_workspaces`.

    Parameters
    ----------
    access_token : str, optional
        The API key or other access token for the workspace management API
    version : str, optional
        Version of the API to use
    base_url : str, optional
        Base URL of the workspace management API

    See Also
    --------
    :func:`singlestoredb.manage_workspaces`

    """

    #: Workspace management API version if none is specified.
    default_version = 'v1'

    #: Base URL if none is specified.
    default_base_url = 'https://api.singlestore.com'

    #: Object type
    obj_type = 'workspace'

    @property
    def workspace_groups(self) -> List[WorkspaceGroup]:
        """Return a list of available workspace groups."""
        res = self._get('workspaceGroups')
        return [WorkspaceGroup.from_dict(item, self) for item in res.json()]

    @property
    def regions(self) -> List[Region]:
        """Return a list of available regions."""
        res = self._get('regions')
        return [Region.from_dict(item, self) for item in res.json()]

    def create_workspace_group(
        self, name: str, region: Union[str, Region],
        firewall_ranges: List[str], admin_password: Optional[str] = None,
    ) -> WorkspaceGroup:
        """
        Create a new workspace group.

        Parameters
        ----------
        name : str
            Name of the workspace group
        region : str or Region
            ID of the region where the workspace group should be created
        firewall_ranges : list[str]
            List of allowed CIDR ranges. An empty list indicates that all
            inbound requests are allowed.
        admin_password : str, optional
            Admin password for the workspace group. If no password is supplied,
            a password will be generated and retured in the response.

        Returns
        -------
        :class:`WorkspaceGroup`

        """
        if isinstance(region, Region):
            region = region.id
        res = self._post(
            'workspaceGroups', json=dict(
                name=name, regionID=region,
                adminPassword=admin_password,
                firewallRanges=firewall_ranges,
            ),
        )
        return self.get_workspace_group(res.json()['workspaceGroupID'])

    def create_workspace(
        self, name: str, workspace_group: Union[str, WorkspaceGroup],
        size: Optional[str] = None, wait_on_active: bool = False,
        wait_interval: int = 10, wait_timeout: int = 600,
    ) -> Workspace:
        """
        Create a new workspace.

        Parameters
        ----------
        name : str
            Name of the workspace
        workspace_group : str or WorkspaceGroup
            The workspace ID of the workspace
        size : str, optional
            Workspace size in workspace size notation (S-00, S-1, etc.)
        wait_on_active : bool, optional
            Wait for the workspace to be active before returning
        wait_timeout : int, optional
            Maximum number of seconds to wait before raising an exception
            if wait=True
        wait_interval : int, optional
            Number of seconds between each polling interval

        Returns
        -------
        :class:`Workspace`

        """
        if isinstance(workspace_group, WorkspaceGroup):
            workspace_group = workspace_group.id
        res = self._post(
            'workspaces', json=dict(
                name=name, workspaceGroupID=workspace_group,
                size=size,
            ),
        )
        out = self.get_workspace(res.json()['workspaceID'])
        if wait_on_active:
            out = self._wait_on_state(
                out, 'Active', interval=wait_interval,
                timeout=wait_timeout,
            )
        return out

    def get_workspace_group(self, id: str) -> WorkspaceGroup:
        """
        Retrieve a workspace group definition.

        Parameters
        ----------
        id : str
            ID of the workspace group

        Returns
        -------
        :class:`WorkspaceGroup`

        """
        res = self._get(f'workspaceGroups/{id}')
        return WorkspaceGroup.from_dict(res.json(), manager=self)

    def get_workspace(self, id: str) -> Workspace:
        """
        Retrieve a workspace definition.

        Parameters
        ----------
        id : str
            ID of the workspace

        Returns
        -------
        :class:`Workspace`

        """
        res = self._get(f'workspaces/{id}')
        return Workspace.from_dict(res.json(), manager=self)


def manage_workspaces(
    access_token: Optional[str] = None,
    version: str = WorkspaceManager.default_version,
    base_url: str = WorkspaceManager.default_base_url,
) -> WorkspaceManager:
    """
    Retrieve a SingleStoreDB workspace manager.

    Parameters
    ----------
    access_token : str, optional
        The API key or other access token for the workspace management API
    version : str, optional
        Version of the API to use
    base_url : str, optional
        Base URL of the workspace management API

    Returns
    -------
    :class:`WorkspaceManager`

    """
    return WorkspaceManager(access_token=access_token, base_url=base_url, version=version)
