#!/usr/bin/env python
"""SingleStoreDB Cluster Management."""
from typing import Dict
from typing import Optional

from .manager import Manager
from .utils import vars_to_str


CODES = {
    'AWS': {
        'US East (N. Virginia)': 'us-east-1',
        'US East (Ohio)': 'us-east-2',
        'US West (N. California)': 'us-west-1',
        'US West (Oregon)': 'us-west-2',
        'Africa (Cape Town)': 'af-south-1',
        'Asia Pacific (Hong Kong)': 'ap-east-1',
        'Asia Pacific (Hyderabad)': 'ap-south-2',
        'Asia Pacific (Jakarta)': 'ap-southeast-3',
        'Asia Pacific (Malaysia)': 'ap-southeast-5',
        'Asia Pacific (Melbourne)': 'ap-southeast-4',
        'Asia Pacific (Mumbai)': 'ap-south-1',
        'Asia Pacific (Osaka)': 'ap-northeast-3',
        'Asia Pacific (Seoul)': 'ap-northeast-2',
        'Asia Pacific (Singapore)': 'ap-southeast-1',
        'Asia Pacific (Sydney)': 'ap-southeast-2',
        'Asia Pacific (Tokyo)': 'ap-northeast-1',
        'Canada (Central)': 'ca-central-1',
        'Canada West (Calgary)': 'ca-west-1',
        'China (Beijing)': 'cn-north-1',
        'China (Ningxia)': 'cn-northweast-1',
        'Europe (Frankfurt)': 'eu-central-1',
        'Europe (Ireland)': 'eu-west-1',
        'Europe (London)': 'eu-west-2',
        'Europe (Milan)': 'eu-south-1',
        'Europe (Paris)': 'eu-west-3',
        'Europe (Spain)': 'eu-south-2',
        'Europe (Stockholm)': 'eu-north-1',
        'Europe (Zurich)': 'eu-central-2',
        'Israel (Tel Aviv)': 'il-central-1',
        'Middle East (Bahrain)': 'me-south-1',
        'Middle East (UAE)': 'me-central-1',
        'South America (São Paulo)': 'sa-east-1',
    },
}


class Region(object):
    """
    Cluster region information.

    This object is not directly instantiated. It is used in results
    of ``WorkspaceManager`` API calls.

    See Also
    --------
    :attr:`WorkspaceManager.regions`

    """

    def __init__(self, id: str, name: str, provider: str):
        """Use :attr:`WorkspaceManager.regions` instead."""
        #: Unique ID of the region
        self.id = id

        #: Name of the region
        self.name = name

        #: Name of the cloud provider
        self.provider = provider

        self._manager: Optional[Manager] = None

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)

    @classmethod
    def from_dict(cls, obj: Dict[str, str], manager: Manager) -> 'Region':
        """
        Convert dictionary to a ``Region`` object.

        Parameters
        ----------
        obj : dict
            Key-value pairs to retrieve region information from
        manager : WorkspaceManager, optional
            The WorkspaceManager the Region belongs to

        Returns
        -------
        :class:`Region`

        """
        out = cls(
            id=obj['regionID'],
            name=obj['region'],
            provider=obj['provider'],
        )
        out._manager = manager
        return out

    @property
    def code(self) -> str:
        return CODES[self.provider][self.name]
