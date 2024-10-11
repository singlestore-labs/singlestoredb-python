#!/usr/bin/env python
"""SingleStoreDB Cluster Management."""
from typing import Dict
from typing import Optional

from .manager import Manager
from .utils import vars_to_str


CODES = {
    'AWS': {
        'US East 1 (N. Virginia)': 'us-east-1',
        'US East 2 (Ohio)': 'us-east-2',
        'US West 1 (N. California)': 'us-west-1',
        'US West 2 (Oregon)': 'us-west-2',
        'Africa South 1 (Cape Town)': 'af-south-1',
        'Asia Pacific East 1 (Hong Kong)': 'ap-east-1',
        'Asia Pacific South 2 (Hyderabad)': 'ap-south-2',
        'Asia Pacific Southeast 3 (Jakarta)': 'ap-southeast-3',
        'Asia Pacific Southeast 5 (Malaysia)': 'ap-southeast-5',
        'Asia Pacific Southeast 4 (Melbourne)': 'ap-southeast-4',
        'Asia Pacific South 1 (Mumbai)': 'ap-south-1',
        'Asia Pacific Northeast 3 (Osaka)': 'ap-northeast-3',
        'Asia Pacific Northeaest 2 (Seoul)': 'ap-northeast-2',
        'Asia Pacific Southeast 1 (Singapore)': 'ap-southeast-1',
        'Asia Pacific Southeast 2 (Sydney)': 'ap-southeast-2',
        'Asia Pacific Northeast 1 (Tokyo)': 'ap-northeast-1',
        'Canada Central 1 (Central)': 'ca-central-1',
        'Canada West 1 (Calgary)': 'ca-west-1',
        'China North 1 (Beijing)': 'cn-north-1',
        'China Northwest 1 (Ningxia)': 'cn-northweast-1',
        'Europe Central 1 (Frankfurt)': 'eu-central-1',
        'Europe West 1 (Ireland)': 'eu-west-1',
        'Europe West 2 (London)': 'eu-west-2',
        'Europe South 1 (Milan)': 'eu-south-1',
        'Europe West 3 (Paris)': 'eu-west-3',
        'Europe South 2 (Spain)': 'eu-south-2',
        'Europe North 1 (Stockholm)': 'eu-north-1',
        'Europe Central 2 (Zurich)': 'eu-central-2',
        'Israel Central 1 (Tel Aviv)': 'il-central-1',
        'Middle East South 1 (Bahrain)': 'me-south-1',
        'Middle East 1 (UAE)': 'me-central-1',
        'South America East 1 (Sao Paulo)': 'sa-east-1',
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
