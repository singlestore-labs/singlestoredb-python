#!/usr/bin/env python
"""SingleStoreDB Cloud Inference API."""
import os
from typing import Any
from typing import Dict
from typing import Optional

from .utils import vars_to_str
from singlestoredb.exceptions import ManagementError
from singlestoredb.management.manager import Manager


class InferenceAPIInfo(object):
    """
    Inference API definition.

    This object is not directly instantiated. It is used in results
    of API calls on the :class:`InferenceAPIManager`. See :meth:`InferenceAPIManager.get`.
    """

    service_id: str
    model_name: str
    name: str
    connection_url: str
    project_id: str

    def __init__(
        self,
        service_id: str,
        model_name: str,
        name: str,
        connection_url: str,
        project_id: str,
    ):
        self.service_id = service_id
        self.connection_url = connection_url
        self.model_name = model_name
        self.name = name
        self.project_id = project_id

    @classmethod
    def from_dict(
        cls,
        obj: Dict[str, Any],
    ) -> 'InferenceAPIInfo':
        """
        Construct a Inference API from a dictionary of values.

        Parameters
        ----------
        obj : dict
            Dictionary of values

        Returns
        -------
        :class:`Job`

        """
        out = cls(
            service_id=obj['serviceID'],
            project_id=obj['projectID'],
            model_name=obj['modelName'],
            name=obj['name'],
            connection_url=obj['connectionURL'],
        )
        return out

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)


class InferenceAPIManager(object):
    """
    SingleStoreDB Inference APIs manager.

    This class should be instantiated using :attr:`Organization.inference_apis`.

    Parameters
    ----------
    manager : InferenceAPIManager, optional
        The InferenceAPIManager the InferenceAPIManager belongs to

    See Also
    --------
    :attr:`InferenceAPI`
    """

    def __init__(self, manager: Optional[Manager]):
        self._manager = manager
        self.project_id = os.environ.get('SINGLESTOREDB_PROJECT')

    def get(self, model_name: str) -> InferenceAPIInfo:
        if self._manager is None:
            raise ManagementError(msg='Manager not initialized')
        res = self._manager._get(f'inferenceapis/{self.project_id}/{model_name}').json()
        return InferenceAPIInfo.from_dict(res)
