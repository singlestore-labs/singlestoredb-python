#!/usr/bin/env python
"""SingleStoreDB Cloud Inference API."""
import os
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

from .utils import vars_to_str
from singlestoredb.exceptions import ManagementError
from singlestoredb.management.manager import Manager


class ModelOperationResult(object):
    """
    Result of a model start or stop operation.

    Attributes
    ----------
    name : str
        Name of the model
    status : str
        Current status of the model (e.g., 'Active', 'Initializing', 'Suspended')
    hosting_platform : str
        Hosting platform (e.g., 'Nova', 'Amazon', 'Azure')
    """

    def __init__(
        self,
        name: str,
        status: str,
        hosting_platform: str,
    ):
        self.name = name
        self.status = status
        self.hosting_platform = hosting_platform

    @classmethod
    def from_start_response(cls, response: Dict[str, Any]) -> 'ModelOperationResult':
        """
        Create a ModelOperationResult from a start operation response.

        Parameters
        ----------
        response : dict
            Response from the start endpoint

        Returns
        -------
        ModelOperationResult

        """
        return cls(
            name=response.get('modelName', ''),
            status='Initializing',
            hosting_platform=response.get('hostingPlatform', ''),
        )

    @classmethod
    def from_stop_response(cls, response: Dict[str, Any]) -> 'ModelOperationResult':
        """
        Create a ModelOperationResult from a stop operation response.

        Parameters
        ----------
        response : dict
            Response from the stop endpoint

        Returns
        -------
        ModelOperationResult

        """
        return cls(
            name=response.get('name', ''),
            status=response.get('status', 'Suspended'),
            hosting_platform=response.get('hostingPlatform', ''),
        )

    @classmethod
    def from_drop_response(cls, response: Dict[str, Any]) -> 'ModelOperationResult':
        """
        Create a ModelOperationResult from a drop operation response.

        Parameters
        ----------
        response : dict
            Response from the drop endpoint

        Returns
        -------
        ModelOperationResult

        """
        return cls(
            name=response.get('name', ''),
            status=response.get('status', 'Deleted'),
            hosting_platform=response.get('hostingPlatform', ''),
        )

    @classmethod
    def from_show_response(cls, response: Dict[str, Any]) -> 'ModelOperationResult':
        """
        Create a ModelOperationResult from a show operation response.

        Parameters
        ----------
        response : dict
            Response from the show endpoint (single model info)

        Returns
        -------
        ModelOperationResult

        """
        return cls(
            name=response.get('name', ''),
            status=response.get('status', ''),
            hosting_platform=response.get('hostingPlatform', ''),
        )

    def get_message(self) -> str:
        """
        Get a human-readable message about the operation.

        Returns
        -------
        str
            Message describing the operation result

        """
        return f'Model is {self.status}'

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)


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
    internal_connection_url: str
    project_id: str
    hosting_platform: str
    _manager: Optional['InferenceAPIManager']

    def __init__(
        self,
        service_id: str,
        model_name: str,
        name: str,
        connection_url: str,
        internal_connection_url: str,
        project_id: str,
        hosting_platform: str,
        manager: Optional['InferenceAPIManager'] = None,
    ):
        self.service_id = service_id
        self.connection_url = connection_url
        self.internal_connection_url = internal_connection_url
        self.model_name = model_name
        self.name = name
        self.project_id = project_id
        self.hosting_platform = hosting_platform
        self._manager = manager

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
            internal_connection_url=obj['internalConnectionURL'],
            hosting_platform=obj['hostingPlatform'],
        )
        return out

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)

    def start(self) -> ModelOperationResult:
        """
        Start this inference API model.

        Returns
        -------
        ModelOperationResult
            Result object containing status information about the started model

        """
        if self._manager is None:
            raise ManagementError(msg='No manager associated with this inference API')
        return self._manager.start(self.name)

    def stop(self) -> ModelOperationResult:
        """
        Stop this inference API model.

        Returns
        -------
        ModelOperationResult
            Result object containing status information about the stopped model

        """
        if self._manager is None:
            raise ManagementError(msg='No manager associated with this inference API')
        return self._manager.stop(self.name)

    def drop(self) -> ModelOperationResult:
        """
        Drop this inference API model.

        Returns
        -------
        ModelOperationResult
            Result object containing status information about the dropped model

        """
        if self._manager is None:
            raise ManagementError(msg='No manager associated with this inference API')
        return self._manager.drop(self.name)


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
        inference_api = InferenceAPIInfo.from_dict(res)
        inference_api._manager = self  # Associate the manager
        return inference_api

    def start(self, model_name: str) -> ModelOperationResult:
        """
        Start an inference API model.

        Parameters
        ----------
        model_name : str
            Name of the model to start

        Returns
        -------
        ModelOperationResult
            Result object containing status information about the started model

        """
        if self._manager is None:
            raise ManagementError(msg='Manager not initialized')
        res = self._manager._post(f'models/{model_name}/start')
        return ModelOperationResult.from_start_response(res.json())

    def stop(self, model_name: str) -> ModelOperationResult:
        """
        Stop an inference API model.

        Parameters
        ----------
        model_name : str
            Name of the model to stop

        Returns
        -------
        ModelOperationResult
            Result object containing status information about the stopped model

        """
        if self._manager is None:
            raise ManagementError(msg='Manager not initialized')
        res = self._manager._post(f'models/{model_name}/stop')
        return ModelOperationResult.from_stop_response(res.json())

    def show(self) -> List[ModelOperationResult]:
        """
        Show all inference APIs in the project.

        Returns
        -------
        List[ModelOperationResult]
            List of ModelOperationResult objects with status information

        """
        if self._manager is None:
            raise ManagementError(msg='Manager not initialized')
        res = self._manager._get('models').json()
        return [ModelOperationResult.from_show_response(api) for api in res]

    def drop(self, model_name: str) -> ModelOperationResult:
        """
        Drop an inference API model.

        Parameters
        ----------
        model_name : str
            Name of the model to drop

        Returns
        -------
        ModelOperationResult
            Result object containing status information about the dropped model

        """
        if self._manager is None:
            raise ManagementError(msg='Manager not initialized')
        res = self._manager._delete(f'models/{model_name}')
        return ModelOperationResult.from_drop_response(res.json())
