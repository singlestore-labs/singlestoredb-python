#!/usr/bin/env python3
import os
from typing import Any
from typing import Dict
from typing import Optional

from .. import result
from ..handler import SQLHandler
from ..result import FusionSQLResult
from .files import ShowFilesHandler
from .utils import get_file_space
from .utils import get_inference_api
from .utils import get_inference_api_manager


class ShowCustomModelsHandler(ShowFilesHandler):
    """
    SHOW CUSTOM MODELS
        [ at_path ] [ <like> ]
        [ <order-by> ]
        [ <limit> ] [ recursive ] [ extended ];

    # File path to list
    at_path = AT '<path>'

    # Should the listing be recursive?
    recursive = RECURSIVE

    # Should extended attributes be shown?
    extended = EXTENDED

    Description
    -----------
    Displays the list of models in models space.

    Arguments
    ---------
    * ``<path>``: A path in the models space.
    * ``<pattern>``: A pattern similar to SQL LIKE clause.
      Uses ``%`` as the wildcard character.

    Remarks
    -------
    * Use the ``LIKE`` clause to specify a pattern and return only the
      files that match the specified pattern.
    * The ``LIMIT`` clause limits the number of results to the
      specified number.
    * Use the ``ORDER BY`` clause to sort the results by the specified
      key. By default, the results are sorted in the ascending order.
    * The ``AT`` clause specifies the path in the models
      space to list the files from.
    * To return more information about the files, use the ``EXTENDED``
      clause.

    Examples
    --------
    The following command lists the models::

        SHOW CUSTOM MODELS;

    The following command lists the models with additional information::

        SHOW CUSTOM MODELS EXTENDED;

    See Also
    --------
    * ``UPLOAD CUSTOM MODEL model_name FROM path``
    * ``DOWNLOAD CUSTOM MODEL model_name``


    """  # noqa: E501

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        params['file_location'] = 'MODELS'

        return super().run(params)


ShowCustomModelsHandler.register(overwrite=True)


class UploadCustomModelHandler(SQLHandler):
    """
    UPLOAD CUSTOM MODEL model_name
        FROM local_path [ overwrite ];

    # Model Name
    model_name = '<model-name>'

    # Path to local file or directory
    local_path = '<local-path>'

    # Should an existing file be overwritten?
    overwrite = OVERWRITE

    Description
    -----------
    Uploads a file or folder to models space.

    Arguments
    ---------
    * ``<model-name>``: Model name.
    * ``<local-path>``: The path to the file or folder to upload in the local
      directory.

    Remarks
    -------
    * If the ``OVERWRITE`` clause is specified, any existing file at the
      specified path in the models space is overwritten.

    Examples
    --------
    The following command uploads a file to models space and overwrite any
    existing files at the specified path::

        UPLOAD CUSTOM MODEL model_name
            FROM 'llama3/' OVERWRITE;

    See Also
    --------
    * ``DOWNLOAD CUSTOM MODEL model_name``

    """  # noqa: E501

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        params['file_location'] = 'MODELS'

        model_name = params['model_name']
        local_path = params['local_path']

        file_space = get_file_space(params)

        if os.path.isdir(local_path):
            file_space.upload_folder(
                local_path=local_path,
                path=os.path.join(model_name, ''),
                overwrite=params['overwrite'],
            )
        else:
            file_space.upload_file(
                local_path=local_path,
                path=os.path.join(model_name, local_path),
                overwrite=params['overwrite'],
            )

        return None


UploadCustomModelHandler.register(overwrite=True)


class DownloadCustomModelHandler(SQLHandler):
    """
    DOWNLOAD CUSTOM MODEL model_name
        [ local_path ]
        [ overwrite ];

    # Model Name
    model_name = '<model-name>'

    # Path to local directory
    local_path = TO '<local-path>'

    # Should an existing directory be overwritten?
    overwrite = OVERWRITE

    Description
    -----------
    Download a model from models space.

    Arguments
    ---------
    * ``<model-name>``: Model name to download in models space.
    * ``<local-path>``: Specifies the path in the local directory
      where the model is downloaded.

    Remarks
    -------
    * If the ``OVERWRITE`` clause is specified, any existing file or folder at
      the download location is overwritten.
    * If ``<local-path>`` is not specified, the model is downloaded to the current location.

    Examples
    --------
    The following command displays the contents of the file on the
    standard output::

        DOWNLOAD CUSTOM MODEL llama3;

    The following command downloads a model to a specific location and
    overwrites any existing models folder with the name ``local_llama3`` on the local storage::

        DOWNLOAD CUSTOM MODEL llama3
            TO 'local_llama3' OVERWRITE;

    See Also
    --------
    * ``UPLOAD CUSTOM MODEL model_name FROM local_path``

    """  # noqa: E501

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        params['file_location'] = 'MODELS'

        file_space = get_file_space(params)

        model_name = params['model_name']
        file_space.download_folder(
            path=os.path.join(model_name, ''),
            local_path=params['local_path'] or model_name,
            overwrite=params['overwrite'],
        )

        return None


DownloadCustomModelHandler.register(overwrite=True)


class DropCustomModelHandler(SQLHandler):
    """
    DROP CUSTOM MODEL model_name;

    # Model Name
    model_name = '<model-name>'

    Description
    -----------
    Deletes a model from models space.

    Arguments
    ---------
    * ``<model-name>``: Model name to delete in models space.

    Example
    --------
    The following commands deletes a model from a model space::

        DROP CUSTOM MODEL llama3;

    """  # noqa: E501

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        params['file_location'] = 'MODELS'
        path = os.path.join(params['model_name'], '')

        file_space = get_file_space(params)
        file_space.removedirs(path=path)

        return None


DropCustomModelHandler.register(overwrite=True)


class StartModelHandler(SQLHandler):
    """
    START MODEL model_name ;

    # Model Name
    model_name = '<model-name>'

    Description
    -----------
    Starts an inference API model.

    Arguments
    ---------
    * ``<model-name>``: Name of the model to start.

    Example
    --------
    The following command starts a model::

        START MODEL my_model;

    See Also
    --------
    * ``STOP MODEL model_name``
    * ``SHOW MODELS``

    """  # noqa: E501

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        inference_api = get_inference_api(params)
        operation_result = inference_api.start()

        res = FusionSQLResult()
        res.add_field('Status', result.STRING)
        res.add_field('Message', result.STRING)
        res.set_rows([
            (
                operation_result.status,
                operation_result.get_message(),
            ),
        ])

        return res


StartModelHandler.register(overwrite=True)


class StopModelHandler(SQLHandler):
    """
    STOP MODEL model_name ;

    # Model Name
    model_name = '<model-name>'

    Description
    -----------
    Stops an inference API model.

    Arguments
    ---------
    * ``<model-name>``: Name of the model to stop.

    Example
    --------
    The following command stops a model::

        STOP MODEL my_model;

    See Also
    --------
    * ``START MODEL model_name``
    * ``SHOW MODELS``

    """  # noqa: E501

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        inference_api = get_inference_api(params)
        operation_result = inference_api.stop()

        res = FusionSQLResult()
        res.add_field('Status', result.STRING)
        res.add_field('Message', result.STRING)
        res.set_rows([
            (
                operation_result.status,
                operation_result.get_message(),
            ),
        ])

        return res


StopModelHandler.register(overwrite=True)


class ShowModelsHandler(SQLHandler):
    """
    SHOW MODELS ;

    Description
    -----------
    Displays the list of inference APIs in the current project.

    Example
    --------
    The following command lists all inference APIs::

        SHOW MODELS;

    See Also
    --------
    * ``START MODEL model_name``
    * ``STOP MODEL model_name``
    * ``DROP MODEL model_name``

    """  # noqa: E501

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        inference_api_manager = get_inference_api_manager()
        models = inference_api_manager.show()

        res = FusionSQLResult()
        res.add_field('Model Name', result.STRING)
        res.add_field('Status', result.STRING)

        rows = []
        for model in models:
            rows.append((
                model.name,
                model.status,
            ))

        res.set_rows(rows)
        return res


ShowModelsHandler.register(overwrite=True)


class DropModelHandler(SQLHandler):
    """
    DROP MODEL model_name ;

    # Model Name
    model_name = '<model-name>'

    Description
    -----------
    Drops (deletes) an inference API model.

    Arguments
    ---------
    * ``<model-name>``: Name of the model to drop.

    Example
    --------
    The following command drops an inference API::

        DROP MODEL my_model;

    See Also
    --------
    * ``START MODEL model_name``
    * ``STOP MODEL model_name``
    * ``SHOW MODELS``

    """  # noqa: E501

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        inference_api = get_inference_api(params)
        operation_result = inference_api.drop()

        res = FusionSQLResult()
        res.add_field('Model Name', result.STRING)
        res.add_field('Status', result.STRING)
        res.add_field('Message', result.STRING)
        res.set_rows([
            (
                operation_result.name,
                operation_result.status,
                operation_result.get_message(),
            ),
        ])

        return res


DropModelHandler.register(overwrite=True)
