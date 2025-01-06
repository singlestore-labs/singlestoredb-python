#!/usr/bin/env python3
import os
from typing import Any
from typing import Dict
from typing import Optional

from ..handler import SQLHandler
from ..result import FusionSQLResult
from .files import ShowFilesHandler
from .utils import get_file_space


class ShowModelsHandler(ShowFilesHandler):
    """
    SHOW MODELS
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
    * The ``AT PATH`` clause specifies the path in the models
      space to list the files from.
    * To return more information about the files, use the ``EXTENDED``
      clause.

    Examples
    --------
    The following command lists the models::

        SHOW MODELS;

    The following command lists the models with additional information::

        SHOW MODELS EXTENDED;

    See Also
    --------
    * ``UPLOAD MODEL model_name FROM path``
    * ``DOWNLOAD MODEL model_name``


    """  # noqa: E501

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        params['file_location'] = 'MODELS'

        return super().run(params)


ShowModelsHandler.register(overwrite=True)


class UploadModelHandler(SQLHandler):
    """
    UPLOAD MODEL model_name
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

        UPLOAD MODEL model_name
            FROM 'llama3/' OVERWRITE;

    See Also
    --------
    * ``DOWNLOAD MODEL model_name``

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


UploadModelHandler.register(overwrite=True)


class DownloadModelHandler(SQLHandler):
    """
    DOWNLOAD MODEL model_name
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

        DOWNLOAD MODEL llama3;

    The following command downloads a model to a specific location and
    overwrites any existing models folder with the name ``local_llama3`` on the local storage::

        DOWNLOAD MODEL llama3
            TO 'local_llama3' OVERWRITE;

    See Also
    --------
    * ``UPLOAD MODEL model_name FROM local_path``

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


DownloadModelHandler.register(overwrite=True)


class DropModelsHandler(SQLHandler):
    """
    DROP MODEL model_name;

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

        DROP MODEL llama3;

    """  # noqa: E501

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        params['file_location'] = 'MODELS'
        path = os.path.join(params['model_name'], '')

        file_space = get_file_space(params)
        file_space.removedirs(path=path)

        return None


DropModelsHandler.register(overwrite=True)
