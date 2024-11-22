#!/usr/bin/env python3
from typing import Any
from typing import Dict
from typing import Optional

from .. import result
from ..handler import SQLHandler
from ..result import FusionSQLResult
from .utils import dt_isoformat
from .utils import get_deployment


class ShowStageFilesHandler(SQLHandler):
    """
    SHOW STAGE FILES [ in ]
        [ at_path ] [ <like> ]
        [ <order-by> ]
        [ <limit> ] [ recursive ] [ extended ];

    # Deployment
    in = { in_group | in_deployment }
    in_group = IN GROUP { deployment_id | deployment_name }
    in_deployment = IN { deployment_id | deployment_name }

    # ID of deployment
    deployment_id = ID '<deployment-id>'

    # Name of deployment
    deployment_name = '<deployment-name>'

    # Stage path to list
    at_path = AT '<path>'

    # Should the listing be recursive?
    recursive = RECURSIVE

    # Should extended attributes be shown?
    extended = EXTENDED

    Description
    -----------
    Displays a list of files in a Stage.

    Refer to `Stage <https://docs.singlestore.com/cloud/developer-resources/stage-storage-service/>`_
    for more information.

    Arguments
    ---------
    * ``<deployment-id>``: The ID of the deployment in which
      the Stage is attached.
    * ``<deployment-name>``: The name of the deployment in which
      which the Stage is attached.
    * ``<path>``: A path in the Stage.
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
    * The ``AT PATH`` clause specifies the path in the Stage to list
      the files from.
    * The ``IN`` clause specifies the ID or the name of the
      deployment in which the Stage is attached.
    * Use the ``RECURSIVE`` clause to list the files recursively.
    * To return more information about the files, use the ``EXTENDED``
      clause.

    Examples
    --------
    The following command lists the files at a specific path::

        SHOW STAGE FILES IN 'wsg1' AT PATH "/data/";

    The following command lists the files recursively with
    additional information::

        SHOW STAGE FILES IN 'wsg1' RECURSIVE EXTENDED;

    See Also
    --------
    * ``UPLOAD FILE TO STAGE``
    * ``DOWNLOAD STAGE FILE``

    """  # noqa: E501

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        wg = get_deployment(params)

        res = FusionSQLResult()
        res.add_field('Name', result.STRING)

        if params['extended']:
            res.add_field('Type', result.STRING)
            res.add_field('Size', result.INTEGER)
            res.add_field('Writable', result.STRING)
            res.add_field('CreatedAt', result.DATETIME)
            res.add_field('LastModifiedAt', result.DATETIME)

            files = []
            for x in wg.stage.listdir(
                params['at_path'] or '/',
                recursive=params['recursive'],
            ):
                info = wg.stage.info(x)
                files.append(
                    tuple([
                        x, info.type, info.size or 0, info.writable,
                        dt_isoformat(info.created_at),
                        dt_isoformat(info.last_modified_at),
                    ]),
                )
            res.set_rows(files)

        else:
            res.set_rows([(x,) for x in wg.stage.listdir(
                params['at_path'] or '/',
                recursive=params['recursive'],
            )])

        if params['like']:
            res = res.like(Name=params['like'])

        return res.order_by(**params['order_by']).limit(params['limit'])


ShowStageFilesHandler.register(overwrite=True)


class UploadStageFileHandler(SQLHandler):
    """
    UPLOAD FILE TO STAGE stage_path
        [ in ]
        FROM local_path [ overwrite ];

    # Path to stage file
    stage_path = '<stage-path>'

    # Deployment
    in = { in_group | in_deployment }
    in_group = IN GROUP { deployment_id | deployment_name }
    in_deployment = IN { deployment_id | deployment_name }

    # ID of deployment
    deployment_id = ID '<deployment-id>'

    # Name of deployment
    deployment_name = '<deployment-name>'

    # Path to local file
    local_path = '<local-path>'

    # Should an existing file be overwritten?
    overwrite = OVERWRITE

    Description
    -----------
    Uploads a file to a Stage.

    Refer to `Stage <https://docs.singlestore.com/cloud/developer-resources/stage-storage-service/>`_
    for more information.

    Arguments
    ---------
    * ``<stage-path>``: The path in the Stage where the file is uploaded.
    * ``<deployment-id>``: The ID of the deployment in which the Stage
      is attached.
    * ``<deployment-name>``: The name of the deployment in which
      which the Stage is attached.
    * ``<local-path>``: The path to the file to upload in the local
      directory.

    Remarks
    -------
    * The ``IN`` clause specifies the ID or the name of the workspace
      group in which the Stage is attached.
    * If the ``OVERWRITE`` clause is specified, any existing file at the
      specified path in the Stage is overwritten.

    Examples
    --------
    The following command uploads a file to a Stage and overwrites any
    existing files at the specified path::

        UPLOAD FILE TO STAGE '/data/stats.csv' IN 'wsg1'
            FROM '/tmp/user/stats.csv' OVERWRITE;

    See Also
    --------
    * ``DOWNLOAD STAGE FILE``

    """  # noqa: E501

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        wg = get_deployment(params)
        wg.stage.upload_file(
            params['local_path'], params['stage_path'],
            overwrite=params['overwrite'],
        )
        return None


UploadStageFileHandler.register(overwrite=True)


class DownloadStageFileHandler(SQLHandler):
    """
    DOWNLOAD STAGE FILE stage_path
        [ in ]
        [ local_path ]
        [ overwrite ]
        [ encoding ];

    # Path to stage file
    stage_path = '<stage-path>'

    # Deployment
    in = { in_group | in_deployment }
    in_group = IN GROUP { deployment_id | deployment_name }
    in_deployment = IN { deployment_id | deployment_name }

    # ID of deployment
    deployment_id = ID '<deployment-id>'

    # Name of deployment
    deployment_name = '<deployment-name>'

    # Path to local file
    local_path = TO '<local-path>'

    # Should an existing file be overwritten?
    overwrite = OVERWRITE

    # File encoding
    encoding = ENCODING '<encoding>'

    Description
    -----------
    Download a file from a Stage.

    Refer to `Stage <https://docs.singlestore.com/cloud/developer-resources/stage-storage-service/>`_
    for more information.

    Arguments
    ---------
    * ``<stage-path>``: The path to the file to download in a Stage.
    * ``<deployment-id>``: The ID of the deployment in which the
      Stage is attached.
    * ``<deployment-name>``: The name of the deployment in which
      which the Stage is attached.
    * ``<encoding>``: The encoding to apply to the downloaded file.
    * ``<local-path>``: Specifies the path in the local directory
      where the file is downloaded.

    Remarks
    -------
    * If the ``OVERWRITE`` clause is specified, any existing file at
      the download location is overwritten.
    * The ``IN`` clause specifies the ID or the name of the
      deployment in which the Stage is attached.
    * By default, files are downloaded in binary encoding. To view
      the contents of the file on the standard output, use the
      ``ENCODING`` clause and specify an encoding.
    * If ``<local-path>`` is not specified, the file is displayed
      on the standard output.

    Examples
    --------
    The following command displays the contents of the file on the
    standard output::

        DOWNLOAD STAGE FILE '/data/stats.csv' IN 'wsgroup1' ENCODING 'utf8';

    The following command downloads a file to a specific location and
    overwrites any existing file with the name ``stats.csv`` on the local storage::

        DOWNLOAD STAGE FILE '/data/stats.csv' IN 'wsgroup1'
            TO '/tmp/data.csv' OVERWRITE;

    See Also
    --------
    * ``UPLOAD FILE TO STAGE``

    """  # noqa: E501

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        wg = get_deployment(params)

        out = wg.stage.download_file(
            params['stage_path'],
            local_path=params['local_path'] or None,
            overwrite=params['overwrite'],
            encoding=params['encoding'] or None,
        )

        if not params['local_path']:
            res = FusionSQLResult()
            if params['encoding']:
                res.add_field('Data', result.STRING)
            else:
                res.add_field('Data', result.BLOB)
            res.set_rows([(out,)])
            return res

        return None


DownloadStageFileHandler.register(overwrite=True)


class DropStageFileHandler(SQLHandler):
    """
    DROP STAGE FILE stage_path
        [ in ];

    # Path to stage file
    stage_path = '<stage-path>'

    # Deployment
    in = { in_group | in_deployment }
    in_group = IN GROUP { deployment_id | deployment_name }
    in_deployment = IN { deployment_id | deployment_name }

    # ID of deployment
    deployment_id = ID '<deployment-id>'

    # Name of deployment
    deployment_name = '<deployment-name>'

    Description
    -----------
    Deletes a file from a Stage.

    Refer to `Stage <https://docs.singlestore.com/cloud/developer-resources/stage-storage-service/>`_
    for more information.

    Arguments
    ---------
    * ``<stage-path>``: The path to the file to delete in a Stage.
    * ``<deployment-id>``: The ID of the deployment in which the
      Stage is attached.
    * ``<deployment-name>``: The name of the deployment in which
      which the Stage is attached.

    Remarks
    -------
    * The ``IN`` clause specifies the ID or the name of the
      deployment in which the Stage is attached.

    Example
    --------
    The following command deletes a file from a Stage attached to
    a deployment named **wsg1**::

        DROP STAGE FILE '/data/stats.csv' IN 'wsg1';

    See Also
    --------
    * ``DROP STAGE FOLDER``

    """  # noqa: E501

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        wg = get_deployment(params)
        wg.stage.remove(params['stage_path'])
        return None


DropStageFileHandler.register(overwrite=True)


class DropStageFolderHandler(SQLHandler):
    """
    DROP STAGE FOLDER stage_path
        [ in ]
        [ recursive ];

    # Path to stage folder
    stage_path = '<stage-path>'

    # Deployment
    in = { in_group | in_deployment }
    in_group = IN GROUP { deployment_id | deployment_name }
    in_deployment = IN { deployment_id | deployment_name }

    # ID of deployment
    deployment_id = ID '<deployment-id>'

    # Name of deployment
    deployment_name = '<deployment-name>'

    # Should folers be deleted recursively?
    recursive = RECURSIVE

    Description
    -----------
    Deletes a folder from a Stage.

    Refer to `Stage <https://docs.singlestore.com/cloud/developer-resources/stage-storage-service/>`_
    for more information.

    Arguments
    ---------
    * ``<stage-path>``: The path to the folder to delete in a Stage.
    * ``<deployment-id>``: The ID of the deployment in which the
      Stage is attached.
    * ``<deployment-name>``: The name of the deployment in which
      which the Stage is attached.

    Remarks
    -------
    * The ``RECURSIVE`` clause indicates that the specified folder
      is deleted recursively.

    Example
    -------
    The following command recursively deletes a folder from a Stage
    attached to a deployment named **wsg1**::

        DROP STAGE FOLDER '/data/' IN 'wsg1' RECURSIVE;

    See Also
    --------
    * ``DROP STAGE FILE``

    """  # noqa: E501

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        wg = get_deployment(params)
        if params['recursive']:
            wg.stage.removedirs(params['stage_path'])
        else:
            wg.stage.rmdir(params['stage_path'])
        return None


DropStageFolderHandler.register(overwrite=True)


class CreateStageFolderHandler(SQLHandler):
    """
    CREATE STAGE FOLDER stage_path
        [ in ]
        [ overwrite ];

    # Deployment
    in = { in_group | in_deployment }
    in_group = IN GROUP { deployment_id | deployment_name }
    in_deployment = IN { deployment_id | deployment_name }

    # ID of deployment
    deployment_id = ID '<deployment-id>'

    # Name of deployment
    deployment_name = '<deployment-name>'

    # Path to stage folder
    stage_path = '<stage-path>'

    # Should an existing folder be overwritten?
    overwrite = OVERWRITE

    Description
    -----------
    Creates a new folder at the specified path in a Stage.

    Arguments
    ---------
    * ``<stage-path>``: The path in a Stage where the folder
      is created. The path must end with a trailing slash (/).
    * ``<deployment-id>``: The ID of the deployment in which
      the Stage is attached.
    * ``<deployment-name>``: The name of the deployment in which
      which the Stage is attached.

    Remarks
    -------
    * If the ``OVERWRITE`` clause is specified, any existing
      folder at the specified path is overwritten.
    * The ``IN`` clause specifies the ID or the name of
      the deployment in which the Stage is attached.

    Example
    -------
    The following command creates a folder in a Stage attached
    to a deployment named **wsg1**::

        CREATE STAGE FOLDER `/data/csv/` IN 'wsg1';

    """

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        wg = get_deployment(params)
        wg.stage.mkdir(params['stage_path'], overwrite=params['overwrite'])
        return None


CreateStageFolderHandler.register(overwrite=True)
