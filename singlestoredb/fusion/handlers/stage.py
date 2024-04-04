#!/usr/bin/env python3
from typing import Any
from typing import Dict
from typing import Optional

from .. import result
from ..handler import SQLHandler
from ..result import FusionSQLResult
from .utils import dt_isoformat
from .utils import get_workspace_group


class ShowStageFilesHandler(SQLHandler):
    """
    SHOW STAGE FILES [ in_group ] [ at_path ] [ <like> ] [ <order-by> ]
                     [ <limit> ] [ recursive ] [ extended ];

    # Workspace group
    in_group = IN GROUP { group_id | group_name }

    # ID of group
    group_id = ID '<group-id>'

    # Name of group
    group_name = '<group-name>'

    # Stage path to list
    at_path = AT '<path>'

    # Should the listing be recursive?
    recursive = RECURSIVE

    # Should extended attributes be shown?
    extended = EXTENDED

    Description
    -----------
    Show the files in a workspace group's stage.

    Remarks
    -------
    * ``IN GROUP`` specifies the workspace group or workspace group ID.
      When using an ID, ``IN GROUP ID`` must be used.
    * ``AT PATH`` specifies the path to list. If no ``AT PATH`` is specified,
      the root directory is used.
    * ``LIKE`` allows you to specify a filename pattern using ``%`` as a wildcard.
    * ``ORDER BY`` allows you to specify the field to sort by.
    * ``LIMIT`` allows you to set a limit on the number of entries displayed.
    * ``RECURSIVE`` indicates that the stage should be listed recursively.
    * ``EXTENDED`` indicates that more detailed information should be displayed.

    Examples
    --------
    Example 1: Show files at path

    This example shows how to list files starting at a specific path::

        SHOW STAGE FILES IN GROUP "My Group" AT PATH "/data/";

    Example 2: Show files recursively

    This example show dow to display files recursively and with extra information::

        SHOW STAGE FILES IN GROUP "My Group" RECURSIVE EXTENDED;

    See Also
    --------
    * UPLOAD FILE TO STAGE
    * DOWNLOAD STAGE FILE

    """

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        wg = get_workspace_group(params)

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
    UPLOAD FILE TO STAGE stage_path [ in_group ] FROM local_path [ overwrite ];

    # Path to stage file
    stage_path = '<stage-path>'

    # Workspace group
    in_group = IN GROUP { group_id | group_name }

    # ID of group
    group_id = ID '<group-id>'

    # Name of group
    group_name = '<group-name>'

    # Path to local file
    local_path = '<local-path>'

    # Should an existing file be overwritten?
    overwrite = OVERWRITE

    Description
    -----------
    Upload a file to the workspace group's stage.

    Remarks
    -------
    * ``<stage-path>`` is the path in stage to upload the file to.
    * ``IN GROUP`` specifies the workspace group or workspace group ID. When
      using an ID ``IN GROUP ID`` should be used.
    * ``<local-path>`` is the path on the local machine of the file to upload.
    * ``OVERWRITE`` indicates that an existing stage file at that path
      should be overwritten if it exists.

    Examples
    --------
    Example 1: Upload with overwrite::

        UPLOAD FILE TO STAGE '/data/stats.csv' IN GROUP 'My Group'
               FROM '/u/user/stats.csv' OVERWRITE;

    See Also
    --------
    * DOWNLOAD STAGE FILE

    """

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        wg = get_workspace_group(params)
        wg.stage.upload_file(
            params['local_path'], params['stage_path'],
            overwrite=params['overwrite'],
        )
        return None


UploadStageFileHandler.register(overwrite=True)


class DownloadStageFileHandler(SQLHandler):
    """
    DOWNLOAD STAGE FILE stage_path [ in_group ] [ local_path ]
                                   [ overwrite ] [ encoding ];

    # Path to stage file
    stage_path = '<stage-path>'

    # Workspace group
    in_group = IN GROUP { group_id | group_name }

    # ID of group
    group_id = ID '<group-id>'

    # Name of group
    group_name = '<group-name>'

    # Path to local file
    local_path = TO '<local-path>'

    # Should an existing file be overwritten?
    overwrite = OVERWRITE

    # File encoding
    encoding = ENCODING '<encoding>'

    Description
    -----------
    Download a stage file.

    Remarks
    -------
    * ``<stage-path>`` is the path in stage to download.
    * ``IN GROUP`` specifies the workspace group or workspace group ID. When
      using an ID ``IN GROUP ID`` should be used.
    * ``<local-path>`` is the destination path for the file. If not specified,
      the file is returned in a result set.
    * ``OVERWRITE`` indicates that an existing local file should be overwritten.
    * ``ENCODING`` specifies the encoding of the file to apply to the downloaded
      file. By default, files are downloaded as binary. This only option
      typically only matters if ``<local-path>`` is not specified and the file
      is to be printed to the screen.

    Examples
    --------
    Example 1: Print a file to the screen::

        DOWNLOAD STAGE FILE '/data/stats.csv' IN GROUP 'My Group';

    Example 2: Download a file to a local path with overwrite set::

        DONLOAD STAGE FILE '/data/stats.csv' IN GROUP 'My Group'
                TO '/u/me/data.csv' OVERWRITE;

    See Also
    --------
    * UPLOAD FILE TO STAGE

    """

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        wg = get_workspace_group(params)

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
    DROP STAGE FILE stage_path [ in_group ];

    # Path to stage file
    stage_path = '<stage-path>'

    # Workspace group
    in_group = IN GROUP { group_id | group_name }

    # ID of group
    group_id = ID '<group-id>'

    # Name of group
    group_name = '<group-name>'

    Description
    -----------
    Drop a stage file.

    Remarks
    -------
    * ``<stage-path>`` is the path in stage to drop.
    * ``IN GROUP`` specifies the workspace group or workspace group ID. When
      using an ID ``IN GROUP ID`` should be used.

    Example
    --------
    Drop a specific file from stage::

        DROP STAGE FILE '/data/stats.csv' IN GROUP 'My Group';

    See Also
    --------
    * DROP STAGE FOLDER

    """

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        wg = get_workspace_group(params)
        wg.stage.remove(params['stage_path'])
        return None


DropStageFileHandler.register(overwrite=True)


class DropStageFolderHandler(SQLHandler):
    """
    DROP STAGE FOLDER stage_path [ in_group ] [ recursive ];

    # Path to stage folder
    stage_path = '<stage-path>'

    # Workspace group
    in_group = IN GROUP { group_id | group_name }

    # ID of group
    group_id = ID '<group-id>'

    # Name of group
    group_name = '<group-name>'

    # Should folers be deleted recursively?
    recursive = RECURSIVE

    Description
    -----------
    Drop a folder from stage.

    Remarks
    -------
    * ``<stage-path>`` is the path in stage to drop.
    * ``IN GROUP`` specifies the workspace group or workspace group ID. When
      using an ID ``IN GROUP ID`` should be used.
    * ``RECURSIVE`` indicates that folders should be removed recursively.

    Example
    -------
    Drop a folder recursively::

        DROP STAGE FOLDER '/data/' IN GROUP 'My Group' RECURSIVE;

    See Also
    --------
    * DROP STAGE FILE

    """

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        wg = get_workspace_group(params)
        if params['recursive']:
            wg.stage.removedirs(params['stage_path'])
        else:
            wg.stage.rmdir(params['stage_path'])
        return None


DropStageFolderHandler.register(overwrite=True)


class CreateStageFolderHandler(SQLHandler):
    """
    CREATE STAGE FOLDER stage_path [ in_group ] [ overwrite ];

    # Workspace group
    in_group = IN GROUP { group_id | group_name }

    # ID of group
    group_id = ID '<group-id>'

    # Name of group
    group_name = '<group-name>'

    # Path to stage folder
    stage_path = '<stage-path>'

    # Should an existing folder be overwritten?
    overwrite = OVERWRITE

    Description
    -----------
    Create a folder in stage.

    Remarks
    -------
    * ``<stage-path>`` is the path to create in stage.
    * ``IN GROUP`` specifies the workspace group or workspace group ID. When
      using an ID ``IN GROUP ID`` should be used.
    * ``OVERWRITE`` indicates that an existing folder should be overwritten
      with a new folder.

    Example
    -------
    Create a folder::

        CREATE STAGE FOLDER `/data/csv/` IN GROUP 'My Group';

    """

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        wg = get_workspace_group(params)
        wg.stage.mkdir(params['stage_path'], overwrite=params['overwrite'])
        return None


CreateStageFolderHandler.register(overwrite=True)
