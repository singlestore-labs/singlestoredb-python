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

    """

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        wg = get_workspace_group(params)
        wg.stage.mkdir(params['stage_path'], overwrite=params['overwrite'])
        return None


CreateStageFolderHandler.register(overwrite=True)
