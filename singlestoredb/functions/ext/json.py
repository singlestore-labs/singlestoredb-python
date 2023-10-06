#!/usr/bin/env python3
import json
from typing import Any
from typing import Iterable
from typing import List
from typing import Tuple


def load(colspec: Iterable[Tuple[str, int]], data: bytes) -> Tuple[List[int], List[Any]]:
    '''
    Convert bytes in JSON format into rows of data.

    Parameters
    ----------
    colspec : Iterable[str]
        An Iterable of column data types
    data : bytes
        The data in JSON format

    Returns
    -------
    list[list[Any]]

    '''
    row_ids = []
    rows = []
    for row_id, *row in json.loads(data.decode('utf-8'))['data']:
        row_ids.append(row_id)
        rows.append(row)
    return row_ids, rows


def dump(returns: Iterable[int], data: Iterable[Tuple[int, Any]]) -> bytes:
    '''
    Convert a list of lists of data into JSON format.

    Parameters
    ----------
    returns : str
        The returned data type
    data : list[list[Any]]
        The rows of data to serialize

    Returns
    -------
    bytes

    '''
    return json.dumps(dict(data=list(data))).encode('utf-8')
