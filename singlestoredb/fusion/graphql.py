import itertools
import os
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

import requests

from . import result
from .result import FusionSQLResult


API_URL = 'https://backend.singlestore.com/public'


def pass_through(x: Any) -> Any:
    """Pass a value through."""
    return x


def find_path(d: Dict[str, Any], path: str) -> Tuple[bool, Any]:
    """
    Find key path in a dictionary.

    Parameters
    ----------
    d : Dict[str, Any]
        Dictionary to search
    path : str
        Period-delimited string indicating nested keys

    Returns
    -------
    (bool, Any) - bool indicating whether or not the path was found
        and the result itself

    """
    curr = d
    keys = path.split('.')
    for i, k in enumerate(keys):
        if k in curr:
            curr = curr[k]
            if not isinstance(curr, dict):
                break
        else:
            return False, None
    if (i + 1) == len(keys):
        return True, curr
    return False, None


class GraphQueryField(object):
    """
    Field in a GraphQuery result.

    Parameters
    ----------
    path : str
        Period-delimited path to the result
    dtype : int, optional
        MySQL data type of the result, defaults to string
    converter : function, optional
        Convert for data value

    """

    _sort_index_count = itertools.count()

    def __init__(
        self,
        path: str,
        dtype: int = result.STRING,
        include: Union[str, List[str]] = '',
        converter: Optional[Callable[[Any], Any]] = pass_through,
    ) -> None:
        self.path = path
        self.dtype = dtype
        self.include = [include] if isinstance(include, str) else include
        self.include = [x for x in self.include if x]
        self.converter = converter
        self._sort_index = next(type(self)._sort_index_count)

    def get_path(self, value: Any) -> Tuple[bool, Any]:
        """
        Retrieve the field path in the given object.

        Parameters
        ----------
        value : Any
            Object parsed from nested dictionary object

        Returns
        -------
        (bool, Any) - bool indicating whether the path was found and
            the result itself

        """
        found, out = find_path(value, self.path)
        if self.converter is not None:
            return found, self.converter(out)
        return found, out


class GraphQuery(object):
    """
    Base class for all GraphQL classes.

    Parameters
    ----------
    api_token : str, optional
        API token to access the GraphQL endpoint
    api_url : str, optional
        GraphQL endpoint

    """

    def __init__(
        self,
        api_token: str = '',
        api_url: str = API_URL,
    ) -> None:
        self.api_token = api_token
        self.api_url = api_url

    @classmethod
    def get_query(cls) -> str:
        """Return the GraphQL for the class."""
        return cls.__doc__ or ''

    @classmethod
    def get_fields(cls) -> List[Tuple[str, GraphQueryField]]:
        """
        Return fields for the query.

        Parameters
        ----------
        groups : str
            List of group characters to include

        Returns
        -------
        List[Tuple[str, QueryField]] - tuple pairs of field name and definition

        """
        attrs = [(k, v) for k, v in vars(cls).items() if isinstance(v, GraphQueryField)]
        attrs = list(sorted(attrs, key=lambda x: x[1]._sort_index))
        return attrs

    def run(
        self,
        variables: Optional[Dict[str, Any]] = None,
        *,
        filter_expr: str = '',
    ) -> FusionSQLResult:
        """
        Run the query.

        Parameters
        ----------
        variables : Dict[str, Any], optional
            Dictionary of substitution parameters

        Returns
        -------
        FusionSQLResult

        """
        api_token = self.api_token or os.environ.get('SINGLESTOREDB_BACKEND_TOKEN')
        res = requests.post(
            self.api_url,
            headers={
                'Content-Type': 'application/json',
                'Authorization': f'Bearer {api_token}',
            },
            json={
                'query': type(self).get_query(),
                'variables': variables or {},
            },
        )

        if res.status_code != 200:
            raise ValueError(f'an error occurred: {res.text}')

        json = res.json()

        if json['data']:
            data = json['data'].popitem()[-1]
            if isinstance(data, Dict):
                data = [data]
        else:
            data = []

        fres = FusionSQLResult()

        rows = []
        fields = type(self).get_fields()
        for i, obj in enumerate(data):
            row = []
            for name, field in fields:
                found, value = field.get_path(obj)
                if found:
                    if i == 0:
                        fres.add_field(name, field.dtype)
                    row.append(value)
            rows.append(tuple(row))

        fres.set_rows(rows)

        return fres
