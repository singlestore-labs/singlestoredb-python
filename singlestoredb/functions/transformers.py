#!/usr/bin/env python3
import dataclasses
import functools
import json
from typing import Any
from typing import Callable
from typing import Dict
from typing import Tuple
from typing import Type
from typing import TypeVar

from . import utils

T = TypeVar('T')


def json_to_dict(cls: Type[T], json_value: str) -> Dict[str, Any]:
    """
    Convert a JSON string to a dictionary.

    Parameters
    ----------
    json_value : str
        The JSON string representing the object.

    Returns
    -------
    dict
        A dictionary with fields populated from the JSON.

    """
    return cls(json.loads(json_value))  # type: ignore


def json_to_pydantic(cls: Type[T], json_value: str) -> T:
    """
    Convert a JSON string to a Pydantic model instance.

    Parameters
    ----------
    cls : Type[T]
        The Pydantic model type to instantiate.
    json_value : str
        The JSON string representing the object.

    Returns
    -------
    T
        An instance of the Pydantic model with fields populated from the JSON.

    """
    return cls(**json.loads(json_value))


def json_to_namedtuple(cls: Type[T], json_value: str) -> T:
    """
    Convert a JSON string to a namedtuple instance.

    Parameters
    ----------
    cls : Type[T]
        The namedtuple type to instantiate.
    json_value : str
        The JSON string representing the object.

    Returns
    -------
    T
        An instance of the namedtuple with fields populated from the JSON.

    """
    data = json.loads(json_value)
    field_types = getattr(cls, '_field_types', getattr(cls, '__annotations__', {}))
    typed_data = {}
    for key, value in data.items():
        if key in field_types:
            typ = field_types[key]
            try:
                typed_data[key] = typ(value)
            except Exception:
                typed_data[key] = value  # fallback if conversion fails
        else:
            typed_data[key] = value
    return cls(**typed_data)


def json_to_typeddict(cls: Type[T], json_value: str) -> Dict[str, Any]:
    """
    Convert a JSON string to a TypedDict instance.

    Parameters
    ----------
    cls : Type[T]
        The TypedDict type to instantiate.
    json_value : str
        The JSON string representing the object.

    Returns
    -------
    T
        An instance of the TypedDict with fields populated from the JSON.

    """
    data = json.loads(json_value)
    field_types = getattr(cls, '__annotations__', {})
    typed_data = {}
    for key, value in data.items():
        if key in field_types:
            typ = field_types[key]
            try:
                typed_data[key] = typ(value)
            except Exception:
                typed_data[key] = value  # fallback if conversion fails
        else:
            typed_data[key] = value
    return typed_data  # TypedDicts are just dicts at runtime


def json_to_dataclass(cls: Type[T], json_value: str) -> T:
    """
    Convert a JSON string to a dataclass instance.

    Parameters
    ----------
    cls : Type[T]
        The dataclass type to instantiate.
    json_str : str
        The JSON string representing the object.

    Returns
    -------
    T
        An instance of the dataclass with fields populated from the JSON.

    """
    data = json.loads(json_value)
    field_types = {f.name: f.type for f in dataclasses.fields(cls)}  # type: ignore
    typed_data = {}
    for key, value in data.items():
        if key in field_types:
            typ = field_types[key]
            try:
                if callable(typ):
                    typed_data[key] = typ(value)
                else:
                    typed_data[key] = value
            except Exception:
                typed_data[key] = value  # fallback if conversion fails
        else:
            typed_data[key] = value
    return cls(**typed_data)


def json_to_pandas_dataframe(cls: Type[T], json_value: str) -> T:
    """
    Convert a JSON string to a DataFrame instance.

    Parameters
    ----------
    cls : Type[T]
        The DataFrame type to instantiate.
    json_value : str
        The JSON string representing the object.

    Returns
    -------
    T
        An instance of the DataFrame with fields populated from the JSON.

    """
    return cls(json.loads(json_value))  # type: ignore


def dict_to_json(cls: Type[T], obj: Dict[str, Any]) -> str:
    """
    Convert a dictionary to a JSON string.
    """
    return json.dumps(obj)


def pydantic_to_json(cls: Type[T], obj: Any) -> str:
    """
    Convert a Pydantic model instance to a JSON string.
    """
    return obj.model_dump_json()


def namedtuple_to_json(cls: Type[T], obj: Any) -> str:
    """
    Convert a namedtuple instance to a JSON string.
    """
    return json.dumps(obj._asdict())


def typeddict_to_json(cls: Type[T], obj: Dict[str, Any]) -> str:
    """
    Convert a TypedDict instance (just a dict at runtime) to a JSON string.
    """
    return json.dumps(obj)


def dataclass_to_json(cls: Type[T], obj: Any) -> str:
    """
    Convert a dataclass instance to a JSON string.
    """
    return json.dumps(dataclasses.asdict(obj))


def pandas_dataframe_to_json(cls: Type[T], obj: Any) -> str:
    """
    Convert a pandas DataFrame to a JSON string (records orientation).
    """
    return obj.to_json(orient='records')


def create_json_transformers(
    cls: Type[T],
) -> Tuple[Callable[[T], str], Callable[[str], T]]:
    """
    Create transformers for arbitrary objects to JSON strings.

    Parameters
    ----------
    cls : Type[T]
        The class type to instantiate for the JSON conversion.

    Returns
    -------
    Tuple[Callable[[T], str], Callable[[str], T]]
        A tuple containing two functions:
        - The first function converts an instance of `cls` to a JSON string.
        - The second function converts a JSON string back to an instance of `cls`.

    """
    if issubclass(cls, dict):
        return (  # type: ignore
            functools.partial(json_to_dict, cls),
            functools.partial(dict_to_json, cls),
        )
    elif utils.is_pydantic(cls):
        return (  # type: ignore
            functools.partial(json_to_pydantic, cls),
            functools.partial(pydantic_to_json, cls),
        )
    elif utils.is_namedtuple(cls):
        return (  # type: ignore
            functools.partial(json_to_namedtuple, cls),
            functools.partial(namedtuple_to_json, cls),
        )
    elif utils.is_typeddict(cls):
        return (  # type: ignore
            functools.partial(json_to_typeddict, cls),
            functools.partial(typeddict_to_json, cls),
        )
    elif utils.is_dataclass(cls):
        return (  # type: ignore
            functools.partial(json_to_dataclass, cls),
            functools.partial(dataclass_to_json, cls),
        )
    elif utils.is_dataframe(cls):
        return (  # type: ignore
            functools.partial(json_to_pandas_dataframe, cls),
            functools.partial(pandas_dataframe_to_json, cls),
        )
    raise TypeError(f'Unsupported type for JSON conversion: {type(cls).__name__}')
