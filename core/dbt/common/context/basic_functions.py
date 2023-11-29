import json
from typing import Any, NoReturn

from dbt.common.exceptions import MacroReturn


def _return(data: Any) -> NoReturn:
    raise MacroReturn(data)


def tojson(value: Any, default: Any = None, sort_keys: bool = False) -> Any:
    """
    The `tojson` method can be used to serialize a Python
        object primitive, eg. a `dict` or `list` to a json string.

    :param value: The value serialize to json
    :param default: A default value to return if the `value` argument cannot be serialized
    :param sort_keys: If True, sort the keys.
    """
    try:
        return json.dumps(value, sort_keys=sort_keys)
    except ValueError:
        return default
