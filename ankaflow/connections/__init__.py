import sys
import typing as t
import logging

from .connection import Connection

# Builtin connection imports
from .rest.rest import Rest as Rest
from .llm.sqlgen import SQLGen as SQLGen
from .file import (
    Parquet as Parquet,
    CSV as CSV,
    JSON as JSON,
    File as File,
    Variable as Variable,
)

from .. import models as m

log = logging.getLogger(__name__)


IS_PYODIDE = sys.platform == "emscripten"

# server-only connections
if t.TYPE_CHECKING or not IS_PYODIDE:
    from .bigquery import BigQuery as BigQuery
    from .clickhouse import Clickhouse as Clickhouse
    from .delta import Deltatable as Deltatable
else:
    pass


current_module = sys.modules[__name__]


class NoConnectionError(Exception):
    pass


def load_connection(
    cls: t.Type[
        t.Union[m.Connection, m.RestConnection, m.CustomConnection, m.Dimension]
    ],
) -> t.Type[Connection]:
    """
    Load built-in connection or custom connection from module

    Args:
        name (str): Connection name
        module (str, optional): If set then try to load from
            given module. Defaults to None i.e. built-in connection.
    """
    mth = getattr(cls, "load", None)

    try:
        if callable(mth):
            loaded = cls.load()  # type: ignore
            if not issubclass(loaded, Connection):
                raise TypeError(
                    f"{cls.__name__} is not a subclass of Connection"
                )
            return loaded

        return getattr(current_module, cls.kind)

    except (ImportError, AttributeError) as e:
        raise NoConnectionError(
            f"Connection '{cls.kind}' unavailable: {e}"
        ) from e
