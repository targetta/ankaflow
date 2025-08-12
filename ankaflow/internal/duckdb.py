from abc import ABC, abstractmethod
import typing as t
import duckdb
from pandas import DataFrame
from pyarrow import Table
from io import StringIO, FileIO
import sys
from duckdb import DuckDBPyRelation  # noqa:E501

from ..models.configs import ConnectionConfiguration

IS_PYODIDE = sys.platform == "emscripten"


class Relation(ABC):
    """
    Relation is an async shim
    similar to DuckDBPyRelation object
    """

    @abstractmethod
    def raw(self) -> DuckDBPyRelation:
        """
        Exposes underlying native DuckDBPyRelation with full api.

        Returns:
            DuckDBPyRelation
        """
        pass

    @abstractmethod
    async def fetchone(self) -> t.Tuple[t.Any, ...] | None:
        pass

    @abstractmethod
    async def fetchall(self) -> t.List[t.Any]:
        pass

    @abstractmethod
    async def df(self) -> DataFrame:
        pass

    @abstractmethod
    async def arrow(self) -> Table:
        pass


# TODO: Replace with Protocol
class DDB(ABC):
    @staticmethod
    async def connect(
        connection_options: ConnectionConfiguration,
        persisted: t.Optional[str] = None,
    ) -> "DDB":
        if IS_PYODIDE:
            from .browser import DDB as BrowserDDB

            browser: "BrowserDDB" = BrowserDDB(connection_options, persisted=persisted)
            # Type "DDB" is not assignable to return type "DDB"
            return t.cast(DDB, await browser.connect())

        from .server import DDB as ServerDDB

        server: "ServerDDB" = ServerDDB(connection_options, persisted=persisted)
        return t.cast(DDB, await server.connect())

    @abstractmethod
    def connection(self) -> duckdb.DuckDBPyConnection:
        pass

    @abstractmethod
    async def disconnect(self):
        pass

    @abstractmethod
    async def get(self):
        pass

    @abstractmethod
    async def inject_secrets(
        self, name: str, connection_options: ConnectionConfiguration
    ):
        pass

    @abstractmethod
    async def sql(self, query: str) -> Relation:
        pass

    @abstractmethod
    async def register(self, view_name: str, object: t.Any):
        pass

    @abstractmethod
    async def unregister(self, view_name: str):
        pass

    @abstractmethod
    async def read_json(
        self, data: t.Union[FileIO, StringIO], table: str, read_opts: t.Dict
    ):
        pass

    @abstractmethod
    async def read_parquet(
        self, data: t.Union[FileIO, StringIO], table: str, read_opts: t.Dict
    ):
        pass

    @abstractmethod
    async def read_csv(
        self, data: t.Union[FileIO, StringIO], table: str, read_opts: t.Dict
    ):
        pass

    @abstractmethod
    async def delta_scan(self):
        pass

    @abstractmethod
    async def parquet_scan(self):
        pass
