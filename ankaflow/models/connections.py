import typing as t
import importlib
from typing_extensions import deprecated
from pydantic import BaseModel, Field as PydanticField

from .configs import ConnectionConfiguration
from .components import Columns
from .rest import Request, RestClientConfig


class VersionedConnection(BaseModel):
    version: t.Optional[str] = None
    """
    Field for record version timestamp
    """
    key: t.Optional[t.List[str]] = None
    """
    List of versioned fields
    """


class Connection(BaseModel):
    kind: str  # Deltatable, Bigquery, Clickhouse, Parquet, File, Rest
    """
    Model type e.g. Deltatable, Bigquery, Clickhouse, Parquet, File
    Custom connections can be loaded from module.
    """
    config: t.Optional[ConnectionConfiguration] = None
    """
    Optional configuration for the current connection.
    If not present then global configuration will be used.
    """
    params: t.Optional[dict] = PydanticField(default_factory=dict)
    """
    Any parameters that can be passed to connection.
    """
    fields: t.Optional[Columns] = None
    """
    If set then schema is used to generate source structure
    in case actual source does not provide data in which case
    generation of ephemeral view fails.
    """
    create_statement: str | None = None
    """
    Create statement for given table. Must be in requested dialect.
    """
    show_schema: t.Optional[bool] = None
    """
    If true then schema is automatically detected from
    the input data and logged
    '"""


class PhysicalConnection(Connection):
    locator: str
    """
    Table name or file name or URI, or other identifier
    required by the connection.
    """


class EphemeralConnection(Connection):
    locator: str | None = None


class DeltatableConnection(PhysicalConnection, VersionedConnection):
    """"""

    kind: t.Literal["Deltatable"]  # type: ignore[assignment]
    """"""
    partition: t.Optional[t.List[str]] = None
    """
    If set then delta table is partitioned using
    specified fields for faster reads
    """
    data_mode: str = "error"
    """
    Data mode for write operation. For Deltatable
    valid options are:

    - `append` adds new data
    - `overwrite` replaces all data
    - `error` fails (table is read-only)
    """
    schema_mode: t.Optional[str] = None
    """
    Deltatable schema behaviour. If not set then write fails
    with error in case the data schema does not match existing
    table schema.

    Schema evolution options:

    - `merge` adds new columns from data
    - `overwrite` adds new columns, drops missing
    """
    optimize: t.Optional[t.Union[str, int]] = 1
    """
    Use with Deltatable and other engines whether to optimize
    after each sink operation. With larger tables this may be a
    lengthy synchronous operation.

    Default value is optimize and vacuum with 7 day retention.

    # Deltatable

      Values are `optimize,vacuum,all,Literal[int]`.
      If value is literal int is provided then parts older than
      number of days will be removed. Note that this will override
      default retention days.

    String options `vacuum,all` are equivalent to 0.
    """


class ParquetConnection(PhysicalConnection):
    """"""

    kind: t.Literal["Parquet"]  # type: ignore[assignment]


class JSONConnection(PhysicalConnection):
    """"""

    kind: t.Literal["JSON"]  # type: ignore[assignment]


class CSVConnection(PhysicalConnection):
    """"""

    kind: t.Literal["CSV"]  # type: ignore[assignment]


class ClickhouseConnection(PhysicalConnection, VersionedConnection):
    """"""

    kind: t.Literal["Clickhouse"]  # type: ignore[assignment]
    """"""
    version: t.Optional[str] = None
    """
    Field for record version timestamp
    """
    key: t.Optional[t.List[str]] = None
    """
    List of versioned fields
    """


class BigQueryConnection(PhysicalConnection, VersionedConnection):
    """"""

    kind: t.Literal["BigQuery"]  # type: ignore[assignment]
    """"""
    version: t.Optional[str] = None
    """
    Field for record version timestamp
    """
    key: t.Optional[t.List[str]] = None
    """
    List of versioned fields
    """
    partition: t.Optional[t.List[str]] = None
    """
    If set then delta table is partitioned using
    specified fields for faster reads
    """
    data_mode: str = "error"
    """
    Data mode for write operation. For Deltatable
    valid options are:

    - `append` adds new data
    - `overwrite` replaces all data
    - `error` fails (table is read-only)
    """
    schema_mode: t.Optional[str] = None
    """
    Deltatable schema behaviour. If not set then write fails
    with error in case the data schema does not match existing
    table schema.

    Schema evolution options:

    - `merge` adds new columns from data
    - `overwrite` adds new columns, drops missing
    """


class FileConnection(PhysicalConnection):
    """"""

    kind: t.Literal["File"]  # type: ignore[assignment]


class VariableConnection(PhysicalConnection):
    """"""

    kind: t.Literal["Variable"]  # type: ignore[assignment]


class CustomConnection(PhysicalConnection):
    """Custom connection provider. Custom connection may implement
    its own logic but must derive from base Connection class, and expose
    tap(), sink(), sql() and show_schema() even if they are no-op.
    """

    kind: t.Literal["CustomConnection"]  # type: ignore
    module: str
    """Python module where the connection class is defined"""
    classname: str
    """Name of the class to load from the module"""
    params: t.Optional[dict] = PydanticField(default_factory=dict)
    """Free-form configuration parameters passed to the loaded class"""
    # Following attributes are rquired by base class.
    # TODO: Refactor to derive from BaseConnection model

    def load(self) -> t.Type[Connection]:
        """
        Dynamically load the connection class from the given module and member.

        Returns:
            A subclass of Connection.

        Raises:
            ImportError: If the module or member can't be imported.
        """
        mod = importlib.import_module(self.module)
        try:
            cls = getattr(mod, self.classname)
        except AttributeError as e:
            raise ImportError(
                f"Could not load '{self.classname}' from module '{self.module}'"
            ) from e
        # TODO: Refactor imports to facilitate this check early
        # if not issubclass(cls, Connection):
        #     raise TypeError(
        #         f"{cls.__name__} is not a subclass of Connection"
        #     )

        return cls


@deprecated("Connection is not supported, will be removed without notice")
class Dimension(EphemeralConnection):
    kind: t.Literal["Dimension"]  # type: ignore[assignment]
    module: str
    config: t.Optional[t.Any] = None
    fields: t.Optional[t.List[Connection]] = None  # type: ignore[assignment]

    def load(self) -> t.Type[Connection]:
        """
        Dynamically load the connection class from the given module and member.

        Returns:
            A subclass of Connection.

        Raises:
            ImportError: If the module or member can't be imported.
        """
        mod = importlib.import_module(self.module)
        try:
            cls = getattr(mod, self.kind)
        except AttributeError as e:
            raise ImportError(
                f"Could not load '{self.kind}' from module '{self.module}'"
            ) from e

        return cls


class RestConnection(EphemeralConnection):
    """
    Configuration for a REST-based data connection.

    This model defines how to configure and execute REST API requests
    as part of a pipeline step. It includes the request definition,
    client behavior (e.g., retries, headers), and optional schema discovery.
    """

    kind: t.Literal["Rest"]  # type: ignore
    """Specifies the connection type as "Rest"."""
    client: RestClientConfig
    """Configuration for the REST client (e.g., base URL, headers, auth)."""
    request: Request
    """Request template specifying method, path, body, etc."""
    fields: t.Optional[Columns] = None
    """Optional schema definition used to validate or transform
    response data.

    It is recommended to manually specify the schema after initial
    discovery. This ensures downstream pipeline stages remain stable,
    even when the remote API returns no results (e.g., due to no updates
    in an incremental fetch). Explicit schema prevents silent failures
    or schema drift in such cases.
    """
    show_schema: t.Optional[bool] = None
    """
    If True, the connector will attempt to infer
    or display the response schema automatically.
    """
