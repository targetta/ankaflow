import typing as t
import logging
from pypika import PostgreSQLQuery, Column
from sqlglot.dialects.dialect import Dialects
import re

from ..common.path import PathFactory, CommonPath, RemotePath
from ..common.util import (
    build_ranked_query,
    validate_simple_query,
    ConfigResolver,
)
from .. import models as m
from ..models.configs import ConnectionConfiguration
from ..models.connections import PhysicalConnection, VersionedConnection
from .. import errors as e
from ..internal import DDB


log = logging.getLogger(__name__)


class UnionConnection(PhysicalConnection, VersionedConnection): ...

_LOCATOR_PATTERNS = [
    # Matches single quotes
    r"(delta_scan)\(\s*'([^']+)'\s*",
    r"(read_parquet)\(\s*'([^']+)'\s*",

    # Matches double quotes
    r'(delta_scan)\(\s*"([^"]+)"\s*',
    r'(read_parquet)\(\s*"([^"]+)"\s*',
]


class Locator:
    def __init__(self, config: ConnectionConfiguration) -> None:
        """
        Initializes the Locator with connection configuration.

        Args:
            config (ConnectionConfiguration): Config with bucket, prefix, etc.
        """
        self.cfg = config
        self.resolver = ConfigResolver(config)

    @property
    def wildcard(self) -> str | None:
        return self.resolver.resolve("locator_wildcard")

    @property
    def bucket(self) -> str | None:
        return self.resolver.resolve("bucket")

    @property
    def prefix(self) -> str | None:
        return self.resolver.resolve("data_prefix")

    def locate(self, name: str, use_wildcard: bool = False) -> CommonPath:
        """
        Resolves a full path using bucket, prefix, and optional wildcard substitution.

        Args:
            name (str): Relative or absolute path string.
            use_wildcard (bool): Whether to apply wildcard regex substitution.

        Returns:
            CommonPath: A fully resolved path.
        """  # noqa: E501
        if use_wildcard and self.wildcard:
            pattern = re.compile(self.wildcard[0])
            name = re.sub(pattern, self.wildcard[1], name)

        name_path = PathFactory.make(name)

        # Rule: Absolute remote path must remain as is
        if isinstance(name_path, RemotePath):
            return name_path

        # Rule: root must exist and be absolute
        if not self.bucket:
            raise ValueError("Root not specified")
        root_path = PathFactory.make(self.bucket)
        if not root_path.is_absolute():
            raise ValueError("Root must be absolute")

        # Rule: absolute name path directly under root
        if name_path.is_absolute():
            name_path = PathFactory.make(name_path.path.lstrip("/"))
            return root_path.joinpath(*name_path.parts)

        # Rule: If name path is relative, it must be under bucket/prefix/.
        # Normalize the prefix by stripping any leading slashes.
        prefix_str = (self.prefix or "").lstrip("/")
        
        prefix_path_obj = None
        if prefix_str:
            prefix_path_obj = PathFactory.make(prefix_str)
            # Guard 1: Ensure prefix is relative
            if prefix_path_obj.is_absolute():
                raise ValueError(f"Configured data_prefix '{self.prefix}' must be a relative path.")  # noqa: E501
            # Guard 2: Prevent prefix from 'sneaking out' of root (via '..')
            # This is a basic check.A more comprehensive check might
            # involve full path resolution and comparison, but typically,
            # preventing '..' at the prefix level is sufficient
            # if the Path object's joinpath handles it correctly.
            if any(part == ".." for part in prefix_path_obj.parts):
                raise ValueError(f"Configured data_prefix '{self.prefix}' cannot contain '..' segments.")  # noqa: E501

        # Build the full prefix path: root_path + prefix_str parts
        # If no prefix_str, prefix_path will just be root_path.
        prefix_path = root_path
        if prefix_path_obj:
            prefix_path = root_path.joinpath(*prefix_path_obj.parts)

        # Join with prefix (already includes root)
        return prefix_path.joinpath(*name_path.parts)


class Schema:
    """
    Class for working with table schemas
    """

    def __init__(self, duck: DDB):
        self.c = duck
        self.schema_ = None  # type: ignore[assignment]

    def generate(
        self, table: str, fields: m.Columns, exists_ok: bool = False
    ) -> str:
        """
        Generates CREATE TABLE statement from schema.

        Args:
            name (str): Table name to create
            schema (m.Fields): List of fields (names and dtypes)
            exists_ok (bool, optional): If true then create statement
                includes `IF NOT EXISTS`. Defaults to False.

        Returns:
            str: CREATE statement
        """
        cols = []
        for f in fields:
            cols.append(Column(f.name, f.type))  # type: ignore[assignment]
        creator = PostgreSQLQuery().create_table(table)
        if exists_ok:
            creator = creator.if_not_exists()
        creator = creator.columns(*cols)
        return creator.get_sql()

    async def show(self, table: str, query: str | None = None) -> m.Columns:
        # OK, refactored. Do not touch
        """
        Returns the schema of a table as a validated Fields model.

        Args:
            table (str): The name of the DuckDB table/view.

        Returns:
            m.Fields: List of validated fields.
        """
        if query:
            qry = f"DESCRIBE {query}"
        else:
            qry = f'DESCRIBE "{table}"'
        rel = await self.c.sql(qry)
        df = await rel.df()
        df = df.rename(columns={"column_name": "name", "column_type": "type"})
        items = [
            m.Column.model_validate(it) for it in df.to_dict(orient="records")
        ]
        return m.Columns.model_validate(items)

    # TODO cleanup once no known problems
    # async def get_fields(self, table: str) -> t.List[m.Field]:
    #     # Don't know why this is required. Apparently unused
    #     rel = await self.c.sql(f"FROM Fn.columns({table})")
    #     df = await rel.df()
    #     res = df.rename(columns={"column_name": "name", "data_type": "type"})
    #     items = [
    #         m.Field.model_validate(it) for it in res.to_dict(orient="records")
    #     ]
    #     return items

    # async def infer_df(self, table: str, df: pd.DataFrame) -> t.List[m.Field]:
    #     # TODO: Remove unused method
    #     name = f"temp_{table}"
    #     if df.empty:
    #         await self.c.sql(
    #             f"""
    #             CREATE OR REPLACE TABLE {name}
    #             (empty_fieldset VARCHAR)
    #             """
    #         )
    #     else:
    #         await self.c.sql(
    #             f"""
    #             CREATE OR REPLACE TABLE {name}
    #             AS SELECT * FROM df
    #             """
    #         )
    #     self.schema_: t.List[m.Field] = await self.get_fields(name)
    #     return self.schema_

    def error(self, message) -> m.Columns:
        # Ok refactored
        return m.Columns.error(str(message))


class Connection:
    """
    Base class for connections such as Deltatable or parquet.
    """

    dialect = Dialects.POSTGRES

    def __init__(
        self,
        duck: DDB,
        name: str,
        connection: UnionConnection,
        context: m.FlowContext,
        variables: m.Variables,
        logger: logging.Logger = None,  # type: ignore[assignment]
    ) -> None:
        self.c = duck
        self.name = name
        self.limit = 0
        self._fields = connection.fields
        self.cfg: m.ConnectionConfiguration = t.cast(
            m.ConnectionConfiguration, connection.config
        )
        self.conn = connection
        self.ctx = context
        self.vars = variables
        self.log = logger
        self.locator = Locator(self.cfg)  # type: ignore[assignment]
        self.schema_ = Schema(self.c)
        if not logger:
            self.log = logging.getLogger()
            self.log.addHandler(logging.NullHandler())
        self.init()

    def init(self):
        """
        Additional initialization called from within
        __init__.
        """
        pass

    def _raw_sql_rewriter(self, sql: str) -> str:
        """Rewrites short locators in supported DuckDB table
        functions to full locators.

        Args:
            sql (str): The original SQL query containing short locators.

        Returns:
            str: SQL query with short locators replaced with full resolved paths.
        """
        for pattern in _LOCATOR_PATTERNS:
            def _replace(match: re.Match) -> str:
                func, short_locator = match.groups()

                # 1. Determine if absolute
                name_path = PathFactory.make(short_locator)
                if name_path.is_absolute():
                    return match.group(0)  # Leave as is

                # 2. Resolve full locator
                long_locator = self.locate(use_wildcard=True)

                # 3. Safety check: ensure short is in connection
                if short_locator != self.conn.locator:
                    raise ValueError(
                        f"Locator '{short_locator}' does not match conection: '{self.conn.locator}'"  # noqa: E501
                    )

                # 4. Return with replacement, preserving trailing kwargs
                return match.group(0).replace(short_locator, long_locator, 1)

            sql = re.sub(pattern, _replace, sql, flags=re.IGNORECASE)

        return sql

    def locate(self, name: str = None, use_wildcard: bool = False) -> str:  # type: ignore[assignment]
        name = str(name or self.conn.locator)
        return str(self.locator.locate(name, use_wildcard=use_wildcard))

    async def create_table(self):
        if not self.conn.fields:
            raise e.ConfigurationError("Fields not specified")
        stmt = self.schema_.generate(self.name, self.conn.fields)
        await self.c.sql(stmt)  # type: ignore[assignment]

    # def validate(self, df: pd.DataFrame):
    #     if df.empty:
    #         if not self.conn.fields:
    #             raise e.DuctError(
    #                 "No data was returned and schema is not specified"
    #             )
    #     return df

    def ranking(
        self, selectable: str, query: str, validate_simple=False
    ) -> tuple[str, str]:
        """
        Injects row ranking logic to deduplicate records based on versioning.

        If the connection defines a `version` and `key`, modifies the query
        to include a `ROW_NUMBER() OVER (...) AS __rank__` column and wraps it
        in a subquery that can be filtered using `WHERE __rank__ = 1`.

        Returns:
            tuple[str, str]: Transformed SQL query and the WHERE clause if applicable.
        """  # noqa: E501
        if not isinstance(self.conn, VersionedConnection):
            return query, ""
        apply_ranking = bool(self.conn.version and self.conn.key)

        # Validate before building
        validate_simple_query(query, apply_ranking)
        return build_ranked_query(
            query=query,
            selectable=selectable,
            version=self.conn.version,
            keys=self.conn.key,
            dialect=self.dialect,
        )

    async def tap(self, query: t.Optional[str] = None, limit: int = 0):
        """
        Implements loading from source storage.

        Args:
            limit (int, optional): Reduce the number of rows returned.
                Defaults to 0.
        """
        raise NotImplementedError("Store object must be implemented")

    async def sink(self, from_name: str):
        """
        Sink or store data from given `name` (typically previous stage)

        Args:
            name (string): table name to use as source
        """
        raise NotImplementedError("Store object must be implemented")

    async def show_schema(self) -> m.Columns:
        raise NotImplementedError("Schema not available")

    async def sql(self, statement: str) -> t.Any:
        """
        Execute raw sql using the specified connection (if supported).
        """
        raise NotImplementedError("SQL execution not available")
