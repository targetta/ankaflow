import deltalake as dl
from deltalake.exceptions import DeltaError
from deltalake.fs import DeltaStorageHandler
import typing as t
from sqlglot import parse_one
import logging
import pyarrow as pa
import pandas as pd
from enum import Enum

from . import errors as e
from .. import errors as ee
from .. import models as m
from ..internal import CatalogException
from .connection import Connection
from ..common.util import pandas_to_pyarrow, duckdb_to_pyarrow_type

log = logging.getLogger(__name__)


class SinkStrategy(str, Enum):
    SKIP = "skip"
    CREATE = "create"
    WRITE = "write"


class Deltatable(Connection):
    def init(self):
        # TODO: implement locking client
        # https://delta-io.github.io/delta-rs/usage/writing/writing-to-s3-with-locking-provider/
        self.delta_opts = {}
        if self.cfg.s3.access_key_id:  # type: ignore
            self.delta_opts.update({
                # Without locking client
                # Atomic rename requires a LockClient for S3 backends
                "aws_s3_allow_unsafe_rename": "true",
                "aws_ec2_metadata_disabled": "true",
                "aws_access_key_id": self.cfg.s3.access_key_id,  # type: ignore
                "aws_secret_access_key": self.cfg.s3.secret_access_key,  # type: ignore
                "aws_region": self.cfg.s3.region,  # type: ignore
            })
        if self.cfg.gs.credential_file:
            self.delta_opts.update({"service_account_path": self.cfg.gs.credential_file})

    async def _maybe_optimize(self, uri: str):  # Enhanced with validation and warnings
        """
        Optionally optimizes and vacuums the Delta table
        based on self.conn.optimize.

        Supported values for self.conn.optimize:
        - "optimize" → compact files only
        - "vacuum" → vacuum old files (default retention)
        - "all" → optimize and vacuum (no retention wait)
        - int (e.g., 7) → optimize and vacuum with retention in days

        Args:
            uri (str): Delta table URI.
        """
        option = self.conn.optimize
        opt = vac = False
        age = 7 * 24  # default retention in hours
        max_age = 365 * 24  # 1 year in hours

        try:
            option = int(option)  # type: ignore - already established to be note none
            if option < 0:
                self.log.warning(
                    f"Negative optimize retention not allowed: {option} days"
                )
                return
            if option * 24 > max_age:
                self.log.warning(f"Optimize retention exceeds max: {option} days > 365")
                return
        except (ValueError, TypeError):
            pass

        if isinstance(option, int):
            opt = vac = True
            age = option * 24
        elif option == "optimize":
            opt = True
        elif option == "vacuum":
            vac = True
            age = 0
        elif option == "all":
            opt = vac = True
            age = 0
        else:
            self.log.warning(
                f"Unknown optimize option '{option}', skipping optimization."
            )
            return

        try:
            dt = dl.DeltaTable(uri, storage_options=self.delta_opts)
            if opt:
                dt.optimize.compact()
            if vac:
                dt.vacuum(age, dry_run=False, enforce_retention_duration=False)
            dt.cleanup_metadata()
        except dl.exceptions.TableNotFoundError:  # type: ignore
            pass
        except Exception as e:
            log.exception(e)

    def _make_cte(self, selectable: str) -> str:
        """
        Materializes the input source using alias `t`.

        Example:
            delta_scan(...) → WITH t AS (SELECT * FROM delta_scan(...))
        """
        return f"WITH t AS (SELECT * FROM {selectable})"

    def _make_final_select(self, cte: str, qry: str, where_clause: str) -> str:
        """
        Assembles the full SQL SELECT wrapped by a CTE and optional filter.

        Args:
            cte: The WITH clause
            qry: Main query (e.g., SELECT * FROM ...)
            where_clause: Optional WHERE clause (e.g., WHERE __rank__ = 1)

        Returns:
            str: Complete CREATE TABLE statement.
        """
        return f"""
            {cte}
            CREATE TABLE "{self.name}" AS
            SELECT * FROM ({qry}) {where_clause}
        """

    async def tap(
        self,
        query: t.Optional[str] = "SELECT * FROM Deltatable",
        limit: int = 0,
    ) -> None:
        if not query:
            raise ValueError("Query is mandatory")

        # Use delta_scan() directly in the FROM clause
        selectable = f"delta_scan('{self.locate(use_wildcard=True)}')"

        # Replace the source with the real delta_scan(...) call
        qry, where_clause = self.ranking(selectable, query, validate_simple=True)

        # Apply optional limit
        if limit:
            qry = parse_one(qry).subquery().limit(limit).sql()  # type: ignore[attr-defined]

        # Inline into final CREATE TABLE statement (no CTE!)
        final_sql = f"""
            CREATE TABLE "{self.name}" AS
            SELECT * FROM ({qry}) {where_clause}
        """.strip()

        try:
            await self.c.sql(final_sql)
        except Exception as ex:
            if "MissingVersionError" in str(ex) or "InvalidTableLocation" in str(ex):
                raise e.UnrecoverableTapError(f"Tap source missing: {self.name}")
            log.exception(ex)
            raise

    async def _create_temp_view(self, from_name: str) -> str:
        """
        Creates a temporary view based on the source table name.

        Args:
            from_name: The source table/view name.

        Returns:
            str: The name of the temporary view created.
        """
        view_name = f"tmp_{from_name}"
        await self.c.sql(
            f'CREATE OR REPLACE VIEW "{view_name}" AS SELECT * FROM "{from_name}"'
        )
        return view_name

    async def _read_view(self, view_name: str):
        """
        Reads data from a temporary view into a DataFrame.

        Args:
            view_name: The temporary view name to read from.

        Returns:
            pd.DataFrame: The resulting DataFrame.
        """
        rel = await self.c.sql(f'SELECT * FROM "{view_name}"')
        return await rel.df()

    async def _drop_view(self, view_name: str):
        """
        Drops a temporary view by name.

        Args:
            view_name: The view name to drop.
        """
        await self.c.sql(f'DROP VIEW IF EXISTS "{view_name}"')

    async def _to_arrow(
        self, df: pd.DataFrame, schema: t.Union[m.Fields, t.List[m.Field]]
    ) -> pa.Table:
        """
        Converts a DataFrame to a PyArrow Table using the provided schema.

        Args:
            df: The pandas DataFrame to convert.
            schema: The schema to apply when converting.

        Returns:
            pa.Table: A PyArrow Table.
        """
        return pandas_to_pyarrow(df, schema)

    def _generate_metadata(self) -> str:
        """
        Generates metadata for new Delta table creation.

        Returns:
            str: A stringified dictionary with table metadata.
        """
        return str(
            {
                "note": "Table rows are versioned",
                "pk": ",".join(self.conn.key or []),
                "ver": self.conn.version,
                "part": ",".join(self.conn.partition or []),
            }
        )

    def _make_delta_kwargs(self, meta: t.Optional[str], create_flag: bool) -> dict:
        """
        Constructs keyword arguments for the dl.write_deltalake call.

        Args:
            meta: Optional metadata description for table creation.
            create_flag: True if creating, False if appending.

        Returns:
            dict: Parameters for dl.write_deltalake.
        """
        kwargs = {
            "mode": self.conn.data_mode,
            "schema_mode": self.conn.schema_mode,
            "storage_options": self.delta_opts,
        }
        if create_flag:
            kwargs.update(
                {
                    "partition_by": self.conn.partition,
                    "description": meta,
                }
            )
        return kwargs

    async def _write_deltatable(
        self, uri: str, tbl: pa.Table, create_flag: bool = False
    ):
        """
        Writes to an existing Delta table.

        Args:
            uri: Delta table URI
            tbl: Arrow Table to write
        """
        try:
            dl.write_deltalake(
                uri,
                tbl,
                **self._make_delta_kwargs(meta=None, create_flag=create_flag),
            )
            self.log.debug(
                f"Written {tbl.num_rows} records to {'existing' if not create_flag else 'new'} Delta table"  # noqa:E501
            )
        except DeltaError as ex:
            raise e.UnrecoverableSinkError(
                f"Write failed: {ex}\ndata_mode:{self.conn.data_mode}\nschema_mode:{self.conn.schema_mode}"  # noqa:E501
            ) from None

    async def _create_deltatable(self, uri: str):
        """
        Creates an empty Delta table using the schema
        defined in self.conn.fields.

        Args:
            uri (str): Delta table location to initialize.
        """
        if not self.conn.fields:
            raise e.UnrecoverableSinkError(
                "Cannot create empty Delta table: no schema fields provided."
            )  # noqa:E501

        schema = pa.schema(
            [
                (field.name, duckdb_to_pyarrow_type(field.type))
                for field in self.conn.fields
            ]
        )

        def default_for_type(pa_type: pa.DataType) -> t.Any:
            if pa.types.is_integer(pa_type):
                return 0
            if pa.types.is_floating(pa_type):
                return 0.0
            if pa.types.is_string(pa_type):
                return ""
            return None  # fallback

        dummy_data = {
            f.name: pa.array([default_for_type(f.type)], type=f.type) for f in schema
        }
        dummy_table = pa.table(dummy_data)

        try:
            dl.write_deltalake(
                uri,
                data=dummy_table,
                mode="overwrite",
                schema_mode="overwrite",
                storage_options=self.delta_opts,
            )
            self.log.info(
                f"Created Delta table with dummy row at {uri} using provided schema."
            )  # noqa:E501
            # Optional: immediately truncate to clear dummy
            # dt = dl.DeltaTable(uri, storage_options=self.delta_opts)
            # dt.delete()
        except Exception as ex:
            raise e.UnrecoverableSinkError(
                f"Failed to create empty Delta table: {ex}"
            ) from None  # noqa:E501

    async def _create_strategy(self, df: pd.DataFrame) -> SinkStrategy:
        """
        Determines sink strategy based on presence of data and schema.

        Returns:
            SinkStrategy: One of SKIP, CREATE, or WRITE
        """
        if not self.conn.fields and df.empty:
            return SinkStrategy.SKIP
        if self.conn.fields and df.empty:
            return SinkStrategy.CREATE
        return SinkStrategy.WRITE

    async def _infer_schema(self, df: t.Optional[pd.DataFrame] = None) -> pa.Schema:
        """
        Infers schema either from DataFrame or from self.conn.fields.

        Args:
            df: Optional DataFrame to infer schema from.
            If None or empty, uses declared schema.

        Returns:
            pa.Schema: PyArrow schema to use for Arrow
                conversion or table creation.

        Raises:
            e.ConfigurationError: if no data and no schema are available.
        """
        if df is not None and not df.empty:
            return pa.Schema.from_pandas(df, preserve_index=False)

        if not self.conn.fields:
            raise ee.ConfigurationError(
                "No data or declared schema available to infer schema."
            )

        return pa.schema(
            [
                (field.name, duckdb_to_pyarrow_type(field.type))
                for field in self.conn.fields
            ]
        )

    async def sink(self, from_name: str):
        """
        DeltaTable sink behavior is driven by available schema and data:

        Strategy Matrix:
        --------------------------------------------------------------------
        | Schema (conn.fields) | Data Available | Resulting Strategy       |
        |----------------------|----------------|--------------------------|
        | No                   | No             | SKIP → no action         |
        | Yes                  | No             | CREATE → define schema   |
        | Yes                  | Yes            | WRITE → create & write   |
        | No                   | Yes            | WRITE → infer & write    |
        --------------------------------------------------------------------

        When table already exists (is_deltatable):
        - CREATE → skip creation, write only if data present
        - WRITE  → directly append/merge data to table

        Optimizations (vacuum/compact) applied if self.conn.optimize is set.
        """
        uri = self.locate()
        view = await self._create_temp_view(from_name)
        df = await self._read_view(view)
        await self._drop_view(view)

        strategy = await self._create_strategy(df)
        if strategy == SinkStrategy.SKIP:
            self.log.info(f"{self.name}: No schema or data to insert.")
            return

        schema = await self._infer_schema(
            df if strategy == SinkStrategy.WRITE else None
        )

        tbl = await self._to_arrow(df, schema)

        is_delta = dl.DeltaTable.is_deltatable(uri, storage_options=self.delta_opts)
        if strategy == SinkStrategy.CREATE:
            # Only create explicitly if no Delta table exists and we have schema
            if not is_delta and self.conn.fields:
                await self._create_deltatable(uri)

            # Always write if there's data
            if not df.empty:
                await self._write_deltatable(uri, tbl)
        elif strategy == SinkStrategy.WRITE:
            if is_delta:
                await self._write_deltatable(uri, tbl)
            else:
                await self._write_deltatable(uri, tbl, create_flag=not is_delta)

        if self.conn.optimize is not None:
            try:
                await self._maybe_optimize(uri)
            except DeltaError as ex:
                raise e.ConnectionException(f"Optimize failed: {ex}") from None

    async def show_schema(self) -> m.Fields:
        try:
            return await self.schema_.show(self.name)
        except CatalogException:
            pass
        try:
            # TODO: actually read 1 record
            # this allows dependant transforms to be explained
            # Idea: add 'materialize option, or create arrow table
            # with single row and insert using nanoarrow
            path = self.locate()

            if not dl.DeltaTable.is_deltatable(path):
                return m.Fields.error(f"Path is not a delta table: {path}")

            table = dl.DeltaTable(path)
            arrow_schema: pa.Schema = table.schema().to_pyarrow()
            fields = [
                m.Field(name=field.name, type=str(field.type)) for field in arrow_schema
            ]

            return m.Fields.model_validate(fields)

        except Exception as err:
            self.log.warning(f"Failed to read Delta schema for {self.name}: {err}")
            return self.schema_.error(str(err))

    async def sql(self, statement: str):
        """
        Executes a limited subset of SQL-like operations on a Delta table.

        Supported commands:
        - DROP Deltatable
        - TRUNCATE Deltatable

        All other statements raise an error.
        """
        stmt = statement.strip().lower()
        if stmt == "drop deltatable":
            return self._drop_deltatable()
        elif stmt == "truncate deltatable":
            return await self._truncate_deltatable()
        else:
            raise ValueError(f"Invalid Delta SQL command: {statement}")

    def _drop_deltatable(self):
        """Deletes the entire Delta table directory and metadata."""
        uri = self.locate()
        try:
            fs = DeltaStorageHandler(uri, options=self.delta_opts)
            fs.delete_dir(uri)
            self.log.info(f"Delta table dropped: {uri}")
        except Exception as ex:
            raise e.ConnectionException(f"Drop failed: {ex}") from None

    async def _truncate_deltatable(self):
        """Deletes all data files but preserves table metadata."""
        uri = self.locate()
        try:
            dt = dl.DeltaTable(uri, storage_options=self.delta_opts)
            dt.delete()  # removes all rows
            if self.conn.optimize:
                await self._maybe_optimize(uri)
            self.log.info(f"Delta table truncated: {uri}")
        except Exception as ex:
            raise e.ConnectionException(f"Truncate failed: {ex}") from None
