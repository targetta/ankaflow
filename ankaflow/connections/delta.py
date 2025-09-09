import shlex
import deltalake as dl
from deltalake.exceptions import DeltaError
from deltalake.fs import DeltaStorageHandler
import typing as t
from sqlglot import parse_one
import logging
import pyarrow as pa
import asyncio

from . import errors as e
from .. import errors as ee
from .. import models as m
from ..models.enums import SinkStrategy
from ..models.connections import DeltatableConnection
from ..internal import CatalogException
from .connection import Connection
from ..common.util import duckdb_to_pyarrow_type

log = logging.getLogger(__name__)

ArrowLike = t.Union[pa.Table, pa.RecordBatchReader]


class Deltatable(Connection):
    def init(self):
        self.conn = t.cast(DeltatableConnection, self.conn)
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
            self.delta_opts.update({
                "service_account_path": self.cfg.gs.credential_file
            })

    async def _maybe_optimize(
        self, uri: str
    ):  # Enhanced with validation and warnings
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
                self.log.warning(
                    f"Optimize retention exceeds max: {option} days > 365"
                )
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
        query: t.Optional[str] = None,
        limit: int = 0,
    ) -> None:
        if not query:
            raise ValueError("Query is mandatory")

        if self.conn.raw_dispatch:
            rewritten = self._raw_sql_rewriter(query)
            final_sql = f"""
                CREATE TABLE "{self.name}"
                AS
                {rewritten}
                """.strip()
        else:
            # Use delta_scan() directly in the FROM clause
            selectable = f"delta_scan('{self.locate(use_wildcard=True)}')"

            # Replace the source with the real delta_scan(...) call
            qry, where_clause = self.ranking(
                selectable, query, validate_simple=True
            )

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
            if "MissingVersionError" in str(
                ex
            ) or "InvalidTableLocation" in str(ex):
                raise e.UnrecoverableTapError(
                    f"Tap source missing: {self.name}"
                )
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

    async def _read_view(self, view_name: str) -> ArrowLike:
        """Return Arrow result directly from DuckDB.

        Args:
            view_name (str): Temporary view name to read from.

        Returns:
            pa.RecordBatchReader | pa.Table: Arrow result for the sink.
        """
        rel = await self.c.sql(f'SELECT * FROM "{view_name}"')
        # Prefer streaming for large results if your relation supports it:
        # return await rel.record_batch_reader()  # wrapper supports awaitable
        return await rel.arrow()  # if wrapper exposes an awaitable Arrow table

    async def _drop_view(self, view_name: str):
        """
        Drops a temporary view by name.

        Args:
            view_name: The view name to drop.
        """
        await self.c.sql(f'DROP VIEW IF EXISTS "{view_name}"')

    async def _to_arrow(self, data: ArrowLike, schema: pa.Schema) -> ArrowLike:
        """Normalize Arrow data if needed.

        Args:
            data (ArrowLike): Arrow table or reader.
            schema (pa.Schema): Target schema (used only if you must cast).

        Returns:
            ArrowLike: Arrow reader (preferred) or combined table.
        """
        # If your writer accepts a RecordBatchReader, return as-is.
        if isinstance(data, pa.RecordBatchReader):
            return data

        # For pa.Table, ensure predictable layout (optional):
        return data.combine_chunks()

    def _generate_metadata(self) -> str:
        """
        Generates metadata for new Delta table creation.

        Returns:
            str: A stringified dictionary with table metadata.
        """
        return str({
            "note": "Table rows are versioned",
            "pk": ",".join(self.conn.key or []),
            "ver": self.conn.version,
            "part": ",".join(self.conn.partition or []),
        })

    def _make_delta_kwargs(
        self, meta: t.Optional[str], create_flag: bool
    ) -> dict:
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
            kwargs.update({
                "partition_by": self.conn.partition,
                "description": meta,
            })
        if self.conn.writer_features:
            kwargs["writer_features"] = self.conn.writer_features
        return kwargs

    async def _write_deltatable(
        self, uri: str, tbl: ArrowLike, create_flag: bool = False
    ):
        """
        Writes to an existing Delta table.

        Args:
            uri: Delta table URI
            tbl: Arrow Table to write
        """
        delta_kwargs = self._make_delta_kwargs(
            meta=None, create_flag=create_flag
        )
        try:
            """Write Arrow to Delta; stream if possible."""
            # If delta-rs accepts readers directly, pass the reader.
            if isinstance(tbl, pa.RecordBatchReader):
                dl.write_deltalake(
                    uri,
                    tbl,
                    **delta_kwargs,
                )
                return

            # pa.Table path:
            dl.write_deltalake(
                uri,
                tbl,
                **delta_kwargs,
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

        schema = pa.schema([
            (field.name, duckdb_to_pyarrow_type(field.type))
            for field in self.conn.fields
        ])

        def default_for_type(pa_type: pa.DataType) -> t.Any:
            if pa.types.is_integer(pa_type):
                return 0
            if pa.types.is_floating(pa_type):
                return 0.0
            if pa.types.is_string(pa_type):
                return ""
            return None  # fallback

        dummy_data = {
            f.name: pa.array([default_for_type(f.type)], type=f.type)
            for f in schema
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

    def _create_strategy(self, rows: int) -> SinkStrategy:
        """Decide SKIP/CREATE/WRITE using row count and declared schema.

        Args:
            rows (int): Number of rows available to write.

        Returns:
            SinkStrategy: SKIP, CREATE, or WRITE.
        """
        has_schema = bool(self.conn.fields)
        if not has_schema and rows == 0:
            return SinkStrategy.SKIP
        if has_schema and rows == 0:
            return SinkStrategy.CREATE
        return SinkStrategy.WRITE

    async def _count_rows(self, view_name: str) -> int:
        """Count rows in the temp view without materializing data.

        Args:
            view_name (str): Temporary view name.

        Returns:
            int: Row count.
        """
        # cast avoids odd integer types
        rel = await self.c.sql(
            f'SELECT COUNT(*)::UBIGINT AS n FROM "{view_name}"' 
        )
        row = await rel.fetchone()  # returns (n,)
        if row is None:
            return 0
        return int(row[0])

    def _cast_dict_to_string(self, tbl: pa.Table) -> pa.Table:
        """Return a table where any dictionary-encoded columns are cast to string.

        Args:
            tbl (pa.Table): Input Arrow table.

        Returns:
            pa.Table: Table with dictionary columns converted to pa.string().
        """
        # Fast path: if no dictionary columns, return as-is.
        if all(
            not pa.types.is_dictionary(col.type) for col in tbl.itercolumns()
        ):
            return tbl

        names = tbl.schema.names
        out_cols: list[pa.Array] = []

        for col in tbl.itercolumns():
            if pa.types.is_dictionary(col.type):
                # Avoid building an intermediate Table: convert only this column.
                # to_pylist() is robust; if you need to preserve large binary,
                # replace with col.cast(pa.string()) when upstream supports it.
                out_cols.append(pa.array(col.to_pylist(), type=pa.string()))
            else:
                out_cols.append(col)

        # Reuse metadata if present
        return pa.table(out_cols, names=names, metadata=tbl.schema.metadata)

    async def _infer_schema(
        self, data: t.Optional[ArrowLike] = None
    ) -> pa.Schema:
        """Infer schema from Arrow data or declared fields.

        Args:
            data (pa.RecordBatchReader | pa.Table | None): Arrow data.

        Returns:
            pa.Schema: Schema for writing.
        """
        if data is not None:
            schema = (
                data.schema
                if isinstance(data, pa.RecordBatchReader)
                else data.schema
            )
            return schema

        if not self.conn.fields:
            raise ee.ConfigurationError(
                "No data or declared schema available to infer schema."
            )

        return pa.schema([
            (field.name, duckdb_to_pyarrow_type(field.type))
            for field in self.conn.fields
        ])

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
        rows = await self._count_rows(view)  # NEW
        data = await self._read_view(view)  # Arrow
        await self._drop_view(view)

        strategy = self._create_strategy(rows)
        if strategy == SinkStrategy.SKIP:
            self.log.info(f"{self.name}: No schema or data to insert.")
            return

        schema = await self._infer_schema(
            data if strategy == SinkStrategy.WRITE else None
        )
        arrow_out = await self._to_arrow(data, schema)
        arrow_out = self._cast_dict_to_string(arrow_out)

        is_delta = dl.DeltaTable.is_deltatable(
            uri, storage_options=self.delta_opts
        )

        if strategy == SinkStrategy.CREATE:
            if not is_delta and self.conn.fields:
                await self._create_deltatable(uri)
            if rows > 0:
                await self._write_deltatable(uri, arrow_out)
        elif strategy == SinkStrategy.WRITE:
            await self._write_deltatable(
                uri, arrow_out, create_flag=not is_delta
            )

        self.log.debug(
            f"Written {rows} records to {'existing' if is_delta else 'new'} Delta table"  # noqa:E501
        )

        if self.conn.optimize is not None:
            try:
                await self._maybe_optimize(uri)
            except DeltaError as ex:
                raise e.ConnectionException(f"Optimize failed: {ex}") from None

    async def show_schema(self) -> m.Columns:
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
                return m.Columns.error(f"Path is not a delta table: {path}")

            table = dl.DeltaTable(path)
            arrow_schema: pa.Schema = table.schema().to_pyarrow()
            fields = [
                m.Column(name=field.name, type=str(field.type))
                for field in arrow_schema
            ]

            return m.Columns.model_validate(fields)

        except Exception as err:
            self.log.warning(
                f"Failed to read Delta schema for {self.name}: {err}"
            )
            return self.schema_.error(str(err))

    async def sql(self, statement: str):
        """
        Executes a limited subset of SQL-like operations on a Delta table.

        Supported commands:
        - DROP Deltatable
        - TRUNCATE Deltatable
        - OPTIMIZE Deltatable [COMPACT] [VACUUM] [AGE=<int>[d|h]] [DRY_RUN] [CLEANUP]

        All other statements raise an error.
        """  # noqa: E501
        tokens: t.List[str] = [t.casefold() for t in shlex.split(statement)]
        if not tokens:
            raise ValueError(f"Invalid Delta SQL command: {statement}")

        if tokens == ["drop", "deltatable"]:
            return self._drop_deltatable()
        elif tokens == ["truncate", "deltatable"]:
            return await self._truncate_deltatable()
        elif (
            len(tokens) >= 2
            and tokens[0] == "optimize"
            and tokens[1] == "deltatable"
        ):
            return await self._sql_optimize(
                statement
            )  # pass original for parsing
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

    def _parse_optimize_flags(self, stmt: str) -> dict[str, t.Any]:
        """Parse OPTIMIZE DELTATABLE flags into an options dict.

        Args:
            stmt (str): Original statement (case-insensitive).

        Returns:
            dict[str, Any]: {
                "compact": bool,
                "vacuum": bool,
                "cleanup": bool,
                "dry_run": bool,
                "retention_hours": int,
            }
        """
        toks = [t.casefold() for t in shlex.split(stmt)]
        compact = "compact" in toks
        vacuum = "vacuum" in toks
        cleanup = "cleanup" in toks
        dry_run = "dry_run" in toks

        # Defaulting rule:
        # - If neither COMPACT nor VACUUM given:
        #     - If CLEANUP present → cleanup-only (no compact/vacuum)
        #     - Else → run both compact + vacuum
        if not compact and not vacuum:
            if cleanup:
                compact = False
                vacuum = False
            else:
                compact = True
                vacuum = True

        # AGE parsing; default 7 days unless unused
        retention_hours = 7 * 24
        for tok in toks:
            if tok.startswith("age="):
                val = tok.split("=", 1)[1]
                if val.endswith("h"):
                    retention_hours = int(val[:-1])
                elif val.endswith("d"):
                    retention_hours = int(val[:-1]) * 24
                else:
                    retention_hours = int(val) * 24
                break

        # guardrails
        retention_hours = max(0, min(retention_hours, 365 * 24))

        return {
            "compact": compact,
            "vacuum": vacuum,
            "cleanup": cleanup,
            "dry_run": dry_run,
            "retention_hours": retention_hours,
        }

    async def _sql_optimize(self, statement: str) -> None:
        """Handle 'OPTIMIZE DELTATABLE ...' pseudo-SQL.

        Args:
            statement (str): Full original statement for parsing.
        """
        uri = self.locate()
        opts = self._parse_optimize_flags(statement)

        # delta-rs API is sync -> keep loop responsive
        def _run():
            dt = dl.DeltaTable(uri, storage_options=self.delta_opts)
            if opts["compact"]:
                out = dt.optimize.compact()
                log.debug(out)
            if opts["vacuum"]:
                out = dt.vacuum(
                    opts["retention_hours"],
                    dry_run=opts["dry_run"],
                    # mirror your existing behavior:
                    enforce_retention_duration=False,
                )
                log.debug(out)
            if opts["cleanup"]:
                dt.cleanup_metadata()

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, _run)
        self.log.info(
            f"OPTIMIZE {uri} compact={opts['compact']} vacuum={opts['vacuum']} age={opts['retention_hours']}h dry_run={opts['dry_run']} cleanup={opts['cleanup']}"  # noqa: E501
        )
