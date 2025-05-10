import contextlib
import functools
import typing as t
import logging
import pandas as pd
from clickhouse_driver import Client
from clickhouse_driver.errors import Error as CHError
from duckdb import CatalogException
from sqlglot.dialects.dialect import Dialects

from . import connection as c
from . import errors as e
from .. import models as m
from ..common.util import print_df, ProgressLogger

log = logging.getLogger(__name__)


DEFAULT_STREAM_BLOCK = 50_000


def strip_trace(exc: str | Exception) -> str:
    if isinstance(exc, CHError):
        return str(exc).split(" Stack trace", 1)[0]
    return str(exc)


def with_clickhouse(func):
    @functools.wraps(func)
    async def wrapper(self, *args, **kwargs):
        client_wrapper = ClickhouseClient(self.cfg)
        with client_wrapper.connect() as ch:
            self.client = client_wrapper  # optional: save client for access
            self.ch = ch  # expose raw client if needed
            try:
                return await func(self, *args, **kwargs)
            finally:
                self.ch = None
                self.client = None

    return wrapper


class ClickhouseClient:
    def __init__(self, cfg: "m.ConnectionConfiguration"):
        """Initializes a Clickhouse client wrapper.

        Args:
            cfg (m.ConnectionConfiguration): Connection configuration.
        """
        self.cfg = cfg

    @contextlib.contextmanager
    def connect(self) -> t.Iterator[Client]:
        """Context-managed connection that disconnects cleanly."""
        client = Client(
            host=self.cfg.clickhouse.host,
            port=self.cfg.clickhouse.port,
            user=self.cfg.clickhouse.username,
            password=self.cfg.clickhouse.password,
            database=self.cfg.clickhouse.database,
            send_receive_timeout=1000,
        )
        try:
            yield client
        finally:
            client.disconnect()

    def query_df(self, client: Client, query: str) -> pd.DataFrame:
        """Executes a SELECT query and returns a DataFrame."""
        return client.query_dataframe(query, settings={"use_numpy": True})

    def execute(self, client: Client, query):
        return client.execute(query, with_column_types=True)

    def insert_dataframe(self, client: Client, query: str, df: pd.DataFrame) -> t.Any:
        """Inserts a DataFrame into Clickhouse using the given query."""
        return client.insert_dataframe(query, df, settings={"use_numpy": True})

    def stream_query(
        self, client: Client, query: str, block_size: int | None = None
    ) -> t.Generator:
        """Streams ClickHouse query results.

        Args:
            client (Client): A live Clickhouse connection.
            query (str): SQL query.
            block_size (int): Optional server-side row block size.

        Yields:
            row blocks from ClickHouse.
        """
        settings = {}
        if block_size:
            settings["max_block_size"] = block_size
        for row in client.execute_iter(
            query, with_column_types=True, settings=settings
        ):
            yield row


class Clickhouse(c.Connection):
    dialect = Dialects.CLICKHOUSE

    def init(self) -> None:
        """Initializes connection-specific defaults."""
        # Created by decorator
        self.client: t.Optional[ClickhouseClient] = None
        self.ch: t.Optional[Client] = None
        self._client = ClickhouseClient(self.cfg)
        self._blocksize: int = self.cfg.clickhouse.blocksize or DEFAULT_STREAM_BLOCK
        self.progress = ProgressLogger(None, self.log)

    def locate(self, name: t.Optional[str] = None, use_wildcard: bool = False) -> str:
        """Returns the fully-qualified table reference with validation.

        Args:
            name (Optional[str]): Table name override (unused).
            use_wildcard (bool): Placeholder flag (unused).

        Returns:
            str: Fully-qualified table name.

        Raises:
            ValueError: If locator format is invalid.
        """
        locator = self.conn.locator
        if "." in locator:
            parts = locator.split(".")
            if len(parts) != 2:
                raise ValueError(f"Invalid locator format: {locator}")
            if self.cfg.clickhouse.database:
                raise ValueError(
                    f"Locator '{locator}' must not include a database prefix when 'database' is set."  # noqa:E501
                )
            database, table = parts
        else:
            if not self.cfg.clickhouse.database:
                raise ValueError(
                    f"Locator '{locator}' must include a database prefix when 'database' is not set."  # noqa:E501
                )
            database, table = self.cfg.clickhouse.database, locator
        return f'"{database}"."{table}"'

    def _build_query_with_ranking(self, base_query: str) -> str:
        """Applies ranking logic to a base query.

        Args:
            base_query (str): SQL query to wrap.

        Returns:
            str: Query string with ranking logic.
        """
        located = self.locate()
        ranked_query, where_clause = self.ranking(located, base_query)
        return f"{ranked_query} {where_clause}".strip()

    async def _create_or_insert_df(self, table: str, df: pd.DataFrame):
        try:
            await self.c.register("chdf_chunk", df)
            await self.c.sql(f'INSERT INTO "{table}" SELECT * FROM chdf_chunk')
        except CatalogException:
            # Table doesn't exist yet — create from first DataFrame
            await self.c.sql(f'CREATE TABLE "{table}" AS SELECT * FROM chdf_chunk')
        except Exception as exc:
            log.debug(exc)
        finally:
            await self.c.unregister("chdf_chunk")

    async def _stream_to_duck(self, table: str, query: str, client, ch):
        result = client.stream_query(ch, query, block_size=self._blocksize)
        rows, columns = [], []
        for chunk in result:
            if not chunk:
                continue
            if not columns:
                columns = [col[0] for col in chunk]
                continue
            rows.append(chunk)
            # Operator ">=" not supported for types "int" and "str | None"
            while len(rows) >= self._blocksize:
                df = pd.DataFrame(rows[: self._blocksize], columns=columns)

                await self._create_or_insert_df(table, df)
                rows = rows[self._blocksize :]
                self.progress.log(len(df))
        if rows:
            df = pd.DataFrame(rows, columns=columns)
            await self._create_or_insert_df(table, df)
            self.progress.log(len(df))

    @with_clickhouse
    async def tap(self, query: t.Optional[str] = None, limit: int = 0):
        self.progress.reset("Streaming", show_memory=True)
        if query is None:
            raise ValueError("Query was not supplied")
        full_query = self._build_query_with_ranking(query)
        try:
            await self.c.sql("BEGIN TRANSACTION")
            # Cannot access attribute "client" for class "Clickhouse*"
            await self._stream_to_duck(self.name, full_query, self.client, self.ch)
            await self.c.sql("COMMIT")
        except Exception as exc:
            await self.c.sql("ROLLBACK")
            raise e.UnrecoverableTapError(f"Tap failed:\n{strip_trace(exc)}")

    @with_clickhouse
    async def _sink_direct(self, from_name: str, to_name: str):
        result = await self.c.sql(f'SELECT * FROM "{from_name}"')
        df = await result.df()
        stmt = f"INSERT INTO {to_name} VALUES"
        assert self.client is not None and self.ch is not None
        self.client.insert_dataframe(self.ch, stmt, df)

    @with_clickhouse
    async def _sink_streaming(self, from_name: str, to_name: str):
        """Streams data from DuckDB into Clickhouse in chunks.

        The table must exist and be ready to receive inserts.

        Args:
            table (str): Target Clickhouse table name.

        Raises:
            UnrecoverableSinkError: If any insert fails.
        """
        rel = await self.c.sql(f'SELECT * FROM "{from_name}"')
        reader = rel.raw().fetch_arrow_reader(batch_size=self._blocksize)
        assert self.client is not None and self.ch is not None
        total = 0
        current_batch = 0
        for batch in reader:
            try:
                current_batch = batch.num_rows
                columns = [col.to_pylist() for col in batch.columns]
                self.ch.execute(
                    f"INSERT INTO {to_name} VALUES",
                    params=columns,
                    columnar=True,
                )
            finally:
                self.progress.log(current_batch)
            total += current_batch

        if total == 0:
            self.log.debug(f"No rows to insert into {to_name}")
            return

        self.log.debug(f"Inserted {total} rows into {to_name}")

    async def sink(self, from_name: str):
        to_name = self.locate()
        try:
            if self._blocksize > 0:
                self.progress.reset("Streaming sink", show_memory=True)
                return await self._sink_streaming(from_name, to_name)
            self.progress.reset("Direct sink", show_memory=True)
            return await self._sink_direct(from_name, to_name)
        except Exception as exc:
            raise e.UnrecoverableSinkError(f"Sink failed:\n{strip_trace(exc)}")
        finally:
            self.progress.log(0)

    @with_clickhouse
    async def show_schema(self) -> m.Columns:
        try:
            return await self.schema_.show(self.name)
        except CatalogException:
            pass
        try:
            assert self.client is not None and self.ch is not None
            df = self.client.query_df(
                self.ch, f"SELECT * FROM {self.locate()} LIMIT 100"
            )
            await self.c.register("ch_preview", df)
            return await self.schema_.show("ch_preview")
        except Exception as exc:
            raise RuntimeError(f"Failed to infer schema for table\n{strip_trace(exc)}")
        finally:
            try:
                await self.c.unregister("ch_preview")
            except Exception as exc:
                log.error(exc)

    @with_clickhouse
    async def sql(self, statement: str):
        self.progress.reset("Execute")
        assert self.client is not None and self.ch is not None
        try:
            df = self.client.query_df(self.ch, statement)
            self.progress.log(len(df) or 1)
            if not df.empty:
                self.log.debug(print_df(df.head(5)))
        except CHError as chexc:
            raise e.ConnectionException(strip_trace(chexc))
