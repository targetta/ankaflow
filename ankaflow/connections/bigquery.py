import logging
import typing as t
from enum import Enum
from google.cloud.bigquery import Client, SchemaUpdateOption, Dataset
from google.cloud.bigquery.job.query import QueryJobConfig
from google.cloud.bigquery.job.load import LoadJobConfig
from google.cloud.bigquery.job import CreateDisposition, WriteDisposition
import google.api_core.exceptions as gex
from sqlglot import parse_one
from sqlglot.dialects.dialect import Dialects
import warnings
from pandas import DataFrame

from .connection import Connection
from . import errors as e
from .. import models as m
from ..models.connections import BigQueryConnection
from ..internal import CatalogException
from ..common.util import print_df

log = logging.getLogger(__name__)


class SinkStrategy(str, Enum):
    SKIP = "skip"
    CREATE = "create"
    WRITE = "write"


class BigQuery(Connection):
    dialect = Dialects.BIGQUERY

    def init(self):
        """Initializes internal configs and client."""
        self.conn = t.cast(BigQueryConnection, self.conn)
        self.queryconfig = QueryJobConfig()
        self.loadconfig = LoadJobConfig()
        self._client = None
        self._setup_configs()

    def _setup_configs(self):
        """Initializes BigQuery job configuration."""
        schema_mode = getattr(self.conn, "schema_mode", None) or "None"
        data_mode = getattr(self.conn, "data_mode", None) or "None"

        write_disposition = (
            WriteDisposition.WRITE_APPEND
            if data_mode == "append"
            else WriteDisposition.WRITE_TRUNCATE
            if data_mode == "overwrite"
            else WriteDisposition.WRITE_EMPTY
        )

        schema_update = (
            [SchemaUpdateOption.ALLOW_FIELD_ADDITION]
            if schema_mode == "merge"
            else []
        )

        self.loadconfig.write_disposition = write_disposition
        if schema_update:
            self.loadconfig.schema_update_options = schema_update

        # Set default dataset if both are present
        if self.cfg.bigquery.project and self.cfg.bigquery.dataset:
            self.queryconfig.default_dataset = (
                f"{self.cfg.bigquery.project}.{self.cfg.bigquery.dataset}"
            )

    def _get_client(self) -> Client:
        """Returns a cached BigQuery client or initializes one."""
        if self._client:
            return self._client

        location = self.cfg.bigquery.region
        if not location:
            log.warning(
                "No dataset_region set in config; default will be used (usually US)"  # noqa:E501
            )
            self.log.warning(
                "No dataset_region set in config; default will be used (usually US)"  # noqa:E501
            )

        if self.cfg.bigquery.credential_file:
            self._client = Client.from_service_account_json(
                self.cfg.bigquery.credential_file,
                location=self.cfg.bigquery.region,
            )
        else:
            # Uses application default credentials
            self._client = Client(location=self.cfg.bigquery.region)

        return self._client

    def _build_load_job_config(self) -> LoadJobConfig:
        """Builds LoadJobConfig based on schema and data mode."""
        schema_mode = self.conn.schema_mode or "None"
        data_mode = self.conn.data_mode or "None"

        write_disposition = (
            WriteDisposition.WRITE_APPEND
            if data_mode == "append"
            else WriteDisposition.WRITE_TRUNCATE
            if data_mode == "overwrite"
            else WriteDisposition.WRITE_EMPTY
        )

        schema_update = (
            [SchemaUpdateOption.ALLOW_FIELD_ADDITION]
            if schema_mode == "merge"
            else []
        )

        config = LoadJobConfig(write_disposition=write_disposition)
        if schema_update:
            config.schema_update_options = schema_update

        return config

    def _get_disposition_modes(
        self, schema_mode: str, data_mode: str
    ) -> t.Tuple[t.List[SchemaUpdateOption], WriteDisposition]:
        """Returns tuple of (schema_update_options, write_disposition)."""

        write_disposition = (
            WriteDisposition.WRITE_APPEND
            if data_mode == "append"
            else WriteDisposition.WRITE_TRUNCATE
            if data_mode == "overwrite"
            else WriteDisposition.WRITE_EMPTY
        )

        schema_update = (
            [SchemaUpdateOption.ALLOW_FIELD_ADDITION]
            if schema_mode == "merge"
            else []
        )

        return schema_update, write_disposition  # type: ignore  # Pylance freaks our becasue if str != literal

    def locate(
        self, name: t.Optional[str] = None, use_wildcard: bool = False
    ) -> str:
        """Returns the fully qualified BigQuery table name using dot notation."""
        name = name or self.conn.locator

        # Normalize identifiers (remove quotes/backticks)
        name = self._normalize_bq_identifier(name)
        dataset = (
            self._normalize_bq_identifier(self.cfg.bigquery.dataset)
            if self.cfg.bigquery.dataset
            else None
        )

        if dataset:
            return f"{dataset}.{name}"
        return name

    def _normalize_bq_identifier(self, identifier: str) -> str:
        """Normalizes BigQuery identifiers by removing quotes or backticks."""
        return identifier.replace('"', "").replace("`", "")

    def _build_query_with_ranking(self, base_query: str) -> str:
        """Injects row ranking into the query if versioning is configured."""
        located = self.locate()
        ranked_query, where_clause = self.ranking(located, base_query)
        return f"{ranked_query} {where_clause}".strip()

    def _execute_query_to_dataframe(self, query: str) -> DataFrame:
        """Submits query to BigQuery and returns DataFrame."""
        log.debug(f"Query sent:\n{query}")
        client = self._get_client()
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")  # Suppress Arrow type warnings
                return (
                    client.query(query, job_config=self.queryconfig)
                    .result()
                    .to_dataframe()
                )
        except Exception as ex:
            raise Exception((f"BigQuery query failed:\n{query}\nError: {ex}"))

    async def _register_dataframe_to_duckdb(
        self, df: DataFrame, temp_name: str
    ):
        """Registers a DataFrame as a DuckDB temp table."""
        try:
            await self.c.register(temp_name, df)
        except Exception as e:
            raise Exception(
                f"Failed to register DataFrame to DuckDB as '{temp_name}': {e}"
            )

    async def tap(self, query: t.Optional[str] = None, limit: int = 0):
        """Executes a BigQuery query and loads result into DuckDB."""
        if not query:
            raise ValueError(
                "BigQuery requires an explicit query — none was provided."
            )

        # TODO: may cause issues when running paraller
        temp_name = "bigdf"
        try:
            ranked_query = self._build_query_with_ranking(query)
            df = self._execute_query_to_dataframe(ranked_query)

            await self._register_dataframe_to_duckdb(df, temp_name)

            return await self.c.sql(f"""
                CREATE TABLE "{self.name}" AS
                SELECT * FROM "{temp_name}"
            """)
        except Exception as ex:
            raise Exception(f"tap failed: {ex}")
        finally:
            if self._client:
                self._client.close()
            try:
                await self.c.unregister(temp_name)
            except Exception as cleanup_ex:
                log.warning(
                    f"Failed to unregister temp table '{temp_name}': {cleanup_ex}"
                )

    async def sink(self, from_name: str):
        """Public entrypoint for writing DuckDB data to BigQuery."""
        config = self._build_load_config_from_modes()
        try:
            return await self._sink_impl(from_name, config=config)
        except Exception as ex:
            raise Exception(ex)
        finally:
            if self._client:
                self._client.close()

    async def _sink_impl(self, from_name: str, config: LoadJobConfig):
        """Handles actual load to BigQuery with retry logic and custom config."""
        client = self._get_client()

        try:
            # Step 1: Read data from DuckDB
            rel = await self.c.sql(f'SELECT * FROM "{from_name}"')
            df = await rel.df()

            # TODO: infer types from duckdb and create proper table
            if df.empty:
                self.log.info("No rows to sink — skipping write")
                return
            # Step 2: Get target BigQuery table name
            target: str = self.locate()

            # Step 3: Submit load job
            job = client.load_table_from_dataframe(
                df,
                target,  # already normalized
                job_config=config,
            )

            job.result()  # Synchronous: wait until complete
            self.log.debug(f"{len(df)} records sent.")
        except gex.NotFound:
            self.create_dataset()
            await self._sink_impl(from_name, config=config)
        except gex.Conflict:
            if self.conn.data_mode == "error":
                raise e.DataModeConflict(
                    "Table exists but data_mode is 'error'. Cannot continue."
                )

            if not config.schema_update_options:
                raise e.SchemaModeConflict(
                    "Schema mismatch but schema_mode is not set."
                )

            raise

        except Exception as exc:
            raise Exception(f"Sink failed: {exc}")

        finally:
            if self._client:
                self._client.close()

    def _build_load_config_from_modes(
        self,
        data_mode: t.Optional[str] = None,
        schema_mode: t.Optional[str] = None,
        create_disposition: str = CreateDisposition.CREATE_IF_NEEDED,
    ) -> LoadJobConfig:
        """Generates LoadJobConfig based on data/schema
        modes and optional overrides."""
        data_mode = data_mode or self.conn.data_mode
        schema_mode = schema_mode or self.conn.schema_mode

        # Map to BigQuery WriteDisposition
        write_disposition = (
            WriteDisposition.WRITE_APPEND
            if data_mode == "append"
            else WriteDisposition.WRITE_TRUNCATE
            if data_mode == "overwrite"
            else WriteDisposition.WRITE_EMPTY  # "error" fallback
        )

        # Map to BigQuery SchemaUpdateOption
        if schema_mode == "merge":
            schema_update_options = [SchemaUpdateOption.ALLOW_FIELD_ADDITION]
        # elif schema_mode == "overwrite":
        #     schema_update_options = [
        #         SchemaUpdateOption.ALLOW_FIELD_ADDITION,
        #             SchemaUpdateOption.ALLOW_FIELD_RELAXATION
        #         ]
        else:
            schema_update_options = []

        return LoadJobConfig(
            create_disposition=create_disposition,
            write_disposition=write_disposition,
            schema_update_options=schema_update_options or None,
        )

    def create_dataset(self):
        """Creates dataset if it does not exist."""
        client = self._get_client()
        dataset_id = f"{self.cfg.bigquery.project}.{self.cfg.bigquery.dataset}"
        location = self.cfg.bigquery.region

        try:
            dataset = Dataset(dataset_id)
            if location:
                dataset.location = location

            client.create_dataset(dataset)
            log.info(
                f"Created dataset `{dataset_id}` in `{location or 'default'}` region."  # noqa:E501
            )
        except gex.Conflict:
            log.info(f" Dataset `{dataset_id}` already exists.")
        except Exception as e:
            raise Exception(f"Failed to create dataset `{dataset_id}`: {e}")

    async def show_schema(self) -> m.Columns:
        """Infers runtime schema from BigQuery data (via DuckDB) and returns as Fields."""  # noqa:E501
        try:
            return await self.schema_.show(self.name)
        except CatalogException:
            pass

        # go to source
        tmp_name = f"schema_df_{self.name}"

        try:
            # Step 1: Fetch 1 row from BigQuery
            query = parse_one(f"SELECT * FROM {self.locate()} LIMIT 1").sql(
                dialect=self.dialect
            )
            df = self._execute_query_to_dataframe(query)

            # Step 2: Register in DuckDB
            await self.c.register(tmp_name, df)

            # Step 3: Return inferred schema as m.Fields
            return await self.schema_.show(tmp_name)

        except Exception as err:
            self.log.exception(f"Failed to infer schema: {err}")
            return self.schema_.error(err)

        finally:
            # Step 4: Always unregister temp table
            await self.c.unregister(tmp_name)

    async def sql(self, statement: str) -> t.Any:
        """Executes a BigQuery SQL statement and logs the result."""
        try:
            df: DataFrame = self._execute_query_to_dataframe(statement)
            self.log.info(f"SQL result:\n{print_df(df, all_rows=False)}")
        except Exception as err:
            raise Exception(f"SQL execution failed: {err}")
        finally:
            if self._client:
                self._client.close()

    def _resolve_exception(
        self, exc: Exception, config: LoadJobConfig, conn
    ) -> Exception:
        msg = str(exc)

        if isinstance(exc, gex.Conflict):
            if config.create_disposition != CreateDisposition.CREATE_IF_NEEDED:
                return e.DataModeConflict(
                    f"Data mode '{self.conn.data_mode}' maps to '{config.write_disposition}', "  # noqa:E501
                    f"but table already exists. Retry with data_mode: 'append'.\n{msg}"  # noqa:E501
                )

            if config.write_disposition == WriteDisposition.WRITE_EMPTY:
                return e.DataModeConflict(
                    f"Cannot write to existing table with data_mode: 'error'.\n{msg}"
                )

            if not config.schema_update_options:
                return e.SchemaModeConflict(
                    "Insert includes columns not present in table, but schema_mode is not 'merge'."  # noqa:E501
                )

            return e.UnrecoverableSinkError(
                f"BigQuery conflict not resolved: {msg}"
            )

        return e.UnrecoverableSinkError(f"Unhandled BigQuery sink error: {msg}")
