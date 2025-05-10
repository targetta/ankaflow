# type: ignore
# TODO fix pylance warnings
import typing as t
from urllib.parse import urljoin
from io import StringIO, FileIO
import logging
import json
from pathlib import Path

from ... import models as m
from ...models import enums as enums
from ..connection import Schema
from ... import internal

log = logging.getLogger(__name__)


class MaterializeError(Exception):
    pass


class RestRequestError(Exception):
    pass


class RestRateLimitError(Exception):
    pass


class RestRetryableError(Exception):
    pass


class MaterializerProtocol(t.Protocol):
    async def materialize(self, data: t.Union[str, dict, list]) -> t.Any: ...


class Materializer:
    """
    Materializer class provides support for inserting
    external data (in-memory structures) or files
    into DuckDB table from multiple batches.
    """

    def __init__(
        self,
        connection: internal.DDB,
        dtype: enums.DataType,
        table: str,
        schema: Schema,
        columns: t.List[m.Column] = None,
        logger: logging.Logger = None,
    ):
        self.dtype = dtype
        self.connection = connection
        self.fields = columns
        self.schema = schema
        self.table = table
        self.log = logger or logging.getLogger(__name__)

    def cols_to_map(self) -> dict[str, str]:
        """
        Converts field list into format required by duckdb.read_x().

        Returns:
            dict[str, str]: {"col_name": "data_type"}
        """
        return {it.name: it.type for it in self.fields}

    async def create_table(self):
        """
        Create DuckDB table from supplied fields.
        """
        stmt = self.schema.generate(self.table, self.fields, exists_ok=True)
        try:
            await self.connection.sql(stmt)
        except Exception as e:
            self.log.error(f"Failed to create table: {e}")
            raise MaterializeError(f"Failed to create table: {e}")

    async def materialize(
        self, data: t.Union[list, str, dict], filename: str = None
    ):
        """
        Insert data into DuckDB table from supplied structure or file name.
        """
        if self.fields:
            await self.create_table()

        try:
            buffer = await self._prepare_buffer(data, filename)
            await self._insert_data(buffer)
        except Exception as e:
            raise MaterializeError(f"Materialization failed: {e}")
        finally:
            if filename:
                buffer.close()
                self._cleanup_file(filename)

    async def _prepare_buffer(self, data, filename):
        """
        Prepare a buffer (StringIO or FileIO) from the input data or file.
        """
        if data is not None:
            if isinstance(data, (list, dict)):
                string = json.dumps(data if isinstance(data, list) else [data])
            elif isinstance(data, str):
                string = data
            else:
                raise ValueError(
                    "Cannot infer type: can be JSON string, list, or dict"
                )  # noqa:E501
            return StringIO(string)
        elif filename:
            try:
                return FileIO(filename, "r")
            except Exception as e:
                self.log.error(f"Failed to read file {filename}: {e}")
                raise
        else:
            raise ValueError("Either data or filename must be provided")

    async def _insert_data(self, buffer: t.Union[StringIO, FileIO]):
        """
        Insert data from a buffer into the database.
        """
        read_opts = {"columns": self.cols_to_map()} if self.fields else {}
        try:
            if self.dtype in [enums.DataType.JSONL, enums.DataType.JSON]:
                await self.connection.read_json(buffer, self.table, read_opts)
            elif self.dtype == enums.DataType.CSV:
                await self.connection.read_csv(buffer, self.table, read_opts)
            elif self.dtype == enums.DataType.PARQUET:
                await self.connection.read_parquet(
                    buffer, self.table, read_opts
                )
            else:
                raise MaterializeError(f"Unsupported data type: {self.dtype}")
        except MaterializeError:
            raise
        except Exception as e:
            self.log.error(f"Failed to insert data: {e}")
            raise MaterializeError(f"Failed to insert data: {e}")
        finally:
            if isinstance(buffer, FileIO):
                buffer.close()

    def _cleanup_file(self, filename):
        """
        Clean up (delete) a file after processing.
        """
        try:
            Path(filename).unlink()
        except Exception as e:
            log.warning(f"Failed to delete file {filename}: {e}")


def get_url(base_url: str, path: str = "/"):
    if path.startswith("http"):
        return path
    try:
        return urljoin(base_url, path)
    except Exception:
        raise
