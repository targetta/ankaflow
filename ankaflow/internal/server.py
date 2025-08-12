import duckdb
import os
from pyarrow import Table
from io import FileIO, StringIO
import typing as t
import logging
import random

from ..models.configs import ConnectionConfiguration
from .macros import Fn
from . import errors as err

log = logging.getLogger(__name__)


class Relation:
    def __init__(self, relation: duckdb.DuckDBPyRelation):
        self.rel = relation

    def raw(self) -> duckdb.DuckDBPyRelation:
        return self.rel

    async def fetchone(self):
        return self.rel.fetchone()

    async def fetchall(self):
        return self.rel.fetchall()

    async def df(self):
        return self.rel.df()

    async def arrow(self) -> Table:
        return self.rel.arrow()


class DDB:
    def __init__(
        self,
        connection_options: ConnectionConfiguration,
        persisted: t.Optional[str] = None,
    ):
        self.dbname = persisted or ":memory:"
        self.conn_opts = connection_options
        self._c: duckdb.DuckDBPyConnection | None = None

    # TODO: Investigate this approach
    @property
    def c(self) -> duckdb.DuckDBPyConnection:
        if not self._c:
            self._c = duckdb.connect(self.dbname)
        return self._c

    def connection(self):
        return self.c

    async def _init_macros(self):
        """@private"""
        self.c.sql("CREATE SCHEMA IF NOT EXISTS Fn;")
        attr = [it for it in Fn.__dict__.keys() if not it.startswith("__")]
        macros = []

        for fn in attr:
            f = getattr(Fn, fn)
            macro = f"""CREATE OR REPLACE MACRO Fn.{fn}{f};"""
            macros.append(macro)
        stmt = " ".join(macros)
        self.c.sql(stmt)

    async def _init_settings(self):
        """@private"""
        ext_dir = os.getenv("DUCKDB_EXTENSION_DIR")
        if ext_dir:
            self.c.execute("SET autoinstall_known_extensions=false;")
            self.c.execute("SET autoload_known_extensions=false;")
            self.c.execute(f"SET extension_directory='{ext_dir}';")
            self.c.execute("LOAD 'httpfs';")
            self.c.execute("LOAD 'aws';")
            self.c.execute("LOAD 'delta';")
        else:
            self.c.execute("SET autoinstall_known_extensions=true;")
            self.c.execute("SET autoload_known_extensions=true;")

        # Remove the default s3 access
        # They will facilitate overriding secret scopes
        self.c.execute("SET s3_access_key_id=null;")
        self.c.execute("SET s3_secret_access_key=null;")
        if os.getenv("DUCKDB_DISABLE_LOCALFS"):
            self.c.execute("SET disabled_filesystems = 'LocalFileSystem';")
        if os.getenv("DUCKDB_LOCK_CONFIG"):
            self.c.execute("SET lock_configuration = true;")

    async def _attach_motherduck(self):
        if "motherduck_token" in os.environ:
            try:
                self.c.execute("ATTACH 'md:'")
                log.info("Motherduck attached")
            except Exception as e:
                log.error(f"Failed loading Motherduck: {e}")

    async def _init_secrets(self):
        # TODO: use bucket property from global connection
        # Instead of prefixes
        if not self.conn_opts:
            return
        if self.conn_opts.gs.hmac_key:
            gs_ep = f"storage.{self.conn_opts.gs.region}.rep.googleapis.com"
            self.c.execute(
                f"""
                    CREATE SECRET IF NOT EXISTS __gs__ (
                    TYPE GCS,
                    KEY_ID '{self.conn_opts.gs.hmac_key}',
                    SECRET '{self.conn_opts.gs.hmac_secret}',
                    REGION '{self.conn_opts.gs.region}',
                    ENDPOINT '{gs_ep}',
                    SCOPE '{self.conn_opts.gs.bucket}'
                    );
                    """
            )
        if self.conn_opts.s3.access_key_id:
            s3_ep = f"s3.{self.conn_opts.s3.region}.amazonaws.com"
            self.c.execute(
                f"""
                CREATE SECRET IF NOT EXISTS __s3__ (
                TYPE S3,
                KEY_ID '{self.conn_opts.s3.access_key_id}',
                SECRET '{self.conn_opts.s3.secret_access_key}',
                REGION '{self.conn_opts.s3.region}',
                ENDPOINT '{s3_ep}',
                SESSION_TOKEN '',
                SCOPE '{self.conn_opts.s3.bucket}'
                );
                """
            )

    async def inject_secrets(
        self, name: str, connection_options: ConnectionConfiguration
    ):
        # TODO: use bucket property from global connection
        # Instead of prefixes
        if not connection_options:
            return
        secret_name = name or f"secret_{random.randint(1, 10000)}"
        if connection_options.gs.bucket:
            gs_ep = f"storage.{connection_options.gs.region or self.conn_opts.gs.region}.rep.googleapis.com"  # noqa: E501
            self.c.execute(
                f"""
                    CREATE SECRET IF NOT EXISTS "{secret_name}" (
                    TYPE GCS,
                    KEY_ID '{connection_options.gs.hmac_key or self.conn_opts.gs.hmac_key}',
                    SECRET '{connection_options.gs.hmac_secret or self.conn_opts.gs.hmac_secret}',
                    REGION '{connection_options.gs.region or self.conn_opts.gs.region}',
                    ENDPOINT '{gs_ep}',
                    SCOPE '{connection_options.gs.bucket}'
                    );
                    """  # noqa: E501
            )
        if connection_options.s3.bucket:
            s3_ep = f"s3.{connection_options.s3.region}.amazonaws.com"
            self.c.execute(
                f"""
                CREATE SECRET IF NOT EXISTS "{secret_name}" (
                TYPE S3,
                KEY_ID '{connection_options.s3.access_key_id or self.conn_opts.s3.access_key_id}',
                SECRET '{connection_options.s3.secret_access_key or self.conn_opts.s3.secret_access_key}',
                REGION '{connection_options.s3.region or self.conn_opts.s3.region}',
                ENDPOINT '{s3_ep}',
                SESSION_TOKEN '',
                SCOPE '{connection_options.s3.bucket}'
                );
                """  # noqa: E501
            )

    async def connect(self):
        try:
            await self._init_settings()
        except Exception as e:
            log.error(e)
        await self._init_macros()
        await self._init_secrets()
        await self._attach_motherduck()
        return self

    async def disconnect(self):
        if self.dbname != ":memory:":
            self.c.close()

    async def sql(self, query: str) -> Relation:
        rel = self.c.sql(query)
        return Relation(rel)

    async def register(self, name: str, object: t.Any):
        self.c.register(name, object)

    async def unregister(self, name: str):
        self.c.unregister(name)

    async def read_json(
        self,
        data: t.Union[FileIO, StringIO],
        table: str,
        read_opts: t.Dict,
        create_when_needed: bool = True,
    ):
        tbl = self.c.read_json(data, **read_opts)  # noqa:F841 #type: ignore
        try:
            self.c.sql(f"INSERT INTO {table} FROM tbl;")
            return tbl
        except duckdb.CatalogException as e:
            if create_when_needed:
                self.c.sql(f"CREATE TABLE {table} AS FROM tbl;")
            else:
                raise err.CatalogException(e)

    async def read_csv(
        self,
        data: t.Union[FileIO, StringIO],
        table: str,
        read_opts: t.Dict,
        create_when_needed: bool = True,
    ):
        tbl = self.c.read_csv(data, **read_opts)  # noqa:F841 #type: ignore
        try:
            self.c.sql(f"INSERT INTO {table} FROM tbl;")
            return tbl
        except duckdb.CatalogException as e:
            if create_when_needed:
                self.c.sql(f"CREATE TABLE {table} AS FROM tbl;")
                return tbl
            else:
                raise err.CatalogException(e)

    async def read_parquet(
        self,
        data: t.Union[FileIO, StringIO],
        table: str,
        read_opts: t.Dict,
        create_when_needed: bool = True,
    ):
        tbl = self.c.read_parquet(data, **read_opts)  # noqa:F841 #type: ignore
        try:
            self.c.sql(f"INSERT INTO {table} FROM tbl;")
            return tbl
        except duckdb.CatalogException as e:
            if create_when_needed:
                self.c.sql(f"CREATE TABLE {table} AS FROM tbl;")
            else:
                raise err.CatalogException(e)

    async def delta_scan(self): ...

    async def parquet_scan(self): ...
