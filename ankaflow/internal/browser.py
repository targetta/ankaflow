import duckdb
import logging
import re
import ast
import typing as t
from pandas import DataFrame

if t.TYPE_CHECKING:
    from pyodide.http import pyfetch  # type: ignore
else:
    try:
        from pyodide.http import pyfetch
    except ImportError:
        raise RuntimeError("Emscripten environment required")

from .macros import Fn
from ..common.filesystem import FileSystem
from ..models.configs import BucketConfig, ConnectionConfiguration
from ..common.path import PathFactory, RemotePath


log = logging.getLogger(__name__)


class DuckDBIORewriter:
    def __init__(self, remote: "RemoteObject", filesystem: FileSystem):
        """
        Intercepts and rewrites DuckDB read() calls that reference remote paths.

        Args:
            scope (str): scope of the current rewrite (stage name)
            remote (RemoteObject): Remote downloader/uploader.
            filesystem (FileSystem): Local filesystem to store fetched content.
        """
        self.remote = remote
        self.fs: FileSystem = filesystem

    async def rewrite(self, query: str) -> str:
        """
        Rewrites read_*() calls with remote URIs to local paths.

        Args:
            query (str): The SQL query string.

        Returns:
            str: The rewritten query.

        Raises:
            NotImplementedError: For globs or multi-file remote reads.
        """
        pattern = (
            r"read_(parquet|csv|json)(_auto)?\s*\(\s*(\[.*?\]|'.*?'|\".*?\")"  # noqa:E501
        )
        matches = re.findall(pattern, query)

        for _, _, arg_str in matches:
            paths = self._parse_paths(arg_str)

            for path_str in paths:
                path = PathFactory.make(path_str)

                if isinstance(path, RemotePath):
                    if path.is_glob:
                        raise NotImplementedError(
                            "Remote globs are not supported."
                        )
                    if len(paths) > 1:
                        raise NotImplementedError(
                            "Multiple remote files are not supported."
                        )

                    local_path = await self._rewrite_remote_to_local(path)
                    query = query.replace(f"'{path_str}'", f"'{local_path}'")

        return query

    def _parse_paths(self, arg_str: str) -> list[str]:
        arg_str = arg_str.strip()
        if arg_str.startswith("["):
            try:
                parsed = ast.literal_eval(arg_str)
                if not isinstance(parsed, list) or not all(
                    isinstance(x, str) for x in parsed
                ):
                    raise ValueError
                return parsed
            except Exception:
                raise ValueError(f"Cannot parse list of files: {arg_str}")
        elif arg_str.startswith(("'", '"')):
            return [arg_str.strip("'\"")]
        else:
            raise ValueError(f"Unrecognized input: {arg_str}")

    async def _rewrite_remote_to_local(self, path: RemotePath) -> str:
        content = await self.remote.fetch(path)
        local_path = path.get_local(self.fs.root_path.as_posix())
        await self.fs.save_file(local_path, content)
        return local_path


class Relation:
    def __init__(self, relation: duckdb.DuckDBPyRelation):
        """
        Wraps a DuckDBPyRelation for async compatibility.

        Args:
            relation (duckdb.DuckDBPyRelation): DuckDB relation object.
        """
        self.rel = relation

    async def fetchone(self):
        return self.rel.fetchone()

    async def fetchall(self):
        return self.rel.fetchall()

    async def df(self) -> DataFrame:
        """Returns the relation as a pandas DataFrame."""
        return self.rel.df()

    async def arrow(self) -> DataFrame:
        """Returns the relation as Arrow table."""
        return self.rel.arrow()


class RemoteObject:
    def __init__(self, secrets: dict, fs: FileSystem):
        """
        Initializes a RemoteObject handler.

        Args:
            secrets (dict): Credentials or connection metadata.
            fs (FileSystem): File system handler to save fetched content.
        """
        self.secrets: t.Dict[str, BucketConfig] = secrets
        self.fs = fs

    async def fetch(self, remote_path: RemotePath) -> bytes:
        """
        Downloads the content at the remote path.

        Args:
            remote_path (RemotePath): The remote object URI.

        Returns:
            bytes: Content fetched from the remote URL.

        Raises:
            IOError: If fetch fails due to network or CORS.
        """
        # secrets dict stores anchor for each bucket.
        secret = self.secrets.get(remote_path.anchor, BucketConfig())
        url = remote_path.get_endpoint(secret.region)
        log.debug(url)
        try:
            response = await pyfetch(url)
            if not response.ok:
                raise IOError(
                    f"Fetch failed: {response.status} {response.statusText} for {remote_path}"  # noqa:E501 # type: ignore
                )
            return await response.bytes()
        except Exception as e:
            raise IOError(
                f"Failed fetch - possible CORS or network issue: {remote_path} => {e}"  # noqa:E501
            )

    async def upload(self, remote_path: RemotePath, local_file: str):
        """
        Uploads a local file to a remote path (stub).

        Args:
            remote_path (RemotePath): Destination remote URI.
            local_file (str): Path to the local file.

        Raises:
            NotImplementedError: Always.
        """
        log.warning(f"Upload stub: {local_file} -> {remote_path}")
        raise NotImplementedError("Upload not implemented")


class DDB:
    def __init__(
        self,
        connection_options: ConnectionConfiguration,
        persisted: t.Optional[str] = None,
    ):
        """
        Initializes the DuckDB browser-bound client.

        Args:
            connection_options (ConnectionConfiguration): Connection settings.
        """
        self.conn_opts = connection_options
        self.secrets: dict[str, BucketConfig] = {}
        self.fs = FileSystem("/tmp")
        self.remote = RemoteObject(self.secrets, self.fs)
        self._c: t.Optional[duckdb.DuckDBPyConnection] = None
        self.dbname = persisted or ":memory:"
        self.unsupported_functions = ["delta_scan", "postgres_scan"]
        self._init_secrets(connection_options)

    @property
    def c(self) -> duckdb.DuckDBPyConnection:
        if self._c is None:
            raise RuntimeError("No active request")
        return self._c

    @c.setter
    def c(self, value: t.Union[duckdb.DuckDBPyConnection, None]):
        self._c = value

    async def connect(self) -> "DDB":
        """Establishes a DuckDB connection and initializes context."""
        self.c = duckdb.connect(self.dbname)
        await self._init_settings()
        await self._init_functions()
        return self

    async def disconnect(self):
        if self.dbname != ":memory:":
            self.c.close()

    async def _init_settings(self):
        """Initializes DuckDB settings for browser environment."""
        self.c.execute("SET autoinstall_known_extensions=true;")
        self.c.execute("SET autoload_known_extensions=true;")
        # Anything related to s3 triggers httpfs extension
        # self.c.execute("SET s3_access_key_id=null;")
        # self.c.execute("SET s3_secret_access_key=null;")
        # self.c.execute("SET lock_configuration = true;")

    async def _init_functions(self):
        """@private"""
        """Reserved for user-defined UDFs or function injection."""
        self.c.sql("CREATE SCHEMA IF NOT EXISTS Fn;")
        attr = [it for it in Fn.__dict__.keys() if not it.startswith("__")]
        macros = []

        for fn in attr:
            f = getattr(Fn, fn)
            macro = f"""CREATE OR REPLACE MACRO Fn.{fn}{f};"""
            macros.append(macro)
        stmt = " ".join(macros)
        self.c.sql(stmt)

    def _init_secrets(self, config: ConnectionConfiguration | None = None):
        """Extracts credentials from connection options."""
        if not config:
            return
        for field_name in config.model_fields:
            cfg = getattr(config, field_name)
            # only consider BucketConfig (or subclasses) with a bucket set
            if isinstance(cfg, BucketConfig) and cfg.bucket:
                self.secrets[cfg.bucket] = cfg
        # self.remote = RemoteObject(self.secrets, self.fs)

    async def get(self) -> duckdb.DuckDBPyConnection:
        """Returns the internal DuckDB connection object."""
        return self.c

    async def inject_secrets(
        self, name: str, connection_options: ConnectionConfiguration
    ):
        """
        Injects secret configuration dynamically.

        Args:
            name (str): Secret identifier.
            connection_options (ConnectionConfiguration): Configuration details.
        """
        self._init_secrets(connection_options)

    async def sql(self, query: str) -> Relation:
        """
        Executes a DuckDB SQL query after intercepting remote I/O references.

        Args:
            query (str): SQL string.

        Returns:
            Relation: Resulting relation object.
        """
        self._check_for_unsupported(query)
        # query = await self._process_io_calls(query)
        rewriter = DuckDBIORewriter(self.remote, self.fs)
        query = await rewriter.rewrite(query)
        return Relation(self.c.sql(query))

    def _check_for_unsupported(self, query: str):
        """
        Checks whether the query contains unsupported function calls.

        Args:
            query (str): SQL string.

        Raises:
            NotImplementedError: If query uses browser-incompatible functions.
        """
        pattern = r"\b({})\s*\(".format(
            "|".join(map(re.escape, self.unsupported_functions))
        )
        matches = re.findall(pattern, query, flags=re.IGNORECASE)
        if matches:
            raise NotImplementedError(
                f"The following function(s) are not supported in the browser environment: {', '.join(set(matches))}"  # noqa:E501
            )

    async def register(self, view_name: str, object):
        """
        Registers a Python object as a DuckDB view.

        Args:
            view_name (str): View name.
            object: Python object to register.
        """
        self.c.register(view_name, object)

    async def unregister(self, view_name: str):
        """
        Unregisters a DuckDB view.

        Args:
            view_name (str): View name to remove.
        """
        self.c.unregister(view_name)

    async def read_json(
        self, data: str, table: str, read_opts: dict
    ) -> duckdb.DuckDBPyRelation:
        """
        Reads JSON data into a DuckDB table.

        Args:
            data (str): File path or URL.
            table (str): Target table name.
            read_opts (dict): Options passed to DuckDB read_json().

        Returns:
            duckdb.DuckDBPyRelation: The resulting table.
        """
        tbl = self.c.read_json(data, **read_opts)
        try:
            self.c.sql(f"INSERT INTO {table} FROM tbl;")
        except duckdb.CatalogException:
            self.c.sql(f"CREATE TABLE {table} AS FROM tbl;")
        return tbl

    async def read_parquet(
        self, data: str, table: str, read_opts: dict
    ) -> duckdb.DuckDBPyRelation:
        """
        Reads Parquet data into a DuckDB table.

        Args:
            data (str): File path or URL.
            table (str): Target table name.
            read_opts (dict): Options passed to DuckDB read_parquet().

        Returns:
            duckdb.DuckDBPyRelation: The resulting table.
        """
        tbl = self.c.read_parquet(data, **read_opts)
        try:
            self.c.sql(f"INSERT INTO {table} FROM tbl;")
        except duckdb.CatalogException:
            self.c.sql(f"CREATE TABLE {table} AS FROM tbl;")
        return tbl

    async def read_csv(
        self, data: str, table: str, read_opts: dict
    ) -> duckdb.DuckDBPyRelation:
        """
        Reads CSV data into a DuckDB table.

        Args:
            data (str): File path or URL.
            table (str): Target table name.
            read_opts (dict): Options passed to DuckDB read_csv().

        Returns:
            duckdb.DuckDBPyRelation: The resulting table.
        """
        tbl = self.c.read_csv(data, **read_opts)
        try:
            self.c.sql(f"INSERT INTO {table} FROM tbl;")
        except duckdb.CatalogException:
            self.c.sql(f"CREATE TABLE {table} AS FROM tbl;")
        return tbl

    async def delta_scan(self):
        """Stub for unsupported delta_scan."""
        raise NotImplementedError(
            "delta_scan is not supported in browser environment"
        )

    async def parquet_scan(self):
        """Stub for unsupported parquet_scan."""
        raise NotImplementedError("parquet_scan is not yet implemented")
