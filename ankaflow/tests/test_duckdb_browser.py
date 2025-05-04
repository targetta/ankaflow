# type: ignore
import sys
import types
import unittest
from urllib.parse import urlparse
from unittest.mock import AsyncMock, MagicMock, patch

from ..internal import browser as ddb
from ..common.path import RemotePath, PathFactory, HTTPPath
from ..models import ConnectionConfiguration, S3Config

# Create mock for pyodide.http.pyfetch
mock_pyodide_http = types.ModuleType("pyodide.http")
mock_pyodide_http.pyfetch = AsyncMock()

# Also mock the parent `pyodide` package (optional unless accessed directly)
mock_pyodide = types.ModuleType("pyodide")
mock_pyodide.http = mock_pyodide_http

# Register both in sys.modules
sys.modules["pyodide"] = mock_pyodide
sys.modules["pyodide.http"] = mock_pyodide_http


class MockRemotePath(RemotePath):
    def __init__(self, url: str):
        self._url = url
        self.path = url

    def __str__(self):
        return self._url

    def __repr__(self):
        return self._url

    def get_endpoint(self, region=None):
        return self._url

    def get_local(self, root: str) -> str:  # type: ignore
        return f"{root}/test.parquet"

    @property
    def is_glob(self) -> bool:
        return any(char in self.path for char in "*?[]")

    @property
    def anchor(self):
        parsed_url = urlparse(self._url)
        return f"{parsed_url.scheme}://{parsed_url.netloc}/"


class TestDDB(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.conn_config = ConnectionConfiguration(
            s3=S3Config(
                access_key_id="AKIA",
                secret_access_key="SECRET",
                region="us-east-1",
                bucket="test-bucket",
            )
        )

    async def asyncSetUp(self):
        self.ddb = ddb.DDB(self.conn_config)
        self.ddb.c = MagicMock()
        self.ddb.c.sql = MagicMock(return_value=MagicMock())
        self.ddb.fs = MagicMock()
        self.ddb.fs.root = "/tmp"
        self.ddb.fs.save_file = AsyncMock()
        self.ddb.remote = MagicMock()
        self.ddb.remote.fetch = AsyncMock(return_value=b"dummy-data")

    async def test_sql_rewrites_remote_path(self):
        with patch.object(
            PathFactory,
            "make",
            return_value=MockRemotePath("s3://bucket/file.parquet"),
        ):
            query = "SELECT * FROM read_parquet('s3://bucket/file.parquet')"
            result = await self.ddb.sql(query)
            self.assertIsInstance(result, ddb.Relation)

    async def test_check_for_unsupported_function(self):
        with self.assertRaises(NotImplementedError):
            self.ddb._check_for_unsupported("SELECT * FROM delta_scan()")

    async def test_rejects_remote_glob(self):
        glob_path = MockRemotePath("s3://bucket/*.parquet")
        with patch.object(PathFactory, "make", return_value=glob_path):
            rewriter = ddb.DuckDBIORewriter(self.ddb.remote, self.ddb.fs)
            with self.assertRaises(NotImplementedError):
                await rewriter.rewrite(
                    "SELECT * FROM read_parquet('s3://bucket/*.parquet')"
                )

    async def test_rejects_remote_glob_2(self):
        glob_path = MockRemotePath("s3://bucket/foo?.parquet")
        with patch.object(PathFactory, "make", return_value=glob_path):
            rewriter = ddb.DuckDBIORewriter(self.ddb.remote, self.ddb.fs)
            with self.assertRaises(NotImplementedError):
                await rewriter.rewrite(
                    "SELECT * FROM read_parquet('s3://bucket/foo?.parquet')"
                )

    async def test_rejects_remote_glob_3(self):
        glob_path = MockRemotePath("s3://bucket/foo*.parquet")
        with patch.object(PathFactory, "make", return_value=glob_path):
            rewriter = ddb.DuckDBIORewriter(self.ddb.remote, self.ddb.fs)
            with self.assertRaises(NotImplementedError):
                await rewriter.rewrite(
                    "SELECT * FROM read_parquet('s3://bucket/foo*.parquet')"
                )

    async def test_rejects_multiple_file_list(self):
        query = "SELECT * FROM read_parquet(['s3://b/a.parquet','s3://b/b.parquet'])"
        rewriter = ddb.DuckDBIORewriter(self.ddb.remote, self.ddb.fs)
        with self.assertRaises(NotImplementedError):
            await rewriter.rewrite(query)

    @patch(f"{__name__}.ddb.pyfetch", new_callable=AsyncMock)
    async def test_remoteobject_fetch_success(self, mock_pyfetch):
        mock_response = AsyncMock()
        mock_response.ok = True
        mock_response.status = 200
        mock_response.statusText = "OK"
        mock_response.bytes = AsyncMock(return_value=b"mocked-bytes")
        mock_pyfetch.return_value = mock_response

        remote = ddb.RemoteObject(secrets={}, fs=MagicMock())
        remote_path = MockRemotePath("https://example.com/file.parquet")
        data = await remote.fetch(remote_path)
        self.assertEqual(data, b"mocked-bytes")

    @patch(f"{__name__}.ddb.pyfetch", new_callable=AsyncMock)
    async def test_remoteobject_fetch_http_error(self, mock_pyfetch):
        mock_response = AsyncMock()
        mock_response.ok = False
        mock_response.status = 403
        mock_response.statusText = "Forbidden"
        mock_response.bytes = AsyncMock()
        mock_pyfetch.return_value = mock_response

        remote = ddb.RemoteObject(secrets={}, fs=MagicMock())
        remote_path = MockRemotePath("https://forbidden.com/file.parquet")

        with self.assertRaises(OSError) as context:
            await remote.fetch(remote_path)

        self.assertIn("Fetch failed: 403 Forbidden", str(context.exception))
        mock_pyfetch.assert_called_once()

    @patch(f"{__name__}.ddb.pyfetch", new_callable=AsyncMock)
    async def test_remoteobject_fetch_cors_like_error(self, mock_pyfetch):
        mock_pyfetch.side_effect = Exception("TypeError: Failed to fetch")

        remote = ddb.RemoteObject(secrets={}, fs=MagicMock())
        remote_path = HTTPPath("https://cors-fail.com/file.parquet")

        with self.assertRaises(OSError) as context:
            await remote.fetch(remote_path)

        self.assertIn("possible CORS or network issue", str(context.exception))
        mock_pyfetch.assert_called_once()
