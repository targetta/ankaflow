import unittest
from unittest.mock import patch, MagicMock, AsyncMock
import pandas as pd
from clickhouse_driver.errors import Error as CHError

# Makse sure to keep imports
from ..connections import clickhouse as ch
from ..connections import errors as e
from ..models.connections import ClickhouseConnection
from ..models.configs import ConnectionConfiguration, ClickhouseConfig
from ..models.core import (
    FlowContext,
    Variables,
)
from ..internal.duckdb import DDB


class TestClickhouseClient(unittest.TestCase):
    def setUp(self):
        self.cfg = ConnectionConfiguration(
            clickhouse=ClickhouseConfig(
                host="localhost",
                port=9000,
                username="user",
                password="pass",
                database="test_db",
                blocksize=1000,
            )
        )
        self.client = ch.ClickhouseClient(self.cfg)

    @patch(f"{__name__}.ch.Client")
    def test_connect_context_manager(self, MockClient):
        mock_instance = MockClient.return_value
        with self.client.connect() as conn:
            self.assertEqual(conn, mock_instance)
        mock_instance.disconnect.assert_called_once()

    def test_strip_trace_cherror(self):
        exc = CHError("Boom! Stack trace:\n...")
        self.assertEqual(ch.strip_trace(exc), "Code: None. Boom!")

    def test_strip_trace_other_error(self):
        exc = ValueError("Blah")
        self.assertEqual(ch.strip_trace(exc), "Blah")


class TestClickhouse(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.cfg = ConnectionConfiguration(
            clickhouse=ClickhouseConfig(
                host="localhost",
                port=9000,
                username="user",
                password="pass",
                database="testdb",
                blocksize=2,
            )
        )
        self.connection = MagicMock(spec=ClickhouseConnection)
        self.connection.locator = "my_table"
        self.connection.fields = []
        self.connection.config = self.cfg
        self.connection.version = None
        self.connection.key = None
        self.context = MagicMock(spec=FlowContext)
        self.variables = MagicMock(spec=Variables)
        self.duck = MagicMock(spec=DDB)

        self.ck = ch.Clickhouse(
            duck=self.duck,
            name="duck_table",
            connection=self.connection,
            context=self.context,
            variables=self.variables,
        )
        self.ck.cfg = self.cfg
        self.ck.init()

    def test_locate_valid_with_db(self):
        self.connection.locator = "my_table"
        self.cfg.clickhouse.database = "testdb"
        result = self.ck.locate()
        self.assertEqual(result, '"testdb"."my_table"')

    def test_locate_invalid_format(self):
        self.connection.locator = "a.b.c"
        with self.assertRaises(ValueError):
            self.ck.locate()

    def test_locate_conflict_with_database_prefix(self):
        self.cfg.clickhouse.database = "some_db"
        self.connection.locator = "other_db.table"
        with self.assertRaises(ValueError):
            self.ck.locate()

    def test_build_query_with_ranking(self):
        self.ck.ranking = MagicMock(return_value=("SELECT * FROM x", "WHERE y"))
        self.ck.locate = MagicMock(return_value="table")
        result = self.ck._build_query_with_ranking("SELECT * FROM x")
        self.assertEqual(result, "SELECT * FROM x WHERE y")

    @patch(f"{__name__}.ch.ClickhouseClient.stream_query")
    @patch(
        f"{__name__}.ch.Clickhouse._create_or_insert_df",
        new_callable=AsyncMock,
    )
    async def test_stream_to_duck(self, mock_insert_df, mock_stream_query):
        rows = [[("col1", "Int32")], [1], [2]]
        mock_stream_query.return_value = iter(rows)
        self.ck.progress = MagicMock()
        await self.ck._stream_to_duck(
            "duck_table", "QUERY", self.ck._client, MagicMock()
        )
        self.assertGreaterEqual(mock_insert_df.await_count, 1)

    @patch(
        f"{__name__}.ch.Clickhouse._stream_to_duck",
        new_callable=AsyncMock,
    )
    @patch(
        f"{__name__}.ch.Clickhouse._build_query_with_ranking",
        return_value="SELECT * FROM bar",
    )
    async def test_tap_success(self, mock_build, mock_stream):
        self.ck.c.sql = AsyncMock()
        self.ck.progress = MagicMock()
        await self.ck.tap(query="SELECT * FROM foo")
        mock_stream.assert_awaited()

    @patch(
        f"{__name__}.ch.Clickhouse._stream_to_duck",
        new_callable=AsyncMock,
    )
    async def test_tap_failure_raises(self, mock_stream):
        self.ck.c.sql = AsyncMock()
        self.ck.progress = MagicMock()
        mock_stream.side_effect = RuntimeError("fail")
        with self.assertRaises(e.UnrecoverableTapError):
            await self.ck.tap("SELECT * FROM something")

    @patch(
        f"{__name__}.ch.Clickhouse._sink_direct",
        new_callable=AsyncMock,
    )
    async def test_sink_direct_call(self, mock_sink_direct):
        self.ck._blocksize = 0
        self.ck.progress = MagicMock()
        await self.ck.sink("duck_table")
        mock_sink_direct.assert_awaited()

    @patch(
        f"{__name__}.ch.Clickhouse._sink_streaming",
        new_callable=AsyncMock,
    )
    async def test_sink_streaming_call(self, mock_sink_streaming):
        self.ck.progress = MagicMock()
        await self.ck.sink("duck_table")
        mock_sink_streaming.assert_awaited()

    async def test_show_schema_from_duck(self):
        self.ck.schema_ = MagicMock(show=AsyncMock(return_value="mock_fields"))
        self.ck.c = MagicMock()
        self.ck.c.sql = AsyncMock()
        df_mock = pd.DataFrame({"x": [1]})
        self.ck.c.sql.return_value.df = AsyncMock(return_value=df_mock)

        result = await self.ck.show_schema()
        self.assertEqual(result, "mock_fields")

    @patch(f"{__name__}.ch.ClickhouseClient.query_df")
    async def test_show_schema_fallback_query(self, mock_query_df):
        self.ck.client = self.ck._client
        self.ck.ch = MagicMock()
        self.ck.schema_ = MagicMock(show=AsyncMock(return_value="fields"))
        mock_query_df.return_value = pd.DataFrame({"col": [1, 2]})
        self.ck.c.register = AsyncMock()
        self.ck.c.unregister = AsyncMock()
        result = await self.ck.show_schema()
        self.assertEqual(result, "fields")

    @patch(f"{__name__}.ch.ClickhouseClient.query_df")
    async def test_sql_query_executes(self, mock_query_df):
        self.ck.client = self.ck._client
        self.ck.ch = MagicMock()
        mock_query_df.return_value = pd.DataFrame({"col": [1]})
        self.ck.progress = MagicMock()
        self.ck.log = MagicMock()
        await self.ck.sql("SELECT * FROM my_table")


if __name__ == "__main__":
    unittest.main()
