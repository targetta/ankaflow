import unittest
import tempfile
import os
import shutil
from unittest.mock import MagicMock, AsyncMock, patch

# import pandas as pd
import pyarrow as pa

from deltalake import DeltaTable

from ..connections.delta import Deltatable, SinkStrategy
from ..models import core as m
from ..models.components import Column, Columns
from ..models.configs import ConnectionConfiguration, BucketConfig
from ..connections.connection import Schema
from ..internal.server import DDB


class TestDeltatable(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.path = os.path.join(self.tmpdir, "table.delta")
        self.arrow_table = pa.table({
            "id": pa.array([1, 2], type=pa.int64()),
            "amount": pa.array([100.5, 200.0], type=pa.float64()),
            "name": pa.array(["Alice", "Bob"], type=pa.string()),
        })

        # Argument missing for parameter "create_statement"Pylance
        self.mock_conn = m.DeltatableConnection(
            kind="Deltatable",
            locator=self.path,
            config=ConnectionConfiguration(),
        )

        self.instance = Deltatable.__new__(Deltatable)
        self.instance.conn = self.mock_conn
        self.instance.name = "test_table"
        self.instance.ctx = m.FlowContext()
        self.instance.vars = m.Variables()
        self.instance.delta_opts = {}
        self.instance.log = MagicMock()
        self.instance.locate = lambda use_wildcard=False: self.path  # type: ignore
        self.instance.c = AsyncMock()
        duck = DDB(ConnectionConfiguration(local=BucketConfig(bucket="*")))
        self.instance.schema_ = Schema(duck=duck)  # type: ignore # noqa: E501

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_generate_metadata(self):
        result = self.instance._generate_metadata()
        self.assertIn("Table rows are versioned", result)

    def test_make_cte_and_final(self):
        cte = self.instance._make_cte("delta_scan('uri')")
        sql = self.instance._make_final_select(cte, "SELECT * FROM t", "")
        self.assertIn("WITH t AS", sql)
        self.assertIn("CREATE TABLE", sql)

    async def test_create_strategy_all_cases(self):
        self.instance.conn.fields = Columns([
            Column(name="id", type="BIGINT"),
            Column(name="amount", type="DOUBLE"),
        ])
        result = self.instance._create_strategy(0)
        self.assertEqual(result, SinkStrategy.CREATE)

        self.instance.conn.fields = None
        result = self.instance._create_strategy(0)
        self.assertEqual(result, SinkStrategy.SKIP)

        result = self.instance._create_strategy(2)
        self.assertEqual(result, SinkStrategy.WRITE)

    async def test_infer_schema_from_df(self):
        schema = await self.instance._infer_schema(self.arrow_table)
        self.assertIsInstance(schema, pa.Schema)

    async def test_to_arrow_conversion(self):
        schema = await self.instance._infer_schema(self.arrow_table)
        tbl_or_reader = await self.instance._to_arrow(self.arrow_table, schema)
        self.assertTrue(
            isinstance(tbl_or_reader, pa.Table)
            or isinstance(tbl_or_reader, pa.RecordBatchReader)
        )

    def test_make_delta_kwargs(self):
        kwargs = self.instance._make_delta_kwargs(meta="test", create_flag=True)
        self.assertIn("description", kwargs)

    async def test_create_deltatable_and_show_schema(self):
        # Define fields that simulate what would come from upstream schema inference
        self.instance.conn.fields = Columns([
            Column(name="id", type="BIGINT"),
            Column(name="amount", type="DOUBLE"),
            Column(name="name", type="VARCHAR"),
        ])

        # Step 1: Create the Delta table at the path
        await self.instance._create_deltatable(self.path)

        # Step 2: Confirm DeltaTable was created and is accessible
        table = DeltaTable(self.path)
        self.assertTrue(table.version() >= 0)

        # Step 3: Fetch schema from Delta metadata and assert fields match
        arrow_schema: pa.Schema = table.schema().to_pyarrow()
        delta_fields = {field.name: str(field.type) for field in arrow_schema}

        expected_fields = {"id": "int64", "amount": "double", "name": "string"}

        self.assertEqual(delta_fields, expected_fields)

        # Step 4: Optionally, use show_schema() to get m.Fields
        schema = await self.instance.show_schema()
        self.assertEqual(
            [f.name for f in schema.values()], ["id", "amount", "name"]
        )

        schema = await self.instance.show_schema()
        self.assertGreaterEqual(len(schema.values()), 1)
        self.assertIn("id", [f.name for f in schema.values()])
        self.assertTrue(len(schema.values()) >= 1)

    async def test_write_deltatable_create_and_append(self):
        await self.instance._write_deltatable(
            self.path, self.arrow_table, create_flag=True
        )
        self.instance.conn.data_mode = "append"
        await self.instance._write_deltatable(
            self.path, self.arrow_table, create_flag=False
        )
        table = DeltaTable(self.path)
        self.assertEqual(table.version(), 1)

    async def test_truncate_and_drop(self):
        await self.instance._write_deltatable(
            self.path, self.arrow_table, create_flag=True
        )
        await self.instance._truncate_deltatable()
        self.assertTrue(DeltaTable(self.path).version() >= 1)

        self.instance._drop_deltatable()
        delta_table_path = os.path.join(self.path, "table.delta")
        self.assertFalse(os.path.exists(delta_table_path))

    async def test_sql_dispatch(self):
        await self.instance._write_deltatable(
            self.path, self.arrow_table, create_flag=True
        )

        await self.instance.sql("truncate deltatable")
        with self.assertRaises(ValueError):
            await self.instance.sql("select * from foo")

        await self.instance.sql("drop deltatable")
        delta_table_path = os.path.join(self.path, "table.delta")
        self.assertFalse(os.path.exists(delta_table_path))

    async def test_maybe_optimize_invalid_options(self):
        self.instance.conn.optimize = "not-a-valid-option"
        await self.instance._maybe_optimize(self.path)

        self.instance.conn.optimize = -1
        await self.instance._maybe_optimize(self.path)

        self.instance.conn.optimize = 9999
        await self.instance._maybe_optimize(self.path)

    async def test_tap_creates_table(self):
        self.instance.ranking = lambda *args, **kwargs: (
            "SELECT * FROM delta_scan('" + self.path + "')",
            "",
        )
        self.instance.c.sql = AsyncMock()
        await self.instance.tap("SELECT * FROM Deltatable", limit=0)
        self.instance.c.sql.assert_awaited()

    async def test_cast_dict_to_string(self):
        dict_col = pa.DictionaryArray.from_arrays(
            pa.array([0, 1, 0]), pa.array(["a", "b"])
        )
        t = pa.table({"k": dict_col})
        t2 = self.instance._cast_dict_to_string(t)
        assert pa.types.is_string(t2["k"].type)


def _patch_target() -> str:
    """Resolve the correct patch target for DeltaTable."""
    return f"{Deltatable.__module__}.dl.DeltaTable"


class TestDeltaOptimizeSQL(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp()
        self.path = os.path.join(self.tmpdir, "table.delta")

        # Minimal instance wiring
        self.instance = Deltatable.__new__(Deltatable)
        self.instance.conn = MagicMock()
        self.instance.name = "test_table"
        self.instance.ctx = MagicMock()
        self.instance.vars = MagicMock()
        self.instance.delta_opts = {}
        self.instance.log = MagicMock()
        self.instance.locate = lambda use_wildcard=False: self.path  # type: ignore
        self.instance.c = AsyncMock()

    def tearDown(self) -> None:
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    @patch(_patch_target())
    async def test_optimize_default_runs_compact_and_vacuum(
        self, MockDT: MagicMock
    ) -> None:
        """OPTIMIZE DELTATABLE → compact + vacuum with default retention."""
        dt = MockDT.return_value
        dt.optimize.compact = MagicMock()
        dt.vacuum = MagicMock()
        dt.cleanup_metadata = MagicMock()

        await self.instance.sql("optimize deltatable")

        dt.optimize.compact.assert_called_once()
        dt.vacuum.assert_called_once()
        # default 7 days → hours
        args, kwargs = dt.vacuum.call_args
        self.assertEqual(args[0], 7 * 24)
        self.assertEqual(kwargs.get("dry_run", False), False)
        self.assertEqual(kwargs.get("enforce_retention_duration", False), False)
        # cleanup is optional in default; assert not called implicitly
        dt.cleanup_metadata.assert_not_called()

    @patch(_patch_target())
    async def test_optimize_cleanup_only(self, MockDT: MagicMock) -> None:
        """OPTIMIZE DELTATABLE CLEANUP → only cleanup, no compact/vacuum."""
        dt = MockDT.return_value
        dt.optimize.compact = MagicMock()
        dt.vacuum = MagicMock()
        dt.cleanup_metadata = MagicMock()

        await self.instance.sql("optimize deltatable cleanup")

        dt.optimize.compact.assert_not_called()
        dt.vacuum.assert_not_called()
        dt.cleanup_metadata.assert_called_once()

    @patch(_patch_target())
    async def test_optimize_compact_only(self, MockDT: MagicMock) -> None:
        """OPTIMIZE DELTATABLE COMPACT → only compaction."""
        dt = MockDT.return_value
        dt.optimize.compact = MagicMock()
        dt.vacuum = MagicMock()
        dt.cleanup_metadata = MagicMock()

        await self.instance.sql(
            "OPTIMIZE   Deltatable   COMPACT"
        )  # mixed case + spacing

        dt.optimize.compact.assert_called_once()
        dt.vacuum.assert_not_called()
        dt.cleanup_metadata.assert_not_called()

    @patch(_patch_target())
    async def test_optimize_vacuum_age_hours_dryrun(
        self, MockDT: MagicMock
    ) -> None:
        """VACUUM AGE=36h DRY_RUN → no compact; vacuum with 36h dry run."""
        dt = MockDT.return_value
        dt.optimize.compact = MagicMock()
        dt.vacuum = MagicMock()
        dt.cleanup_metadata = MagicMock()

        await self.instance.sql("optimize deltatable vacuum age=36h dry_run")

        dt.optimize.compact.assert_not_called()
        dt.vacuum.assert_called_once()
        args, kwargs = dt.vacuum.call_args
        self.assertEqual(args[0], 36)
        self.assertTrue(kwargs.get("dry_run", False))
        self.assertEqual(kwargs.get("enforce_retention_duration", False), False)
        dt.cleanup_metadata.assert_not_called()

    @patch(_patch_target())
    async def test_optimize_compact_vacuum_age_days_cleanup(
        self, MockDT: MagicMock
    ) -> None:
        """COMPACT + VACUUM AGE=1 CLEANUP → runs all three in order."""
        dt = MockDT.return_value
        dt.optimize.compact = MagicMock()
        dt.vacuum = MagicMock()
        dt.cleanup_metadata = MagicMock()

        await self.instance.sql(
            "optimize deltatable compact vacuum age=1 cleanup"
        )

        dt.optimize.compact.assert_called_once()
        dt.vacuum.assert_called_once()
        args, _ = dt.vacuum.call_args
        self.assertEqual(args[0], 24)  # 1 day → 24 hours
        dt.cleanup_metadata.assert_called_once()

    async def test_sql_invalid_command(self) -> None:
        """Unknown SQL raises ValueError."""
        with self.assertRaises(ValueError):
            await self.instance.sql("optimize something_else")
