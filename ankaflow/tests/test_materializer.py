import unittest
import tempfile
from unittest.mock import AsyncMock, MagicMock, patch
from io import StringIO, FileIO
from pathlib import Path
import logging

from ..connections.rest import common

class TestMaterializer(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        patcher = patch(f"{__name__}.common.log.warning")
        self.mock_warning = patcher.start()
        self.addCleanup(patcher.stop)
        self.mock_connection = AsyncMock()
        self.mock_schema = MagicMock(spec=common.Schema)
        self.mock_schema.generate.return_value = "CREATE TABLE test_table ..."
        self.mock_logger = MagicMock(spec=logging.Logger)
        self.materializer = common.Materializer(
            self.mock_connection,
            common.m.DataType.JSONL,
            "test_table",
            self.mock_schema,
            columns=[common.m.Field(name="col1", type="VARCHAR")],
            logger=self.mock_logger,
        )

    async def test_create_table(self):
        await self.materializer.create_table()
        self.mock_schema.generate.assert_called_once()
        self.mock_connection.sql.assert_called_once()

    async def test_create_table_exception(self):
        self.mock_connection.sql.side_effect = Exception("Database error")
        with self.assertRaises(Exception):  # noqa: B017
            await self.materializer.create_table()
        # Expect logger.error (not logger.exception) to be called
        self.mock_logger.error.assert_called_once()

    def test_cols_to_map(self):
        expected_map = {"col1": "VARCHAR"}
        actual_map = self.materializer.cols_to_map()
        self.assertEqual(actual_map, expected_map)

    async def test__prepare_buffer_data_list(self):
        data = [{"col1": "value1"}]
        buffer = await self.materializer._prepare_buffer(data, None)
        self.assertIsInstance(buffer, StringIO)
        self.assertEqual(buffer.getvalue(), '[{"col1": "value1"}]') # type: ignore

    async def test__prepare_buffer_data_dict(self):
        data = {"col1": "value1"}
        buffer = await self.materializer._prepare_buffer(data, None)
        self.assertIsInstance(buffer, StringIO)
        self.assertEqual(buffer.getvalue(), '[{"col1": "value1"}]') # type: ignore

    async def test__prepare_buffer_data_string(self):
        data = '[{"col1": "value1"}]'
        buffer = await self.materializer._prepare_buffer(data, None)
        self.assertIsInstance(buffer, StringIO)
        self.assertEqual(buffer.getvalue(), '[{"col1": "value1"}]') # type: ignore

    async def test__prepare_buffer_file(self):
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as tmp_file:
            tmp_file.write('[{"col1": "value1"}]')
            filename = tmp_file.name

        buffer = await self.materializer._prepare_buffer(None, filename)  # Pass file
        self.assertIsInstance(buffer, FileIO)
        self.assertEqual(buffer.name, filename)
        buffer.close()
        Path(filename).unlink()

    async def test__prepare_buffer_no_data_or_file(self):
        with self.assertRaises(ValueError):
            await self.materializer._prepare_buffer(None, None)

    async def test__insert_data_jsonl(self):
        mock_buffer = MagicMock()
        await self.materializer._insert_data(mock_buffer)
        self.mock_connection.read_json.assert_called_once_with(
            mock_buffer, "test_table", {"columns": {"col1": "VARCHAR"}}
        )

    async def test__insert_data_csv(self):
        self.materializer.dtype = common.m.DataType.CSV
        mock_buffer = MagicMock()
        await self.materializer._insert_data(mock_buffer)
        self.mock_connection.read_csv.assert_called_once_with(
            mock_buffer, "test_table", {"columns": {"col1": "VARCHAR"}}
        )

    async def test__insert_data_parquet(self):
        self.materializer.dtype = common.m.DataType.PARQUET
        mock_buffer = MagicMock()
        await self.materializer._insert_data(mock_buffer)
        self.mock_connection.read_parquet.assert_called_once_with(
            mock_buffer, "test_table", {"columns": {"col1": "VARCHAR"}}
        )

    async def test__insert_data_unsupported_type(self):
        self.materializer.dtype = "UnsupportedType"  # type: ignore # Simulate unsupported type
        mock_buffer = MagicMock()
        with self.assertRaises(common.MaterializeError):
            await self.materializer._insert_data(mock_buffer)

    def test__cleanup_file(self):
        filename = "test_file.txt"
        with open(filename, "w") as f:
            f.write("test content")

        self.materializer._cleanup_file(filename)
        self.assertFalse(Path(filename).exists())

    def test__cleanup_file_error(self):
        filename = "test_file.txt"
        # Patch the module-level logger (log) used in _cleanup_file
        with patch(f"{__name__}.common.log.warning") as mock_warning:
            with patch("pathlib.Path.unlink") as mock_unlink:
                mock_unlink.side_effect = Exception("Deletion error")
                self.materializer._cleanup_file(filename)
                mock_warning.assert_called_once()

    async def test_materialize_list(self):
        data = [{"col1": "value1"}]
        await self.materializer.materialize(data)
        self.mock_connection.read_json.assert_called_once()

    async def test_materialize_file(self):
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as tmp_file:
            tmp_file.write('[{"col1": "value1"}]')
            filename = tmp_file.name

        await self.materializer.materialize(None, filename=filename) # type: ignore

        self.mock_connection.read_json.assert_called_once()
        self.assertFalse(Path(filename).exists())

    async def test_materialize_error(self):
        self.mock_connection.read_json.side_effect = Exception("DB Error")
        with self.assertRaises(common.MaterializeError):
            await self.materializer.materialize([{"col1": "value1"}])
        # Expect logger.error calls (instead of logger.exception)
        self.mock_logger.error.assert_called()

    async def test_materialize_no_fields(self):
        self.materializer.fields = None # type: ignore
        data = [{"col1": "value1"}]
        await self.materializer.materialize(data)
        self.mock_schema.generate.assert_not_called()
        self.mock_connection.read_json.assert_called_once()

if __name__ == "__main__":
    unittest.main()
