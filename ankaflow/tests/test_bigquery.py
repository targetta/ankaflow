import unittest
from unittest.mock import AsyncMock, MagicMock, patch
import pandas as pd
import logging
from types import SimpleNamespace

from google.cloud.bigquery.job import WriteDisposition, SchemaUpdateOption

from ..connections import bigquery as bg


class TestBigQueryConnection(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        mock_duck = MagicMock()
        mock_name = "test_table"
        mock_connection = MagicMock()
        mock_context = MagicMock()
        mock_variables = MagicMock()
        mock_connection = MagicMock()
        mock_connection.config = SimpleNamespace(
            bigquery = SimpleNamespace(
                project="test_project",
                dataset="test_dataset",
                region="US",
                credential_file=None
            )
        )

        self.conn = bg.BigQuery(
            duck=mock_duck,
            name=mock_name,
            connection=mock_connection,
            context=mock_context,
            variables=mock_variables,
        )

        self.conn.conn.data_mode = "append"
        self.conn.conn.schema_mode = "merge"
        self.conn.name = "test_table"
        self.conn.c = AsyncMock()
        self.conn.schema_ = AsyncMock()
        self.conn.log = self.conn.log = MagicMock(spec=logging.Logger)

    @patch(f"{__name__}.bg.Client")
    def test_get_client_from_service_account(self, mock_client):
        self.conn.cfg.bigquery.credential_file = "/fake/credentials.json"
        mock_instance = MagicMock()
        mock_client.from_service_account_json.return_value = mock_instance

        client = self.conn._get_client()
        self.assertEqual(client, mock_instance)

    def test_build_load_config_from_modes(self):
        config = self.conn._build_load_config_from_modes()
        self.assertEqual(config.write_disposition, WriteDisposition.WRITE_APPEND)
        self.assertEqual(config.schema_update_options, [SchemaUpdateOption.ALLOW_FIELD_ADDITION])  # noqa: E501

    @patch(f"{__name__}.bg.BigQuery._execute_query_to_dataframe")
    async def test_sql_logs_output(self, mock_exec_query):
        df = pd.DataFrame({"a": [1, 2]})
        mock_exec_query.return_value = df

        await self.conn.sql("SELECT * FROM test")
        self.conn.log.info.assert_called() # type: ignore


if __name__ == "__main__":
    unittest.main()
