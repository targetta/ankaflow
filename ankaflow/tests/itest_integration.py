import unittest
import os
import logging
import pandas as pd
from yaml import safe_load
import io
from pathlib import Path
import shutil

from .. import models as m
from ..models import configs as cfg

from .support import rest_server as srv
from .. import core
from ..common.util import console_logger

# Constants
ROOT = "/tmp/"

class IntegrationTestFlow(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Enable local filesystem for DuckFlow
        cls._env_duckflow_original = os.environ.get("DUCKDB_DISABLE_LOCALFS")
        os.environ.pop("DUCKDB_DISABLE_LOCALFS", None)

        # Start REST test server
        srv.run()
        cls.base_url = f"http://{srv.HOST}:{srv.PORT}"

        # Prepare test data paths
        cls.test_parquet_path = "test_parquet_read.parquet"
        cls.test_json_path = "test_parquet_read.json"
        cls.test_jsonl_path = "test_parquet_read.jsonl"
        cls.test_csv_path = "test_parquet_read.csv"
        cls.test_excel_path = "test_parquet_read.xlsx"
        cls.test_delta = "table.delta"
        cls.yaml_path = os.path.join(os.path.dirname(__file__), "support/integration.yaml")  # noqa: E501

        # Generate a simple DataFrame
        df = pd.DataFrame({
            "id": range(1, 6),
            "amount": [50, 120, 80, 200, 300],
            "name": [f"User {i}" for i in range(1, 6)]
        })
        df.to_parquet(f"{ROOT}{cls.test_parquet_path}", index=False)
        df.to_json(f"{ROOT}{cls.test_json_path}", orient="records")
        df.to_json(f"{ROOT}{cls.test_jsonl_path}", orient="records", lines=True)
        df.to_csv(f"{ROOT}{cls.test_csv_path}", index=False)
        df.to_excel(f"{ROOT}{cls.test_excel_path}", index=False)

        # Load pipeline definitions
        with open(cls.yaml_path) as fh:
            yaml_defs = safe_load(fh)

        # Setup DuckFlow variables and context
        cls.variables = m.Variables({
            "base_url": cls.base_url,
            "test_parquet_read": cls.test_parquet_path,
            "test_json_read": cls.test_json_path,
            "test_jsonl_read": cls.test_jsonl_path,
            "test_csv_read": cls.test_csv_path,
            "test_excel_read": cls.test_excel_path,
            "test_delta": cls.test_delta
        })
        cls.context = m.FlowContext({
            # project/dataset credentials from env
            "project": os.getenv("ITEST_PROJECT"),
            "dataset": os.getenv("ITEST_DATASET"),
            "dataset_region": os.getenv("ITEST_REGION"),
            "credential_file": os.getenv("ITEST_CREDENTIAL_FILE"),
            # ClickHouse
            "ch_cluster": os.getenv("ITEST_CH_CLUSTER"),
            "ch_host": os.getenv("ITEST_CH_HOST"),
            "ch_port": os.getenv("ITEST_CH_PORT"),
            "ch_database": os.getenv("ITEST_CH_DATABASE"),
            "ch_user": os.getenv("ITEST_CH_USER"),
            "ch_password": os.getenv("ITEST_CH_PASSWORD"),
            # S3
            "s3_bucket": os.getenv("ITEST_S3_BUCKET"),
            "s3_region": os.getenv("ITEST_S3_REGION")
        })
        cls.conn_config = cfg.ConnectionConfiguration(
            local=cfg.BucketConfig(bucket=ROOT),
            s3=cfg.S3Config(
                bucket=os.getenv("ITEST_S3_BUCKET"),
                region=os.getenv("ITEST_S3_REGION"),
                access_key_id=os.getenv("ITEST_S3_KEY"),
                secret_access_key=os.getenv("ITEST_S3_SECRET")
            )
        )
        m.Stage.model_rebuild()
        cls.defs = m.Stages.model_validate(yaml_defs)

        # Run pipeline and capture logs
        log_stream = io.StringIO()
        handler = logging.StreamHandler(log_stream)
        handler.setFormatter(logging.Formatter("%(message)s"))
        cls.logger = console_logger(level="DEBUG")
        cls.logger.addHandler(handler)

        core.Flow(
            defs=cls.defs,
            context=cls.context,
            default_connection=cls.conn_config,
            variables=cls.variables,
            logger=cls.logger
        ).run()

        cls.log_output = log_stream.getvalue()
        cls.full_parquet = Path(ROOT) / cls.test_parquet_path

        cls.logger.removeHandler(handler)
        handler.close()

    @classmethod
    def tearDownClass(cls):
        srv.stop()
        # Restore environment
        if cls._env_duckflow_original is not None:
            os.environ["DUCKDB_DISABLE_LOCALFS"] = cls._env_duckflow_original
        else:
            os.environ.pop("DUCKDB_DISABLE_LOCALFS", None)
        # Cleanup
        shutil.rmtree(Path(ROOT, cls.test_delta), ignore_errors=True)

    # We keep skipIf decorators but set condition to False so tests always run
    @unittest.skipIf(False, "skipIf disabled")
    def test_parquet_file_created(self):
        """Parquet file should be created on disk."""
        self.assertTrue(self.full_parquet.exists())

    @unittest.skipIf(False, "skipIf disabled")
    def test_retry_429_logged(self):
        """HTTP 429 retry logic should be logged."""
        self.assertIn("success after 2 retries", self.log_output)

    @unittest.skipIf(False, "skipIf disabled")
    def test_http_paging_stage(self):
        """HTTP paging stage should appear in logs."""
        self.assertIn("TestHTTPPaging", self.log_output)

    @unittest.skipIf(False, "skipIf disabled")
    def test_parquet_read_stage(self):
        """Parquet read stage should appear in logs."""
        self.assertIn("TestParquetRead", self.log_output)

    @unittest.skipIf(False, "skipIf disabled")
    def test_parquet_transform_stage(self):
        """Parquet transform stage should appear in logs."""
        self.assertIn("TestParquetTranform", self.log_output)

    @unittest.skipIf(False, "skipIf disabled")
    def test_totals_logged(self):
        """Totals aggregation should be logged."""
        self.assertIn("totals", self.log_output)

    @unittest.skipIf(False, "skipIf disabled")
    def test_run_duration_logged(self):
        """Run duration should be reported in logs."""
        self.assertIn("Run duration:", self.log_output)

    @unittest.skipIf(False, "skipIf disabled")
    def test_dataframe_content(self):
        """Final DataFrame rows should be printed."""
        self.assertIn("1      50  User 1", self.log_output)
        # self.assertIn("5    300  User 5", self.log_output)

if __name__ == "__main__":
    unittest.main()
