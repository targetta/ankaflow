import unittest
import os
import logging
import io

from .support import rest_server as srv
from .. import core, models as m
from ..common.util import console_logger
from ..models import Stages

# Constants
ROOT = "/tmp/"

class IntegrationTestFlow(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Enable local filesystem for DuckFlow
        cls._env_duckflow_original = os.environ.get("DUCKDB_DISABLE_LOCALFS")
        os.environ.pop("DUCKDB_DISABLE_LOCALFS", None)
        cls.yaml_path = os.path.join(os.path.dirname(__file__), "support/sqlgen.yaml")  # noqa: E501

        # Start REST test server
        srv.run()
        cls.base_url = f"http://{srv.HOST}:{srv.PORT}"

        # Load pipeline definitions
        cls.defs = Stages.load(cls.yaml_path)

        # Setup DuckFlow variables and context
        cls.variables = m.Variables()
        cls.context = m.FlowContext()
        cls.conn_config = m.ConnectionConfiguration(
            local=m.BucketConfig(bucket=ROOT)
        )

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

    # We keep skipIf decorators but set condition to False so tests always run
    @unittest.skipIf(False, "skipIf disabled")
    def test_something(self):
        pass

if __name__ == "__main__":
    unittest.main()
