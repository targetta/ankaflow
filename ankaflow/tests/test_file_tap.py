import unittest
from unittest.mock import AsyncMock, MagicMock
from ..connections.connection import Connection

# Create a minimal test dummy to expose the protected base method safely
class TestableConnection(Connection):
    pass

class TestExecuteFileTap(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        """Set up a mocked version of a connection instance."""
        # Create an instance of our testable dummy
        mock = MagicMock()
        self.conn = TestableConnection(mock, "test", mock, mock, mock)
        self.conn.name = "my_target_table"

        # Mock the engine executor
        self.conn.c = MagicMock()
        self.conn.c.sql = AsyncMock()

        # Mock the file locator
        self.conn.locate = MagicMock(
            return_value="http://s3/bucket/filename.csv"
        )

    async def test_execute_file_tap_default_no_query_no_limit(self):
        """Test the fallback logic when no query or limit is provided."""
        await self.conn._execute_file_tap(
            reader_func_name="read_csv", 
            query=None, 
            opts={"ignore_errors": True}, 
            limit=0
        )

        # Notice how SQLGlot spaces assignments out as `key = value`
        expected_sql = (
            'CREATE OR REPLACE TABLE "my_target_table" AS '
            "SELECT * FROM READ_CSV('http://s3/bucket/filename.csv', ignore_errors = TRUE)"  # noqa: E501
        )
        self.conn.c.sql.assert_awaited_once_with(expected_sql) # type: ignore

    async def test_execute_file_tap_swaps_table(self):
        """Test that a user query has its placeholder correctly replaced."""
        user_query = "SELECT id, name FROM placeholder_tbl WHERE id > 10"

        await self.conn._execute_file_tap(
            reader_func_name="read_csv", 
            query=user_query, 
            opts=None, 
            limit=0
        )

        expected_sql = (
            'CREATE OR REPLACE TABLE "my_target_table" AS '
            "SELECT id, name FROM READ_CSV('http://s3/bucket/filename.csv') WHERE id > 10"  # noqa: E501
        )
        self.conn.c.sql.assert_awaited_once_with(expected_sql) # type: ignore

    async def test_execute_file_tap_wraps_subquery_with_limit(self):
        """Test that limit wraps the mutated query inside a subquery 'sub'."""
        user_query = "SELECT * FROM anything WHERE status = 'active'"

        await self.conn._execute_file_tap(
            reader_func_name="read_csv", 
            query=user_query, 
            opts=None, 
            limit=100
        )

        expected_sql = (
            'CREATE OR REPLACE TABLE "my_target_table" AS '
            "SELECT * FROM ("
            "SELECT * FROM READ_CSV('http://s3/bucket/filename.csv') WHERE status = 'active'"  # noqa: E501
            ") AS sub LIMIT 100"
        )
        self.conn.c.sql.assert_awaited_once_with(expected_sql) # type: ignore