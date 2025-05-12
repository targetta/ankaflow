import unittest
from unittest.mock import AsyncMock, MagicMock

from ..models.llm import LLMConfig, LLMKind
from ..models import ConnectionConfiguration, SQLGenConnection

from ..connections.llm.sqlgen import (
    SQLGen,
    UnrecoverableTapError,
    REPLAYABLE_SQL_ERRORS
)

from ..connections.llm.protocols import LLMResponse


class TestLLMConnectionReplay(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.vars = {}
        self.conn = SQLGenConnection(
            kind="SQLGen",
            variables={"var": "value"},
            config=ConnectionConfiguration(
            llm=LLMConfig(
                kind=LLMKind.MOCK,
                model="mock-model",
            )
        )
        )
        self.duck = MagicMock()
        self.llm = SQLGen(
            duck=self.duck,
            name="Test",
            connection=self.conn, # type: ignore
            context=MagicMock(),
            variables=self.vars, # type: ignore - Varibales is just wrapper around dict 
            logger=MagicMock(),
        )
        self.llm.init()
        self.llm._prompt = "SELECT * FROM test;"
        self.llm.c = MagicMock()

    async def test_replay_on_recoverable_error(self):
        """Should retry once and succeed on replayable error."""
        response1 = LLMResponse(query="SELECT * FROM error;", message="Bad")
        response2 = LLMResponse(query="SELECT * FROM good;", message="Ok")

        mock_proto = MagicMock()
        mock_proto.kind = "mock"
        mock_proto.format = MagicMock(return_value={})
        mock_proto.parse = MagicMock(side_effect=[response1, response2])
        self.llm.protocol = mock_proto

        self.llm.client.fetch = AsyncMock(
            side_effect=[{"query": "SELECT *", "message": "retrying"}]
        )
        self.llm._sql_execute = AsyncMock(
            side_effect=[REPLAYABLE_SQL_ERRORS[0]("fail"), None]
        )

        await self.llm._handle_response(response1)

        self.assertEqual(self.llm.vars[self.llm.success_msg], True)
        self.assertEqual(self.llm._retries_left, 2)  # default max_retries=3

    async def test_fails_after_max_retries(self):
        """Should raise UnrecoverableTapError after exhausting retries."""

        response = LLMResponse(query="SELECT * FROM table;", message="msg")

        # Use a mock protocol that always returns the same response
        mock_proto = MagicMock()
        mock_proto.kind = "mock"
        mock_proto.parse = MagicMock(return_value=response)
        self.llm.protocol = mock_proto

        # Simulate repeated replayable errors on execution
        self.llm.client.fetch = AsyncMock(
            return_value={"query": "Q", "message": "retrying"}
        )
        self.llm._sql_execute = AsyncMock(
            side_effect=REPLAYABLE_SQL_ERRORS[1]("fail")
        )

        # Execute enough retries to exceed limit
        with self.assertRaises(UnrecoverableTapError):
            await self.llm._handle_response(response)

        # After all retries exhausted, SELECT_FAIL should be set
        self.assertIn(self.llm.fail_msg, self.vars)
        self.assertIn("fail", self.vars[self.llm.fail_msg])
        self.assertNotIn(self.llm.success_msg, self.vars)  # sanity check

    async def test_no_query_sets_feedback_and_raises(self):
        """Should set feedback and raise if query is missing."""
        response = MagicMock(query=None, message="Sorry, try again")
        with self.assertRaises(UnrecoverableTapError):
            await self.llm._handle_response(response)
        self.assertEqual(self.vars[self.llm.user_msg], "Sorry, try again")
    
    async def test_sql_execute_triggers(self):
        self.llm.c = MagicMock()
        self.llm.c.sql = AsyncMock()

        query = "SELECT 42 AS answer"
        await self.llm._sql_execute(query)

        self.llm.c.sql.assert_awaited_once()
        sql_text = self.llm.c.sql.await_args.args[0]
        self.assertIn('CREATE OR REPLACE VIEW "Test"', sql_text)
        self.assertIn(query.strip(), sql_text)


if __name__ == "__main__":
    unittest.main()
