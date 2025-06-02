from __future__ import annotations
import sys
import logging
import typing as t
from jinja2 import Environment, BaseLoader


from . import protocols as p
from .. import Connection
from ..errors import UnrecoverableTapError
from ... import models as m
from ...models import llm
from ...internal.errors import (
    CatalogException,
    BinderException,
    ParserException,
    SyntaxException,
)

log = logging.getLogger(__name__)

IS_PYODIDE = sys.platform == "emscripten"

REPLAYABLE_SQL_ERRORS = (
    CatalogException,
    BinderException,
    ParserException,
    SyntaxException,
)

ERROR_EXTRAS_TEMPLATE = {
    "1": "Previous query failed.",  # noqa: E501
    "2": "",
    "3": "## Previous SQL Query:",
    "query": None,
    "4": "",
    "5": "## SQL Execution Error:",
    "error": None,
    "6": "",
    "7": "## Previous message to User (if any):",
    "message": None,
    "8": "",
    "9": "Review and regenerate the SQL query based on this feedback. Keep the original intent, correct the issues, and do not include explanations—only update the SQL.",  # noqa: E501
}





class FetchError(Exception): ...


# === Protocol Factory ===
def make_protocol(backend: llm.LLMConfig) -> p.LLMProtocol:
    """Resolves an LLMProtocol implementation from LLMBackend config."""
    if backend.kind == llm.LLMKind.OPENAI:
        return t.cast(
            p.LLMProtocol,
            p.OpenAIProtocol(
                model=backend.model or "gpt-4",
                temperature=backend.temperature or 0.0,
            ),
        )
    if backend.kind == llm.LLMKind.MOCK:
        return t.cast(
            p.LLMProtocol,
            p.MockProtocol(),
        )
    raise ValueError(f"Unsupported backend kind: {backend.kind}")


# === Client Factory ===
def make_client(
    protocol: p.LLMProtocol,
    proxy: llm.LLMProxy | None = None,
    logger: logging.Logger | None = None,
    auditor: p.LLMAuditor | None = None,
) -> p.LLMClient:
    """Resolves an LLMClient implementation from protocol and proxy config."""
    if protocol.kind == llm.LLMKind.MOCK:
        return p.MockClient(protocol, logger=logger, auditor=auditor)
    if proxy and IS_PYODIDE:
        return p.ProxyClient(protocol, proxy, auditor=auditor)
    if proxy:
        return p.ProxyClient(protocol, proxy, logger=logger, auditor=auditor)
    if protocol.kind == "openai":
        return p.OpenAIClient(protocol, logger=logger, auditor=auditor)
    raise ValueError(f"Unsupported protocol kind: {protocol.kind}")


class SQLGen(Connection):
    """Pipeline-facing connection that executes an LLM tap step."""

    # Local init called from base class __init__
    # mostly to reduce super boilerplate
    def init(self):
        self.auditor = None
        self.conn = t.cast(m.SQLGenConnection, self.conn)
        self._cfg = self.conn.config.llm # type: ignore
        self.protocol = make_protocol(self._cfg)
        self.client = make_client(self.protocol, self._cfg.proxy, self.log)
        self.materializer = p.Materializer()
        self._max_retries = 3
        self._retries_left = self._max_retries
        self._prompt: str | None = None
        # Debugging, introspection
        self.last_raw: dict | None = None
        self.last_response: p.LLMResponse | None = None
        self.success_msg = f"{self.name}_success"
        self.fail_msg = f"{self.name}_fail"
        self.user_msg = f"{self.name}_user"

    def _render_prompt(self, template: str) -> str:
        """Renders the user prompt using Jinja2 templating.

        Args:
            template: Jinja2 template string.

        Returns:
            Rendered prompt string.
        """
        jenv = Environment(loader=BaseLoader)  # type: ignore
        tmpl = jenv.from_string(template)
        return tmpl.render(**(self.conn.variables or {}))

    async def _replay(self, response: p.LLMResponse, error: Exception):
        self.log.debug(f"Replays left: {self._retries_left}")
        self._retries_left -= 1

        if self._retries_left < 0:
            self.vars[self.fail_msg] = str(error).replace("'", "''")[:500]
            raise UnrecoverableTapError("Retry limit reached")

        extras = {
            "query": response.query or "<null>",
            "error": str(error),
            "message": response.message or "<none>",
        }
        if not self._prompt:
            raise ValueError("Prompt must be set before calling fetch")
        self.log.info(f"Query was not properly composed:\n{error}\nRetrying...")  # noqa: E501
        raw = await self.client.fetch(self._prompt, extras)
        self.log.debug(f"Raw response:\n{raw}")
        parsed = self.protocol.parse(raw)
        self.log.debug(f"Parsed response:\n{parsed}")
        self.last_raw = raw
        self.last_response = parsed
        await self._handle_response(parsed)

    async def _handle_response(self, response: p.LLMResponse):
        self.log.info(f"Message to user:\n{response.message}")
        self.vars[self.user_msg] = response.message
        if not response.query:
            raise UnrecoverableTapError("No SQL query returned by model")

        try:
            await self._sql_execute(response.query)
            self.vars[self.success_msg] = True
        except REPLAYABLE_SQL_ERRORS as e:
            await self._replay(response, e)

    async def _sql_execute(self, querystring: str):
        """Executes the generated SQL query by creating a named view.

        Args:
            query: The SQL query to execute.
        """
        sql = f"""
        CREATE OR REPLACE VIEW "{self.name}"
        AS
        {querystring}
        """
        await self.c.sql(sql)

    async def tap(self, query: t.Optional[str] = None, limit: int = 0):
        if not query:
            raise ValueError("No prompt")
        self._prompt = self._render_prompt(query)
        self.log.debug(f"Prompt:\n{self._prompt}")
        raw = await self.client.fetch(self._prompt)
        parsed = self.protocol.parse(raw)
        self.last_raw = raw
        self.last_response = parsed
        self.log.debug(f"Parsed LLM response:\n{parsed}")
        await self._handle_response(parsed)

    async def sink(self, from_name: str):
        """No-op sink method placeholder."""
        raise RuntimeError("Sink not supported by SQLGenConnection")

    async def show_schema(self) -> t.Any:
        """No-op schema method placeholder."""
        raise RuntimeError("Show schema not supported by SQLGenConnection")
