from pydantic import BaseModel
import typing as t
import json
import re
import os
import logging

from .errors import FetchError
from ..rest import rest
from ...common.util import null_logger
from ...common.types import StringDict
from ...models import llm
from ...models import rest as rst
from ...models import enums


def parse_model_response(content: str) -> dict:
    """Extracts a valid JSON object from a model-generated response string.

    Args:
        content: The full model response string.

    Returns:
        Parsed JSON as dict.

    Raises:
        ValueError: If no valid JSON object can be extracted.
    """
    content = content.strip()

    print("CONTENT")
    print(content)

    if content.startswith("{"):
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            pass

    match = re.search(r"```json\s*(\{.*?\})\s*```", content, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(1))
        except json.JSONDecodeError:
            raise ValueError("Malformed JSON inside ```json``` block.")

    match = re.search(r"(\{.*?\})", content, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(1))
        except json.JSONDecodeError:
            raise ValueError("Malformed inline JSON object.")

    raise ValueError("No valid JSON object found in model response.")


class Materializer:
    """Proxy response from a REST API call."""

    def __init__(self):
        self.data = {}

    async def materialize(self, data: t.Any):
        """Stores materialized data.

        Args:
            data: The response payload to store.
        """
        self.data = data


class LLMResponse(BaseModel):
    """Represents the output of an LLM protocol call."""

    query: str | None
    message: str | None


class LLMProtocol(t.Protocol):
    """LLM protocol interface for formatting and parsing."""

    kind: str

    def format(self, prompt: str, extras: dict | None = None) -> dict: ...

    def parse(self, response: dict) -> LLMResponse: ...


class LLMAuditor(t.Protocol):
    """Audit interface for observing LLM client behavior."""

    def pre_audit(self, protocol: LLMProtocol) -> None: ...

    def post_audit(self, protocol: LLMProtocol, response: dict) -> None: ...


class MockProtocol(BaseModel):
    """Mock protocol for testing and offline scenarios."""

    kind: t.Literal["mock"] = "mock"
    model: str = "mock-model"
    temperature: float = 0.0

    def format(self, prompt: str, extras: t.Optional[dict] = None) -> dict:
        return {
            "model": self.model,
            "prompt": prompt,
            "extras": extras or {},
        }

    def parse(self, response: dict) -> LLMResponse:
        return LLMResponse(
            query=response.get("query"), message=response.get("message")
        )


class OpenAIProtocol(BaseModel):
    """Concrete implementation of LLMProtocol for OpenAI-compatible models."""

    kind: t.Literal["openai"] = "openai"
    model: str = "gpt-4"
    temperature: float = 0.0

    def format(self, prompt: str, extras: t.Optional[dict] = None) -> dict:
        messages = [{"role": "system", "content": prompt}]
        if extras:
            messages.append({
                "role": "user",
                "content": "\n".join(extras.values()),
            })
        return {
            "model": self.model,
            "temperature": self.temperature,
            "messages": messages,
        }

    def parse(self, response: dict) -> LLMResponse:
        content = response["choices"][0]["message"]["content"]
        parsed = parse_model_response(content)
        return LLMResponse(
            query=parsed.get("query"), message=parsed.get("message")
        )


class LLMClient(t.Protocol):
    """Abstract interface for LLM client transports."""

    async def fetch(
        self, prompt: str, extras: t.Dict | None = None
    ) -> dict: ...


class OpenAIClient:
    """Stub OpenAI client implementation using the protocol interface."""

    def __init__(
        self,
        protocol: LLMProtocol,
        logger: logging.Logger | None,
        auditor: t.Optional[LLMAuditor] = None,
    ):
        self.auditor = auditor  # currently unused
        self.logger = logger or null_logger()
        self.protocol = protocol
        self.materializer = Materializer()
        self._client: rest.RestApi = self._make_client()

    def _make_client(self):
        cfg = rst.RestClientConfig(
            base_url="https://api.openai.com",
            auth=rst.RestAuth(
                method=rst.AuthType.OAUTH2,
                values=StringDict({
                    "token": os.getenv("OPENAI_API_KEY", "missing-api-key")
                }),
            ),
        )
        return rest.RestApi(cfg, self.materializer, logger=self.logger)

    def _make_request(self, prompt: str, extras: t.Dict | None = None):
        return rst.Request(
            endpoint="/v1/chat/completions",
            method=rst.RequestMethod.POST,
            content_type=enums.ContentType.JSON,
            body=self.protocol.format(prompt, extras=extras or {}),
            response=rst.RestResponse(
                handler=rst.BasicHandler(kind=rst.ResponseHandlerTypes.BASIC),
                content_type=enums.DataType.JSON,
                locator="",
            ),
        )

    async def fetch(self, prompt: str, extras: t.Dict | None = None) -> dict:
        # TODO: add audit hooks
        try:
            await self._client.fetch(
                self._make_request(prompt, extras=extras or {})
            )
            return self.materializer.data
        except Exception as e:
            raise FetchError(f"OpenAI fetch failed: {e}") from e


class ProxyClient:
    """Stub proxy-based LLM client using an external endpoint."""

    def __init__(
        self,
        protocol: LLMProtocol,
        proxy: llm.LLMProxy,
        logger: logging.Logger | None = None,
        auditor: t.Optional[LLMAuditor] = None,
    ):
        self.protocol = protocol
        self.client = proxy.client
        self.logger = logger or null_logger()
        self.request = proxy.request
        self.auditor = auditor
        self.materializer = Materializer()
        self._client = self._make_client()

    def _make_client(self):
        return rest.RestApi(self.client, self.materializer, logger=self.logger)

    def _make_request(self, prompt: str, extras: t.Dict | None = None):
        request = self.request.model_copy(deep=True)
        request.body = self.protocol.format(prompt, extras=extras or {})
        return request

    async def fetch(self, prompt: str, extras: t.Dict | None = None) -> dict:
        # TODO: add audit hooks
        try:
            await self._client.fetch(
                self._make_request(prompt, extras=extras or {})
            )
            return self.materializer.data
        except Exception as e:
            raise FetchError(f"Proxy fetch failed: {e}") from e


class MockClient:
    """Stub client that returns static or echo-like responses for testing."""

    def __init__(
        self,
        protocol: LLMProtocol,
        logger: logging.Logger | None = None,
        auditor: t.Optional[LLMAuditor] = None,
    ):
        self.protocol = protocol
        self.response = {
            "query": "SELECT 1 AS mock;",
            "message": "This is a mock response.",
        }
        self.auditor = auditor
        self.logger = logger or null_logger()

    async def fetch(self, prompt: str, extras: dict | None = None) -> dict:
        if prompt.startswith("ERROR"):
            raise FetchError("Mock Error Requested")
        try:
            # If prompt is a JSON literal, use it directly
            response = json.loads(prompt)
            if isinstance(response, dict):
                return response
        except json.JSONDecodeError:
            pass

        return {
            "query": "SELECT 1 AS mock;",
            "message": "This is a mock response.",
        }
