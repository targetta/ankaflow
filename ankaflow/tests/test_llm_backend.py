from logging import config
import unittest
import asyncio
import json
from unittest.mock import AsyncMock, MagicMock
import typing as t

from ..connections.llm.protocols import (  # replace with actual path
    parse_model_response,
    MockProtocol,
    OpenAIProtocol,
    MockClient,
    ProxyClient,
    OpenAIClient,
    Materializer,
    FetchError,
    LLMProtocol,
)

from ..connections.llm.sqlgen import make_client, make_protocol

from .. import models as m
from ..models import enums
from ..models import rest
from ..models import llm


# Mock backend enum/constants
class FakeKind:
    OPENAI = "openai"
    MOCK = "mock"


class FakeBackend:
    def __init__(self, kind="mock", model=None, temperature=None):
        self.kind = kind
        self.model = model
        self.temperature = temperature


class FakeProxy:
    def __init__(self):
        self.client = MagicMock()
        self.client.base_url = "http://fake.com"
        self.request = MagicMock()
        self.request.model_copy = MagicMock(return_value=MagicMock())


class TestParseModelResponse(unittest.TestCase):
    def test_plain_json(self):
        assert (
            parse_model_response('{"query": "SELECT 1;"}')["query"]
            == "SELECT 1;"
        )

    def test_fenced_json(self):
        result = parse_model_response('```json\n{"query": "SELECT 2;"}\n```')
        self.assertEqual(result["query"], "SELECT 2;")

    def test_inline_json(self):
        result = parse_model_response(
            'Here is a result: {"query": "SELECT 3;"}'
        )
        self.assertEqual(result["query"], "SELECT 3;")

    def test_malformed_json_raises(self):
        with self.assertRaises(ValueError):
            parse_model_response("not valid json")


class TestMockProtocol(unittest.TestCase):
    def test_format_and_parse(self):
        proto = MockProtocol()
        result = proto.format("test", {"a": 1})
        self.assertIn("model", result)
        parsed = proto.parse({"query": "SELECT 1;", "message": "hi"})
        self.assertEqual(parsed.query, "SELECT 1;")


class TestOpenAIProtocol(unittest.TestCase):
    def test_parse_openai_response(self):
        proto = OpenAIProtocol()
        response = {
            "choices": [
                {
                    "message": {
                        "content": '{"query": "SELECT 5;", "message": "ok"}'
                    }
                }
            ]
        }
        result = proto.parse(response)
        self.assertEqual(result.query, "SELECT 5;")


class AsyncTestCase(unittest.IsolatedAsyncioTestCase):
    async def test_mock_client_response(self):
        client = MockClient(protocol=t.cast(LLMProtocol, MockProtocol()))
        resp = await client.fetch("some prompt")
        self.assertIn("query", resp)

    async def test_mock_client_error(self):
        client = MockClient(protocol=t.cast(LLMProtocol, MockProtocol()))
        with self.assertRaises(FetchError):
            await client.fetch("ERROR trigger")

    async def test_mock_client_json_echo(self):
        client = MockClient(protocol=t.cast(LLMProtocol, MockProtocol()))
        raw = {"query": "SELECT 9;"}
        result = await client.fetch(json.dumps(raw))
        self.assertEqual(result["query"], "SELECT 9;")

    async def test_proxy_client(self):
        proxy = FakeProxy()

        # Build a valid fake Request
        real_request = rest.Request(
            endpoint="/test",
            method=rest.RequestMethod.POST,
            content_type=enums.ContentType.JSON,
            body={"prompt": "x"},
            response=rest.RestResponse(
                handler=rest.BasicHandler(kind=rest.ResponseHandlerTypes.BASIC),
                content_type=enums.DataType.JSON,
                locator="",
            ),
        )
        proxy.request.model_copy = (
            lambda deep=False: real_request
        )  # return the valid request
        client = ProxyClient(
            protocol=t.cast(LLMProtocol, MockProtocol()),
            proxy=t.cast(llm.LLMProxy, proxy),
        )
        # Patch the _client.fetch (RestApi.fetch), not the top-level fetch()
        client._client.fetch = AsyncMock(return_value=None)

        # Simulate the materialized data
        client.materializer.data = {"query": "SELECT 1", "message": "ok"}

        result = await client.fetch("test")

        self.assertIsInstance(result, dict)

    async def test_openai_client(self):
        client = OpenAIClient(
            protocol=t.cast(LLMProtocol, MockProtocol()), logger=None
        )
        client._client.fetch = AsyncMock(return_value=None)
        result = await client.fetch("prompt")
        self.assertIsInstance(result, dict)


class TestFactories(unittest.TestCase):
    def test_make_protocol_openai(self):
        proto = make_protocol(
            t.cast(
                llm.LLMConfig, FakeBackend(kind=FakeKind.OPENAI, model="gpt-4")
            )
        )
        self.assertIsInstance(proto, OpenAIProtocol)

    def test_make_protocol_mock(self):
        proto = make_protocol(
            t.cast(llm.LLMConfig, FakeBackend(kind=FakeKind.MOCK))
        )
        self.assertIsInstance(proto, MockProtocol)

    def test_make_protocol_invalid(self):
        with self.assertRaises(ValueError):
            make_protocol(t.cast(llm.LLMConfig, FakeBackend(kind="other")))

    def test_make_client_mock(self):
        proto = MockProtocol()
        client = make_client(t.cast(LLMProtocol, proto))
        self.assertIsInstance(client, MockClient)


class TestMaterializer(unittest.TestCase):
    def test_materialize_store(self):
        m = Materializer()
        asyncio.run(m.materialize({"x": 1}))
        self.assertEqual(m.data["x"], 1)


if __name__ == "__main__":
    unittest.main()
