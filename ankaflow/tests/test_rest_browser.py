# type: ignore
import sys
import types
from unittest.mock import AsyncMock
import unittest
from unittest.mock import MagicMock, patch

from ..connections.rest import browser as br
from .. import models as m
from ..models import rest as rst
from ..models import enums
from ..connections.rest import common

# Create mock for pyodide.http.pyfetch
mock_pyodide_http = types.ModuleType("pyodide.http")
mock_pyodide_http.pyfetch = AsyncMock()

# Also mock the parent `pyodide` package (optional unless accessed directly)
mock_pyodide = types.ModuleType("pyodide")
mock_pyodide.http = mock_pyodide_http

# Register both in sys.modules
sys.modules["pyodide"] = mock_pyodide
sys.modules["pyodide.http"] = mock_pyodide_http

# Create a fake 'js' module with the expected structure
mock_js = types.ModuleType("js")
mock_js.getHTTPResponse = lambda url, args: None

# Inject the fake module into sys.modules
sys.modules["js"] = mock_js


class MockJSResponse:
    def __init__(
        self, status=200, ok=True, url="https://example.com", content=b"{}"
    ):
        self.status = status
        self.ok = ok
        self.url = url
        self._json = content
        self._text = (
            content.decode() if isinstance(content, bytes) else str(content)
        )

    async def json(self):
        return self._json

    async def text(self):
        return self._text

    async def bytes(self):
        return self._json


class TestRestClient(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.mock_request = MagicMock(spec=rst.Request)
        self.mock_request.endpoint = "test"
        self.mock_request.query = {}
        self.mock_request.body = {"key": "val"}
        self.mock_request.method = enums.RequestMethod.POST
        self.mock_request.content_type = enums.ContentType.JSON
        self.mock_request.errorhandler = MagicMock()
        self.mock_request.errorhandler.condition = None
        self.mock_request.errorhandler.error_status_codes = []

        self.mock_client = MagicMock(spec=rst.RestClientConfig)
        self.mock_client.base_url = "https://example.com"
        self.mock_client.auth = None
        self.client = br.RestClient(self.mock_client)

    async def test_rest_like_response_wraps_js(self):
        mock = MockJSResponse(content=b'{"foo": "bar"}')
        wrapped = br.RestLikeResponse(mock, is_js=True)
        self.assertTrue(wrapped.ok)
        self.assertEqual(wrapped.status, 200)
        self.assertEqual(await wrapped.text(), mock._text)

    async def test_dispatch_fetch_uses_js(self):
        with (
            patch(f"{__name__}.br.JS_FETCH_AVAILABLE", True),
            patch(
                f"{__name__}.br.get_http_response",
                new_callable=AsyncMock,
            ) as mock_js,
        ):
            mock_js.return_value = MockJSResponse()
            result = await br._dispatch_fetch("https://mock.url", {}, "{}")
            self.assertIsInstance(result, br.RestLikeResponse)

    @patch(f"{__name__}.br.JS_FETCH_AVAILABLE", False)
    @patch(f"{__name__}.br.pyfetch", new_callable=AsyncMock)
    async def test_dispatch_fetch_uses_pyfetch(mock_pyfetch, _mock_js_flag):
        mock_pyfetch.return_value = MockJSResponse()
        result = await br._dispatch_fetch("https://mock.url", {}, "{}")
        assert isinstance(result, br.RestLikeResponse)

    async def test_handle_response_400_raises(self):
        mock = MockJSResponse(status=400, content=b"fail")
        wrapped = br.RestLikeResponse(mock, is_js=True)
        self.client.request = self.mock_request
        with self.assertRaises(common.RestRequestError):
            await self.client.handle_response(wrapped)

    async def test_handle_response_429_raises(self):
        mock = MockJSResponse(status=429, content=b"rate")
        wrapped = br.RestLikeResponse(mock, is_js=True)
        self.client.request = self.mock_request
        with self.assertRaises(common.RestRateLimitError):
            await self.client.handle_response(wrapped)

    async def test_handle_response_500_raises(self):
        mock = MockJSResponse(status=500, content=b"server")
        wrapped = br.RestLikeResponse(mock, is_js=True)
        self.client.request = self.mock_request
        with self.assertRaises(common.RestRetryableError):
            await self.client.handle_response(wrapped)

    async def test_fetch_successful(self):
        with patch(
            f"{__name__}.br._dispatch_fetch",
            new_callable=AsyncMock,
        ) as mock_dispatch:
            resp = MockJSResponse()
            mock_dispatch.return_value = br.RestLikeResponse(resp, is_js=True)
            result = await self.client.fetch(self.mock_request)
            self.assertIsInstance(result, br.RestResponse)

    async def test_stream_saves_file(self):
        mock_resp = MockJSResponse(content=b"hello world")
        with patch(
            f"{__name__}.br._dispatch_fetch",
            return_value=br.RestLikeResponse(mock_resp, is_js=True),
        ):
            path = "/tmp/test_stream.txt"
            result = await self.client.stream("https://example.com/data", path)
            with open(result) as f:
                content = f.read()
            self.assertIn("hello world", content)


if __name__ == "__main__":
    unittest.main()
