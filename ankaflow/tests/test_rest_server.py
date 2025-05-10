import unittest
from unittest.mock import MagicMock, patch, AsyncMock
import httpx
import logging

from ..connections.rest.server import (
    RestClient,
    common,
    RestResponse,
    HeaderAuth,
    Oauth2Auth,
)
from ..models import rest as rst
from ..models import enums
from ..common.types import StringDict


# Helper to create a dummy request with the required attributes.
def create_dummy_request(
    method=enums.RequestMethod.GET,
    content_type=enums.ContentType.JSON,
    query=None,
    body=None,
    endpoint="/",
    errorhandler=None,
):
    req = MagicMock(spec=rst.Request)
    req.method = method
    req.content_type = content_type
    req.query = query if query is not None else {}
    req.body = body
    req.endpoint = endpoint
    # If no errorhandler is provided, use a dummy one.
    if errorhandler is None:
        dummy_errorhandler = MagicMock()
        dummy_errorhandler.condition = ""
        dummy_errorhandler.message = ""
        req.errorhandler = dummy_errorhandler
    else:
        req.errorhandler = errorhandler
    return req


class TestRestClient(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.mock_config = MagicMock(spec=rst.RestClientConfig)
        self.mock_config.base_url = "http://test.com"
        self.mock_config.timeout = 10
        # Set auth attribute to avoid AttributeError in connect/disconnect.
        self.mock_config.auth = None
        self.mock_logger = MagicMock(spec=logging.Logger)
        self.rest_client = RestClient(self.mock_config, self.mock_logger)

    def test_connect(self):
        # connect() is synchronous.
        self.rest_client.connect()
        self.assertIsInstance(self.rest_client.client, httpx.Client)

    def test_disconnect(self):
        self.rest_client.connect()
        self.rest_client.disconnect()
        self.assertTrue(self.rest_client.closed)

    def test_auth_cached(self):
        mock_auth = MagicMock(spec=httpx.Auth)
        self.rest_client._auth = mock_auth
        auth = self.rest_client.auth()
        self.assertEqual(auth, mock_auth)

    def test_auth_not_cached(self):
        mock_auth = MagicMock(spec=httpx.Auth)
        self.rest_client.get_auth = MagicMock(return_value=mock_auth)
        auth = self.rest_client.auth()
        self.assertEqual(auth, mock_auth)

    def test_get_auth_no_auth(self):
        self.mock_config.auth = None
        auth = self.rest_client.get_auth()
        self.assertIsNone(auth)

    def test_get_auth_basic(self):
        self.mock_config.auth = rst.RestAuth(
            method=enums.AuthType.BASIC,
            values=StringDict({"username": "test", "password": "password"}),
        )
        auth = self.rest_client.get_auth()
        self.assertIsInstance(auth, httpx.BasicAuth)

    def test_get_auth_digest(self):
        self.mock_config.auth = rst.RestAuth(
            method=enums.AuthType.DIGEST,
            values=StringDict({"username": "test", "password": "password"}),
        )
        auth = self.rest_client.get_auth()
        self.assertIsInstance(auth, httpx.DigestAuth)

    def test_get_auth_header(self):
        self.mock_config.auth = rst.RestAuth(
            method=enums.AuthType.HEADER,
            values=StringDict({"Authorization": "Bearer token"}),
        )
        auth = self.rest_client.get_auth()
        self.assertIsInstance(auth, HeaderAuth)

    def test_get_auth_oauth2(self):
        self.mock_config.auth = rst.RestAuth(
            method=enums.AuthType.OAUTH2, values=StringDict({"token": "token"})
        )
        auth = self.rest_client.get_auth()
        self.assertIsInstance(auth, Oauth2Auth)

    def test_headers(self):
        req = create_dummy_request(content_type=enums.ContentType.JSON)
        self.rest_client.request = req
        headers = self.rest_client.headers()
        self.assertEqual(
            headers, {"content-type": enums.ContentType.JSON.value}
        )

    def test_params(self):
        req = create_dummy_request(query={"key": "value"})
        self.rest_client.request = req
        params = self.rest_client.params()
        self.assertEqual(params, {"key": "value"})

    def test_payload(self):
        req = create_dummy_request(body={"data": "test"})
        self.rest_client.request = req
        payload = self.rest_client.payload()
        self.assertEqual(payload, {"data": "test"})

    def test_arguments_get(self):
        req = create_dummy_request(
            method=enums.RequestMethod.GET,
            query={"key": "value"},
            content_type=enums.ContentType.JSON,
        )
        self.rest_client.request = req
        args = self.rest_client.arguments()
        self.assertEqual(
            args,
            {
                "params": {"key": "value"},
                "headers": {"content-type": enums.ContentType.JSON.value},
            },
        )

    def test_arguments_get_with_content_type(self):
        req = create_dummy_request(
            method=enums.RequestMethod.GET,
            query={"key": "value"},
            content_type=enums.ContentType.JSON,
        )
        self.rest_client.request = req
        args = self.rest_client.arguments()
        self.assertEqual(
            args,
            {
                "params": {"key": "value"},
                "headers": {"content-type": enums.ContentType.JSON.value},
            },
        )

    def test_arguments_post_form(self):
        req = create_dummy_request(
            method=enums.RequestMethod.POST,
            content_type=enums.ContentType.FORM,
            body={"data": "test"},
            query={},
        )
        self.rest_client.request = req
        args = self.rest_client.arguments()
        self.assertEqual(
            args,
            {
                "params": {},
                "headers": {"content-type": enums.ContentType.FORM.value},
                "data": {"data": "test"},
            },
        )

    def test_arguments_post_json(self):
        req = create_dummy_request(
            method=enums.RequestMethod.POST,
            content_type=enums.ContentType.JSON,
            body={"data": "test"},
            query={},
        )
        self.rest_client.request = req
        args = self.rest_client.arguments()
        self.assertEqual(
            args,
            {
                "params": {},
                "headers": {"content-type": enums.ContentType.JSON.value},
                "json": {"data": "test"},
            },
        )

    async def test_handle_response_200(self):
        req = create_dummy_request()
        self.rest_client.request = req
        response = httpx.Response(
            200, request=httpx.Request("GET", "http://test.com")
        )
        result = await self.rest_client.handle_response(response)
        self.assertEqual(result, response)

    async def test_handle_response_429(self):
        req = create_dummy_request(body={"data": "test"})
        self.rest_client.request = req
        response = httpx.Response(
            429, request=httpx.Request("GET", "http://test.com")
        )

        # Patch fetch as an async function.
        async def fake_fetch(r):
            return RestResponse(response)

        with patch.object(
            self.rest_client, "fetch", new=AsyncMock(side_effect=fake_fetch)
        ) as mock_fetch:
            result = await self.rest_client.handle_response(response)
            mock_fetch.assert_called_once_with(req)
            self.assertEqual(result, response)

    async def test_handle_response_500(self):
        req = create_dummy_request(body={"data": "test"})
        self.rest_client.request = req
        response = httpx.Response(
            500, request=httpx.Request("GET", "http://test.com")
        )

        async def fake_fetch(r):
            return RestResponse(response)

        with patch.object(
            self.rest_client, "fetch", new=AsyncMock(side_effect=fake_fetch)
        ) as mock_fetch:
            result = await self.rest_client.handle_response(response)
            mock_fetch.assert_called_once_with(req)
            self.assertEqual(result, response)

    async def test_handle_response_500_no_retry(self):
        req = create_dummy_request(body={"data": "test"})
        self.rest_client.request = req
        self.rest_client.retry = 0  # Disable retries.
        response = httpx.Response(
            500, request=httpx.Request("GET", "http://test.com")
        )
        with self.assertRaises(common.RestRequestError):
            await self.rest_client.handle_response(response)

    async def test_handle_response_400(self):
        req = create_dummy_request(body={"data": "test"})
        self.rest_client.request = req
        response = httpx.Response(
            400, request=httpx.Request("GET", "http://test.com")
        )
        with self.assertRaises(common.RestRequestError):
            await self.rest_client.handle_response(response)

    async def test_handle_response_error_condition(self):
        # Setup an errorhandler in the request.
        error_handler = rst.RestErrorHandler(
            condition="status == 'error'", message="message"
        )
        req = create_dummy_request(
            body={"data": "test"}, errorhandler=error_handler
        )
        self.rest_client.request = req
        response_json = {"status": "error", "message": "Test Error"}
        response = httpx.Response(
            200,
            request=httpx.Request("GET", "http://test.com"),
            json=response_json,
        )
        with self.assertRaises(common.RestRequestError):
            await self.rest_client.handle_response(response)


if __name__ == "__main__":
    unittest.main()
