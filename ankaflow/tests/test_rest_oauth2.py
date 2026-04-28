import unittest
from unittest.mock import MagicMock, patch
import httpx
import logging

from ..connections.rest.server import RestClient, OAuth2Auth
from ..models import rest as rst
from ..models import enums


class TestRestOAuth2(unittest.TestCase):
    def setUp(self):
        self.mock_config = MagicMock(spec=rst.RestClientConfig)
        self.mock_config.base_url = "http://test.com"
        self.mock_config.timeout = 10
        self.mock_logger = MagicMock(spec=logging.Logger)

        # Define a test provider
        self.test_provider = rst.OAuth2Provider(
            name="google_test",
            config=rst.OAuth2Config(
                authorize_url="https://auth.com",
                access_token_url="https://token.com",
                client_id="id",
                client_secret="secret",
            ),
            access_token="old_token",
            refresh_token="refresh_me",
        )
        self.oauth_list = [self.test_provider]
        self.rest_client = RestClient(
            self.mock_config, self.mock_logger, oauth_keyring=self.oauth_list
        )

    def test_get_auth_oauth2_resolution(self):
        self.mock_config.auth = rst.RestAuth(
            method=enums.AuthType.OAUTH2, provider="google_test"
        )
        # Passing the keyring into the client factory
        auth = self.rest_client.get_auth()

        self.assertIsInstance(auth, OAuth2Auth)

        oauth_auth: OAuth2Auth = auth  # type: ignore

        self.assertEqual(oauth_auth.provider_val, "google_test")
        self.assertEqual(oauth_auth.current.access_token, "old_token")

    @patch("httpx.post")
    def test_oauth2_refresh_success(self, mock_post):
        # 1. Setup Mock Refresh Response
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {
                "access_token": "new_token",
                "refresh_token": "new_refresh",
            },
        )

        # 2. Setup Callback Spy
        refresh_spy = MagicMock()
        self.test_provider.on_token_refresh = refresh_spy

        auth = OAuth2Auth(provider="google_test", _oauth=self.oauth_list)

        # 3. Simulate the httpx flow
        # We simulate a 401 response from the server
        mock_request = httpx.Request("GET", "https://api.com")
        mock_response_401 = httpx.Response(401, request=mock_request)

        # auth_flow is a generator
        generator = auth.auth_flow(mock_request)
        next(generator)  # Yields original request

        try:
            generator.send(mock_response_401)
        except StopIteration:
            pass  # Generator finished after retry

        # 4. Assertions
        self.assertEqual(self.test_provider.access_token, "new_token")
        refresh_spy.assert_called_once_with(
            "google_test",
            {"access_token": "new_token", "refresh_token": "new_refresh"},
        )

    @patch("httpx.post")
    def test_oauth2_refresh_failure_callback(self, mock_post):
        # 1. Mock a 400 Bad Request from the token endpoint
        mock_post.return_value = MagicMock(
            status_code=400, json=lambda: {"error": "invalid_grant"}
        )
        # Make raise_for_status actually raise
        req = MagicMock()
        mock_post.return_value.raise_for_status.side_effect = (
            httpx.HTTPStatusError(
                "Error", request=req, response=mock_post.return_value
            )
        )

        fail_spy = MagicMock()
        self.test_provider.on_refresh_fail = fail_spy

        auth = OAuth2Auth(provider="google_test", _oauth=self.oauth_list)

        # 2. Trigger refresh
        with self.assertRaises(httpx.HTTPStatusError):
            auth._refresh_and_update()

        # 3. Verify fail callback
        fail_spy.assert_called_once()
        self.assertEqual(fail_spy.call_args[0][0], "google_test")

    def test_oauth2_custom_header(self):
        auth = OAuth2Auth(
            provider="google_test",
            oauth_header="X-Custom-Header",
            _oauth=self.oauth_list,
        )
        request = httpx.Request("GET", "https://example.com")

        # Manually trigger the header application
        auth._set_headers(request)

        self.assertEqual(request.headers["X-Custom-Header"], "old_token")
        self.assertNotIn("Authorization", request.headers)
