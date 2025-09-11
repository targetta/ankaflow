# TODO: Fix pylance warnings
# type: ignore
import typing as t
import httpx
from asyncio import sleep
import logging
import jmespath
from pathlib import Path


from . import common

# from ...models import models as m
from ...models import rest as rst
from ...models import enums
from ...common.util import print_error

log = logging.getLogger(__name__)


class HeaderAuth(httpx.Auth):
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def auth_flow(self, request):
        for k in self.kwargs:
            request.headers[k] = self.kwargs[k]
        yield request


class Oauth2Auth(httpx.Auth):
    def __init__(self, token: str = None):
        self.token = token

    def auth_flow(self, request):
        request.headers["Authorization"] = f"Bearer {self.token}"
        yield request


class RestResponse:
    def __init__(self, httpresponse: httpx.Response):
        self.resp = httpresponse

    async def json(self):
        return self.resp.json()

    async def text(self):
        return self.resp.text


class RestClient:
    """
    A REST client for making HTTP requests with retry and error handling.
    """

    def __init__(
        self, clientconfig: rst.RestClientConfig, logger: logging.Logger = None
    ):
        self.config: rst.RestClientConfig = clientconfig
        self.base_url: str = clientconfig.base_url
        self._auth: httpx.Auth = None
        self.client: httpx.Client = None
        self.request: rst.Request = None
        self.wait = 1  # Initial wait for replayable errors
        self.retry = 3  # Retry on 500+ errors
        self.log = logger or logging.getLogger(__name__)
        if not logger:
            self.log.addHandler(logging.NullHandler())

    def __repr__(self):
        return f"RestClient {self.base_url}"

    def __enter__(self):
        return self.connect()

    def __exit__(self, *args):
        self.disconnect()

    def connect(self) -> "RestClient":
        """Connect to the REST client."""
        self.client = httpx.Client(
            auth=self.auth(), timeout=self.config.timeout
        )
        return self

    def disconnect(self):
        """Disconnect the REST client."""
        if self.client:
            self.client.close()

    def auth(self) -> httpx.Auth:
        """Get the authentication handler."""
        if not self._auth:
            self._auth = self.get_auth()
        return self._auth

    def get_auth(self) -> httpx.Auth:
        """Create the authentication handler based on configuration."""
        if not self.config.auth:
            return None
        vars = self.config.auth.values or {}
        auth_methods = {
            enums.AuthType.BASIC: httpx.BasicAuth,
            enums.AuthType.DIGEST: httpx.DigestAuth,
            enums.AuthType.HEADER: HeaderAuth,
            enums.AuthType.OAUTH2: Oauth2Auth,
        }
        return auth_methods.get(self.config.auth.method, lambda **_: None)(
            **vars
        )

    @property
    def closed(self):
        """Check if the client is closed."""
        return self.client.is_closed

    @property
    def url(self):
        """Build the full URL for the request."""
        path = self.request.endpoint
        try:
            return common.get_url(self.base_url, path)
        except Exception as e:
            self.log.warning(
                print_error(f"HTTP Transport error {e}", self.base_url, path)
            )
            raise

    def headers(self):
        """Get the request headers."""
        return {"content-type": self.request.content_type.value}

    def params(self):
        """Get the request query parameters."""
        return self.request.query

    def payload(self):
        """Get the request payload."""
        return (
            self.request.body
            if self.request and self.request.body is not None
            else None
        )

    def arguments(self):
        """Prepare the arguments for the HTTP request."""
        args = {"params": self.params(), "headers": self.headers()}
        if self.request.method != enums.RequestMethod.GET:
            if self.request.content_type == enums.ContentType.FORM:
                args["data"] = self.payload()
            elif self.request.content_type == enums.ContentType.JSON:
                args["json"] = self.payload()
        return args

    async def handle_response(self, response: httpx.Response):
        """Handle the HTTP response."""
        url = response.request.url
        request_body = self.payload()
        response_body = getattr(response, "text", None)

        if response.status_code == 429:
            self.log.debug(
                print_error(f"Request limit reached, waiting {self.wait}")
            )
            retried_response = await self._retry_request()
            return retried_response.resp  # unwrapped
        elif response.status_code >= 500:
            return await self._handle_retryable_error(
                response, url, request_body, response_body
            )
        elif response.status_code >= 400:
            self._handle_client_error(
                response, url, request_body, response_body
            )
        elif self.request.errorhandler.condition:
            self._handle_custom_error(
                response, url, request_body, response_body
            )
        return response

    async def _retry_request(self):
        """Retry the request with exponential backoff."""
        await sleep(self.wait)
        response = await self.fetch(self.request)
        self.wait *= 2
        return response

    async def _handle_retryable_error(
        self, response: httpx.Response, url, request_body, response_body
    ):
        """Handle retryable errors (500+)."""
        if not self.retry:
            self.log.warning(
                print_error(
                    f"Error {response.status_code}",
                    url,
                    response_body,
                    request_body,
                )
            )
            raise common.RestRequestError(
                print_error(f"Error {response.status_code}", url, response_body)
            )
        await sleep(self.wait)
        self.retry -= 1
        self.wait += 1
        retried_response = await self.fetch(self.request)
        return retried_response.resp

    def _handle_client_error(self, response, url, request_body, response_body):
        """Handle client errors (400+)."""
        self.log.warning(
            print_error(
                f"Error {response.status_code}",
                url,
                response_body,
                request_body,
            )
        )
        raise common.RestRequestError(
            print_error(f"Error {response.status_code}", url, response_body)
        )

    def _handle_custom_error(self, response, url, request_body, response_body):
        """Handle custom errors defined in the request error handler."""
        resp = response.json()
        if jmespath.search(self.request.errorhandler.condition, resp):
            msg = (
                jmespath.search(self.request.errorhandler.message, resp) or resp
            )
            self.log.warning(
                print_error(
                    f"Error {response.status_code}",
                    url,
                    response_body,
                    request_body,
                )
            )
            raise common.RestRequestError(
                print_error(f"Error in {url}", msg, request_body)
            )

    async def fetch(self, request: rst.Request) -> RestResponse:
        self.request = request
        args = self.arguments()
        method = getattr(self.client, request.method.value)

        attempt = 0
        try:
            while True:
                try:
                    response = method(self.url, **args)
                    response = await self.handle_response(response)
                    return RestResponse(response)
                except httpx.TransportError as e:
                    attempt += 1
                    if attempt > request.max_retries:
                        self.log.warning(
                            print_error(
                                f"HTTP Transport error {e}",
                                self.url,
                                self.request.query,
                            )
                        )
                        raise common.RestRequestError(
                            print_error(
                                f"HTTP Transport error {e}",
                                self.url,
                                self.request.query,
                            )
                        )
                    delay = request.initial_backoff * (2 ** (attempt - 1))
                    self.log.debug(
                        f"Retrying after transport error ({attempt}/{request.max_retries}), "  # noqa: E501
                        f"waiting {delay:.1f}s"
                    )
                    await sleep(delay)
        finally:
            self.request = None

    async def stream(self, url: str, destination: t.Union[str, Path]):
        """Stream data from the REST API to a file."""
        with self.client.stream("GET", url) as response:
            if response.status_code >= 400:
                raise common.RestRequestError(
                    print_error(f"Read failed for {url}")
                )
            with open(destination, "w", encoding="utf8") as file:
                for chunk in response.iter_text():
                    file.write(chunk)
        return destination
