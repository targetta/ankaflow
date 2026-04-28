# TODO: Fix pylance warnings
# type: ignore
import typing as t
import time
import httpx
from asyncio import sleep
import logging
import jmespath
from pathlib import Path


from . import common

# from ...models import models as m
from ...models import rest as rst
from ...models import enums
from ...models import OAuth2Provider
from ...common.util import print_error

log = logging.getLogger(__name__)


class BasicAuthShim(httpx.BasicAuth):
    def __init__(self, **kwargs):
        # BasicAuth expects (username, password)
        # We assume your 'vars' from YAML has these keys
        super().__init__(
            username=kwargs.get("username"), password=kwargs.get("password")
        )


class DigestAuthShim(httpx.DigestAuth):
    def __init__(self, **kwargs):
        super().__init__(
            username=kwargs.get("username"), password=kwargs.get("password")
        )


class HeaderAuth(httpx.Auth):
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def auth_flow(self, request):
        for k in self.kwargs:
            request.headers[k] = self.kwargs[k]
        yield request


class BearerAuth(httpx.Auth):
    def __init__(self, token: str = None, **kwargs):
        self.token = token

    def auth_flow(self, request):
        request.headers["Authorization"] = f"Bearer {self.token}"
        yield request


class OAuth2Auth(httpx.Auth):
    def __init__(
        self,
        provider: str | OAuth2Provider | None = None,
        _oauth: t.List[OAuth2Provider] | None = None,
        oauth_header: str | None = None,
    ):
        self._keyring = {p.name: p for p in (_oauth or [])}
        self.provider_val = provider
        self.header = oauth_header

    def _resolve_provider(self) -> OAuth2Provider:
        if isinstance(self.provider_val, OAuth2Provider):
            # Already an object (Inline definition)
            return self.provider_val

        # It's a string, look it up in the registry/keyring
        resolved = self._keyring.get(self.provider_val)
        if not resolved:
            raise ValueError(
                f"Provider '{self.provider_val}' not found in keyring."
            )
        return resolved

    @property
    def current(self) -> OAuth2Provider:
        """Resolution of the specific provider from the keyring."""
        return self._resolve_provider()

    def _set_headers(self, request: httpx.Request):
        # Do not set headers if AT missing
        if not self.current.access_token:
            return request
        if self.header:
            request.headers[self.header] = self.current.access_token
        else:
            request.headers["Authorization"] = (
                f"Bearer {self.current.access_token}"
            )
        return request

    def auth_flow(
        self, request: httpx.Request
    ) -> t.Generator[httpx.Request, httpx.Response, None]:
        # 1. Ensure we have a token to start with
        if not self.current.access_token:
            self._refresh_and_update()
        
        # 2. Initial Attempt
        response = yield self._set_headers(request)

        # 3. If we get a 401, the token expired during the flow
        if response.status_code == 401:
            # Re-fetch (this updates refresh_token in the model too)
            self._refresh_and_update()
            
            # 4. Update headers with the NEW token and yield again
            yield self._set_headers(request)

    def _refresh_and_update(self):
        try:
            new_data = self._fetch_new_tokens()

            # Update state
            self.current.access_token = new_data["access_token"]
            if "refresh_token" in new_data:
                self.current.refresh_token = new_data["refresh_token"]

            if self.current.on_token_refresh:
                self.current.on_token_refresh(self.current.name, new_data)

        except httpx.HTTPStatusError as exc:
            self.current.access_token = None # Kill switch
            # Providers sometimes return html
            try:
                error_payload = exc.response.json()
                # Most OAuth2 providers use the "error" key per RFC 6749
                error_msg = error_payload.get("error", "unknown_error")
                body = error_payload
            except Exception:
                error_msg = "non_json_response"
                body = exc.response.text[:200]

            error_data = {
                "error": error_msg,
                "status_code": exc.response.status_code,
                "body": body,
            }
            # Trigger failure callback if provided
            if self.current.on_refresh_fail:
                self.current.on_refresh_fail(self.current.name, error_data)

            # Re-raise so the pipeline stops and doesn't retry indefinitely
            raise

    def _fetch_new_tokens(self) -> dict:
        """Determines the correct RFC anatomy and calls the token endpoint."""
        p = self.current
        conf = p.config
        # Standard Refresh Flow
        if p.refresh_token:
            payload = {
                "grant_type": "refresh_token",
                "refresh_token": p.refresh_token,
                "client_id": conf.client_id,
                "client_secret": conf.client_secret,
            }
        # Token Exchange Flow (RFC 8693)
        else:
            payload = {
                "grant_type": conf.grant_type,
                "subject_token": p.subject_token or conf.subject_token,
                "subject_token_type": conf.subject_token_type,
                "requested_token_type": conf.requested_token_type,
                "client_id": conf.client_id,
                "client_secret": conf.client_secret,
            }

        if self.current.config.extra_params:
            payload.update(self.current.config.extra_params)

        TRANSIENT_STATUSES = {429, 500, 502, 503, 504}
        MAX_RETRIES = 3

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded",
        }

        for attempt in range(MAX_RETRIES):
            try:
                resp = httpx.post(
                    conf.access_token_url,
                    data=payload,
                    timeout=10.0,  # Don't let auth hang forever
                    headers=headers
                )

                if (
                    resp.status_code in TRANSIENT_STATUSES
                    and attempt < MAX_RETRIES - 1
                ):
                    time.sleep(2 ** (attempt + 1))  # Exponential backoff
                    continue

                resp.raise_for_status()
                return resp.json()

            except (httpx.TimeoutException, httpx.NetworkError):
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2 ** (attempt + 1))
                    continue
                raise


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
        self,
        clientconfig: rst.RestClientConfig,
        logger: logging.Logger = None,
        oauth_keyring: t.Optional[t.List[OAuth2Provider]] = None,
    ):
        self._oauth = oauth_keyring
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
            enums.AuthType.BASIC: BasicAuthShim,
            enums.AuthType.DIGEST: DigestAuthShim,
            enums.AuthType.HEADER: HeaderAuth,
            enums.AuthType.BEARER: BearerAuth,
            enums.AuthType.OAUTH2: OAuth2Auth,
        }
        method = auth_methods.get(self.config.auth.method)

        if not method:
            raise ValueError("Invalid Auth value")

        if self.config.auth.method == enums.AuthType.OAUTH2:
            return OAuth2Auth(
                provider=self.config.auth.provider,
                _oauth=self._oauth,
                oauth_header=self.config.auth.oauth_header
            )

        return method(**vars)

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
