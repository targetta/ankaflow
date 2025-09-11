import typing as t
import jmespath
import json
import urllib.parse
from asyncio import sleep
import logging

from ...models import rest as rst
from ...models import enums as enums
from . import common
from ...common.util import print_error

# Always import for patching support and editor hints
from pyodide.http import pyfetch  # type: ignore

# Default to false — will override if js available
JS_FETCH_AVAILABLE = False
get_http_response: t.Optional[t.Callable] = None
# Runtime check for JS API
if not t.TYPE_CHECKING:
    try:
        import js

        get_http_response = js.getHTTPResponse
        JS_FETCH_AVAILABLE = True
    except Exception:
        pass  # Use fallback to pyfetch


class RestLikeResponse:
    """
    Wraps JS or pyfetch responses into a unified interface for status,
    content, and methods.
    JS implementation should throw exceptions for the handler to deal with.

    Attributes:
        _resp: The raw response object.
        _is_js: Indicates if the response is from JS (True) or pyfetch (False).
    """

    def __init__(self, resp: t.Any, is_js: bool = True) -> None:
        self._resp = resp
        self._is_js = is_js

    @property
    def ok(self) -> bool:
        """Returns True if the response status is 2xx."""
        return (
            self._resp.ok
            if self._is_js
            else self._resp.status in range(200, 300)
        )

    @property
    def status(self) -> int:
        """Returns the HTTP status code."""
        return self._resp.status

    @property
    def url(self) -> str:
        """Returns the request URL."""
        return self._resp.url

    @property
    def encoding(self) -> str:
        """Returns the encoding used, defaults to 'utf-8'."""
        return getattr(self._resp, "encoding", "utf-8")

    async def json(self) -> t.Any:
        """Parses and returns JSON content."""
        return await self._resp.json()

    async def text(self) -> str:
        """Returns the response content as text."""
        return await self._resp.text()

    async def bytes(self) -> bytes:
        """Returns the response content as bytes."""
        return await self._resp.bytes()


async def _dispatch_fetch(
    url: str, args: dict, args_json: str
) -> RestLikeResponse:
    """
    Dispatches the fetch call to either JS getHTTPResponse or pyodide pyfetch.

    Args:
        url: The request URL.
        args: The normalized fetch arguments.
        args_json: JSON-encoded string for JS fetch.

    Returns:
        A RestLikeResponse wrapping the backend-specific result.
    """
    if JS_FETCH_AVAILABLE:  # type: ignore
        js_resp = await get_http_response(url, args_json)  # type: ignore
        return RestLikeResponse(js_resp, is_js=True)
    else:
        py_resp = await pyfetch(url, **args)
        return RestLikeResponse(py_resp, is_js=False)


class RestResponse:
    """
    Provides higher-level access to text or JSON from a RestLikeResponse.

    Attributes:
        resp: A wrapped response object from JS or pyfetch.
    """

    def __init__(self, httpresponse: RestLikeResponse) -> None:
        self.resp = httpresponse

    async def json(self) -> t.Any:
        """Returns parsed JSON from the response."""
        data = await self.resp.json()
        try:
            return json.loads(data)
        except json.JSONDecodeError:
            return data

    async def text(self) -> str:
        """Returns response as plain text."""
        return await self.resp.text()


class RestClient:
    """
    HTTP client that works in Pyodide with fallback
    support for native JS or pyfetch.

    Args:
        clientconfig: Pydantic client configuration model.
        logger: Optional custom logger instance.
    """

    def __init__(
        self,
        clientconfig: rst.RestClientConfig,
        logger: t.Optional[logging.Logger] = None,
    ) -> None:
        self.config: rst.RestClientConfig = clientconfig
        self.base_url: str = clientconfig.base_url
        self._request: t.Optional[rst.Request] = None
        self.wait: int = 1
        self.retry: int = 3
        self.log = logger or logging.getLogger()
        if not logger:
            self.log.addHandler(logging.NullHandler())
        self._headers: dict = {}
        self._closed: bool = True

    @property
    def request(self) -> rst.Request:
        """Returns the active request."""
        if self._request is None:
            raise RuntimeError("No active request")
        return self._request

    @request.setter
    def request(self, value: t.Optional[rst.Request]) -> None:
        """Sets the active request."""
        self._request = value

    def connect(self) -> "RestClient":
        """Returns self (stub)."""
        return self

    def disconnect(self) -> None:
        """Disconnects client (no-op)."""
        pass

    @property
    def closed(self) -> bool:
        """Indicates connection is always closed (no state)."""
        return True

    def get_auth(self) -> None:
        """Populates internal headers with authorization info."""
        if not self.config.auth:
            return None
        vars = self.config.auth.values or {}
        if self.config.auth.method == enums.AuthType.BASIC:
            self._headers["Authorization"] = (
                f"Basic {vars['username']}:{vars['password']}"
            )
        elif self.config.auth.method == enums.AuthType.HEADER:
            self._headers.update(vars)
        elif self.config.auth.method == enums.AuthType.OAUTH2:
            self._headers["Authorization"] = f"Bearer {vars['token']}"
        elif self.config.auth.method == enums.AuthType.DIGEST:
            raise NotImplementedError("Digest auth not supported")

    def url(self) -> str:
        """Constructs full request URL with query params."""
        path = self.request.endpoint
        url = common.get_url(self.base_url, path)
        if self.params():
            return f"{url}?{urllib.parse.urlencode(self.params())}"
        return url

    def headers(self) -> dict:
        """Returns headers with content-type and auth."""
        self.get_auth()
        self._headers["Content-Type"] = self.request.content_type.value
        return self._headers

    def params(self) -> dict:
        """Returns query params."""
        return self.request.query

    def payload(self) -> t.Any:
        """Returns request payload."""
        return self.request.body

    def arguments(self) -> dict:
        """Returns fetch-style arguments."""
        args = {
            "cache": "no-cache",
            "headers": self.headers(),
            "method": self.request.method.value,
        }
        if self.request.method != enums.RequestMethod.GET:
            if self.request.content_type == enums.ContentType.FORM:
                body = self.payload()
                args["body"] = (
                    urllib.parse.urlencode(body)
                    if isinstance(body, dict)
                    else body
                )
            elif self.request.content_type == enums.ContentType.JSON:
                args["body"] = json.dumps(self.payload())
        return args

    async def handle_response(
        self, response: RestLikeResponse
    ) -> RestLikeResponse:
        """
        Validates response status and raises typed errors if needed.

        Args:
            response: Wrapped fetch response.

        Returns:
            The same response object if valid.

        Raises:
            RestRequestError, RestRetryableError, RestRateLimitError on failure.
        """
        url = response.url
        if response.status == 429:
            self.log.warning(f"Rate limited (429) from {url}")
            raise common.RestRateLimitError(
                f"Rate limit exceeded: 429 for {url}"
            )
        elif response.status >= 500:
            text = await response.text()
            raise common.RestRetryableError(
                print_error("Server error", url, text)
            )
        elif response.status >= 400:
            text = await response.text()
            raise common.RestRequestError(
                print_error("Client error", url, text)
            )

        if self.request.errorhandler.condition:
            try:
                body = json.loads(await response.json())
            except Exception:
                raise common.RestRequestError(await response.text())
            if t.cast(
                bool, jmespath.search(self.request.errorhandler.condition, body)
            ):
                raise common.RestRequestError(
                    print_error("Validation failed", url, body)
                )

        if response.status in self.request.errorhandler.error_status_codes:
            raise common.RestRequestError(await response.text())

        return response

    async def fetch(self, request: rst.Request) -> RestResponse:
        """
        Executes the HTTP request with retry and backoff.

        Args:
            request: The request model.

        Returns:
            RestResponse: a unified wrapper.
        """
        self.request = request
        args = self.arguments()
        args_json = json.dumps(args)
        url = self.url()

        max_retries = request.max_retries
        wait = request.initial_backoff

        for attempt in range(max_retries + 1):
            try:
                resp = await _dispatch_fetch(url, args, args_json)
                await self.handle_response(resp)
                self.request = None
                return RestResponse(resp)
            except (common.RestRateLimitError, common.RestRetryableError):
                if attempt == max_retries:
                    raise
                await sleep(wait)
                wait *= 2
            except Exception as e:
                if attempt == max_retries:
                    raise common.RestRequestError(
                        print_error("Fatal error", url, e)
                    )
                await sleep(wait)
                wait *= 2

        raise RuntimeError("Unreachable: fetch retry loop exhausted")

    async def stream(self, url: str, destination: str) -> str:
        """
        Downloads file to destination path.

        Args:
            url: Source URL.
            destination: Target file path.

        Returns:
            Destination file path.

        Raises:
            RestRequestError if fetch fails.
        """
        args = {}
        args_json = json.dumps(args)
        resp = await _dispatch_fetch(url, args, args_json)

        if not resp.ok:
            raise common.RestRequestError(
                print_error(f"Download failed: {url}")
            )

        data = await resp.bytes()
        encoding = resp.encoding or "utf-8"

        try:
            text = bytes(data).decode(encoding)
            with open(destination, "w", encoding="utf-8") as fh:
                fh.write(text)
        except UnicodeDecodeError:
            with open(destination, "wb") as fh:
                fh.write(data)

        return destination
