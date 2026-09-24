from enum import Enum
import typing as t
from pydantic import (
    BaseModel,
    field_validator,
    Field as PydanticField,
    ConfigDict,
)

from .enums import (
    AuthType,
    ParameterDisposition,
    DataType,
    ContentType,
    RequestMethod,
)

from ..common.types import StringDict


RefreshSuccessCallable = t.Callable[[str, dict], None]
RefreshFailCallable = t.Callable[[str, dict], None]


class OAuth2Config(BaseModel):
    authorize_url: str
    access_token_url: str
    client_id: str
    client_secret: str

    # --- RFC 8693 Standard Exchange Fields ---
    grant_type: str = "urn:ietf:params:oauth:grant-type:token-exchange"
    """The grant type for the credential exchange (default: Token Exchange)."""
    subject_token_type: str = "urn:ietf:params:oauth:token-type:id_token"
    """The identifier for the type of security token being provided."""
    requested_token_type: str = "urn:ietf:params:oauth:token-type:access_token"
    """The type of security token being requested from the provider."""
    subject_token: str | None = None
    """A security token that represents the identity of the party
    on whose behalf the request is being made."""
    extra_params: dict = PydanticField(default_factory=dict)
    """
    Arbitrary key-value pairs for vendor-specific extensions 
    (e.g., {'resource': 'https://analysis.windows.net/powerbi/api'}).
    """


class OAuth2Provider(BaseModel):
    """
    Implements standard OAuth2 authorization and token exchange flows.

    Retry Policy: "The auth mechanism automatically retries transient errors
    (5xx, 429, and timeouts) up to 3 times before triggering on_refresh_fail."
    """

    name: str
    """Unique identifier used in YAML connection 'provider' field."""

    config: OAuth2Config

    access_token: str | None = None
    """Current valid access token (Bearer).
    In the event token exchange fails this will be set to None immediately.
    """
    refresh_token: str | None = None
    """Long-lived token used to obtain new access tokens."""
    subject_token: str | None = None
    """The session-specific identity token used for RFC 8693 exchanges."""

    on_token_refresh: t.Optional[RefreshSuccessCallable] = None
    """
    Optional callback triggered on successful token update. 
    (eg storing in secrets manager)

    Signature: (name: str, data: dict) -> None
    """
    on_refresh_fail: t.Optional[RefreshFailCallable] = None
    """
    Callback triggered when refresh fails (4xx/5xx or malformed response).
    (eg notifying UI or invalidating old tokens)
    
    Signature: (name: str, error_context: dict) -> None
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)


class RestAuth(BaseModel):
    """
    Authenctication configuration for Rest connection.

    NOTE: Not all authentication methods may not work
    in browser due to limitations in the network API.
    """

    method: AuthType
    """Specifies authentiation type."""

    provider: str | OAuth2Provider | None = None
    """
    Either a string (lookup key for the keyring) or an inline 
    OAuth2Provider definition.
    """
    oauth_header: str | None = None
    """ Some providers use different header pattern than Authorization-Bearer.
    If set then auth mechanism will user oauth_header=token
    """

    values: StringDict | dict = PydanticField(default_factory=dict)
    """Mapping of parameter names and values.
    
        {
            'X-Auth-Token': '<The Token>'
        }
    
    """

    model_config = {"arbitrary_types_allowed": True}

    @field_validator("values", mode="before")
    @classmethod
    def coerce_to_stringdict(cls, v):
        """Rest header values must be strings.
        This convenience validator  automatically
        converts regiular dictionary to StringDict.
        """
        if isinstance(v, StringDict):
            return v
        if isinstance(v, dict):
            return StringDict(v)
        raise TypeError("Expected a StringDict or a dict for `values`")


class RestClientConfig(BaseModel):
    """
    Rest client for given base URL.
    Includes transport and authentication
    configuration
    """

    base_url: str
    """
    Base URL, typically server or API root.
    All endpoints with the same base URL share the
    same authentication.

    Example: `https://api.example.com/v1`
    """
    transport: t.Optional[str] = None
    timeout: t.Optional[float] = None
    """
    Request timeout in seconds. Default is 5.
    Set 0 to disable timout.
    """
    auth: t.Optional[RestAuth] = None


class RestErrorHandler(BaseModel):
    error_status_codes: t.List[int] = []
    """
    List of HTTP status codes to be treated as errors.
    """
    condition: t.Optional[str] = None
    """
    JMESPath expression to look for in the response body.
    Error will be generated if expression evaluates to True
    """
    message: t.Optional[str] = None
    """
    JMESPath expression to extract error message from respose.
    If omitted entire response will be included in error.
    """


class ResponseHandlerTypes:
    """"""

    BASIC = "Basic"
    PAGINATOR = "Pagination"
    URLPOLLING = "URLPolling"
    STATEPOLLING = "StatePolling"
    CURSOR_PAGINATOR = "CursorPagination"


class BasicHandler(BaseModel):
    """
    A no-op response handler used when no special processing
    (like pagination or transformation) is required.

    Typically used for single-response REST endpoints where the
    entire payload is returned in one request.

    Attributes:
        kind (Literal): Specifies the handler type as BASIC.
    """

    kind: t.Literal[ResponseHandlerTypes.BASIC]  # type: ignore
    """Specifies the handler type as Basic."""


class Paginator(BaseModel):
    """
    A response handler for paginated REST APIs.

    This handler generates repeated requests by incrementing a
    page-related parameter until no more data is available.
    The stopping condition is usually inferred from the number
    of records in the response being less than `page_size`, or
    from a total record count field.
    """

    kind: t.Literal[ResponseHandlerTypes.PAGINATOR]  # type: ignore
    """Specifies the handler type as Paginator."""
    page_param: str
    """
    Page parameter in the request (query or body)
    This will be incremented from request to request
    """
    page_size: int
    """
    Page size should be explicitly defined. If response contains
    less records it is considered to be last page
    """
    param_locator: ParameterDisposition
    """
    Define where the parameter is located: body or query
    """
    total_records: t.Optional[str] = None
    """
    JMESPath to total records count in the response.
    """
    increment: int
    """
    Page parameter increment. Original request configuration
    should include initial value e.g. `page_no=1`
    """
    throttle: t.Optional[t.Union[int, float]] = None
    """
    If set to positive value then each page request is throttled
    given number of seconds.

    Useful when dealing with rate limits or otherwise spreading the load
    over time.
    """


class URLPoller(BaseModel):
    """
    URL Poller makes request(s) to remote API
    until an URL is returned
    """

    kind: t.Literal[ResponseHandlerTypes.URLPOLLING]  # type: ignore
    """Specifies the handler type as URLPolling."""
    ready_status: t.Optional[str] = None
    """
    JMESPath to read status from response. If the value at path
    evaluates to True then no more requests are made, and
    the API tries to read data from URL specified by `locator`.
    """


class StatePoller(BaseModel):
    """
    A response handler for state-based polling APIs.

    This handler is designed for asynchronous workflows where the
    client repeatedly polls an endpoint until a certain state is reached
    (e.g., job completion, resource readiness). Once the condition is met,
    the pipeline continues by reading from the final data `locator`.
    """

    kind: t.Literal[ResponseHandlerTypes.STATEPOLLING]  # type: ignore
    """Specifies the handler type as StatePolling."""
    ready_status: str
    """
    JMESPath to read status from response. If the value at path
    evaluates to True then no more requests are made, and
    the API tries to read data from URL specified by `locator`.
    """


class CursorResponseLocator(str, Enum):
    """Specifies where and how to extract
    the cursor from an incoming response."""

    BODY = "body"
    """Extract cursor from JSON response body using a JMESPath expression."""

    HEADER = "header"
    """Read cursor directly from an HTTP response header."""

    RFC_TOKEN = "rfc_token"
    """Extract a named query parameter from an RFC 8288 Link header."""

    RFC_URL = "rfc_url"
    """Extract full next URL/path from an RFC 8288 Link header."""


class CursorRequestDisposition(str, Enum):
    """Specifies where and how to inject the
    pagination cursor into the next request."""

    QUERY = "query"
    """Inject cursor as a URL query parameter (e.g., ?cursor=xyz123)."""

    BODY = "body"
    """Inject cursor into the JSON request body. Supports dot-notation paths
    for nested fields (e.g., 'variables.cursor').
    """


class CursorPaginator(BaseModel):
    """
    A response handler for cursor-based paginated REST and GraphQL APIs.

    Generates repeated requests by extracting a cursor token or URL from the response
    and injecting it into subsequent requests until an empty/null cursor or end-sentinel is met.
    """  # noqa: E501

    kind: t.Literal[ResponseHandlerTypes.CURSOR_PAGINATOR]  # type: ignore

    # --- Response Extraction ---
    cursor_locator: CursorResponseLocator = CursorResponseLocator.BODY
    """Where and how to locate the cursor in the response."""

    cursor_param: t.Optional[str] = None
    """
    Field or path name to extract from response:


    - JMESPath expression if cursor_locator='body' (e.g., 'nextCursor', 'data.orders.pageInfo.endCursor')
    - Header name if cursor_locator='header' (e.g., 'X-Next-Cursor')
    - Query parameter name if cursor_locator='rfc_token'
    - Ignored if cursor_locator='rfc_url'
    """  # noqa: E501

    # --- Request Injection ---
    cursor_disposition: CursorRequestDisposition = (
        CursorRequestDisposition.QUERY
    )  # noqa: E501
    """Where to put the extracted cursor in
    the next request (QUERY, BODY, HEADER)."""

    cursor_name: t.Optional[str] = None
    """
    Parameter name or path in the next request (e.g., 'cursor', 'start_after').
    Supports dot-notation paths for nested dicts
    when cursor_disposition='body' (e.g., 'variables.cursor').
    """

    # --- Termination Controls ---
    stop_value: t.Optional[t.Any] = None
    """Optional explicit value signalling end of pagination (e.g., 'END')."""

    throttle: t.Optional[t.Union[int, float]] = None
    """Optional pause duration in seconds between page requests."""


class RestResponse(BaseModel):
    """
    Response configuration. Response can be paged,
    polled URL or in body.
    """

    handler: t.Union[
        BasicHandler, Paginator, URLPoller, StatePoller, CursorPaginator, None
    ] = PydanticField(None, discriminator="kind")  # noqa:E501

    content_type: DataType
    """
    Returned data type
    """
    locator: t.Optional[str] = None
    """
    JMESPath to read data from JSON body.
    If not set then entire body is treated as data.
    """


class Request(BaseModel):
    endpoint: str
    """
    Request endpoint e.g. `get/data` under base url:

    Example `https://api.example.com/v1` + `get/data`
    """
    method: RequestMethod
    """
    Request method e.g. `post,get,put`
    """
    content_type: ContentType = ContentType.JSON
    """
    Request content type
    """
    query: t.Dict = {}  # string to compile to json query params
    """
    Query parameters. Parameters may contain template variables.
    """
    body: t.Optional[t.Union[str, t.Dict]] = None
    """
    Request body parameters.

    This field accepts either:
    - A Python `dict` representing a direct key-value mapping, or
    - A Jinja-templated JSON string with magic `@json` prefix, e.g.:
    `@json{"parameter": "value"}`

    The template will be rendered using the following custom delimiters:
    - `<< ... >>` for variable interpolation
    - `<% ... %>` for logic/control flow (e.g., for-loops)
    - `<# ... #>` for inline comments

    The template will be rendered before being parsed into a valid JSON object.
    This allows the use of dynamic expressions, filters, and control flow
    such as loops.

    ### Example with looping

    Given:
    ```python
    variables = {
        "MyList": [
            {"id": 1, "value": 10},
            {"id": 2, "value": 20}
        ]
    }
    ```
    You can generate a dynamic body with:
    ```
    body: >
    @json[
        <% for row in API.look("MyTable", variables) %>
            { "id": << row.id >>, "value": << row.value >> }<% if not loop.last %>,<% endif %>
        <% endfor %>
    ]
    ```
    This will render to a proper JSON list:
    ```
    [
        { "id": 1, "value": 10 },
        { "id": 2, "value": 20 }
    ]
    ```
    Notes:
    - When using @json, the entire string is rendered as a Jinja template
        and then parsed with json.loads().
    - Nested @json blocks are not supported.
    - Newlines and whitespace are automatically collapsed during rendering.
    """  # noqa: E501
    errorhandler: RestErrorHandler = PydanticField(
        default_factory=RestErrorHandler
    )
    """
    Custom error handler e.g. for searching conditions in response
    or custom status codes
    """
    response: RestResponse
    """
    Response handling configuration
    """
    max_retries: int = 0
    """
    Maximum number of retries on transport errors.
    Default is 0 (no retry).
    """

    initial_backoff: float = 0.5
    """
    Initial backoff time in seconds.
    Will be multiplied exponentially for subsequent retries (2^n).
    """
