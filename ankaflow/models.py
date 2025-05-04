import typing as t
from pathlib import Path
import yaml
import importlib

from pydantic import (
    BaseModel,
    RootModel,
    Field as PydanticField,
    field_validator,
)
from enum import Enum

from .common.types import StringDict, ImmutableMap


# define a simple Protocol so Stages.load can accept any loader
@t.runtime_checkable
class Loadable(t.Protocol):
    def load(self) -> t.Any: ...


class ModelType(Enum):
    """"""

    source = "source"
    transform = "transform"
    sink = "sink"


class LogLevel(Enum):
    """"""

    INFO = "INFO"
    WARNING = "WARNING"
    DEBUG = "DEBUG"


class Variables(dict):
    """
    Variables is a `dict`-based collection
    storing arbitrary data under keys. Variable can be populated via:

    - Dictionary passed to pipeline:
      `Flow(defs, context, variables={'foo': 'bar'})`
    - Sink operation:

            - kind: sink\n
                name: my_sink\n
                connection:\n
                    kind: Variable\n
                    locator: variable_name

      This step will place a list of records from preceding step in
      variables as `{'variable_name': t.List[dict]}`

    In most cases sainking to variable is not needed as all preceding stages
    in a pipeline can be referenced via `name`. Sinking to variable is
    useful when you need to share data with subpipeline.

    Special variable is `loop_control` which is populated
    dynamically from previous step and current stage type is pipeline.
    In case previous step generates multiple records then the new pipeline
    is called for each record, and control variable holds a `dict`
    representing the current record.
    """


class FlowContext(ImmutableMap):
    """
    Context dictionary can be used to supply arbitrary data
    to pipeline that can be referenced in templates much
    much like variables with the difference that they cannot
    be modified at runtime.

    Context can be initiated via dictionary:

    `context = FlowContext(**{'foo':'Bar'}])`

    Items can be reference using both bracket and dot notation:

    `context.foo == context['foo'] == 'Bar'`

    """

    pass


class Field(BaseModel):
    """Data fields (equivalent to database field)"""

    name: str
    """
    Field name must follow rules set to SQL engine field names
    """
    type: str
    """
    Any data type support by SQL engine
    """


class Fields(RootModel[t.List[Field]]):
    """
    Iterable list-like collection of Fields.
    """

    root: t.List[Field]
    _error: t.Optional[str] = None  # not part of validation

    def values(self):
        return self.root

    def __getitem__(self, item):
        return self.root[item]

    def __iter__(self):  # type: ignore[override]
        return iter(self.root)

    @classmethod
    def error(cls, message: str) -> "Fields":
        fields = cls([])
        fields._error = message
        return fields

    def is_error(self) -> bool:
        return self._error is not None

    def print(self) -> str:
        if self._error:
            return f"⚠️ Schema Error: {self._error}"
        out = []
        for item in self.root:
            out.append(f"- name: {item.name}\n  type: {item.type}")
        return "\n".join(out)


class BucketConfig(BaseModel):
    bucket: t.Optional[str] = None
    """
    Bucket name eg. `s3://my-bucket`
    or local absolute path eg. `/data/path`
    """
    data_prefix: t.Optional[str] = None
    """
    Prefix for data files: `s3://my-bucket/<prefix>` or
    `/data/path/<prefix>`
    """
    locator_wildcard: t.Optional[t.Tuple] = None
    """
    Regular expression and wildcard to modify `locator`.
    Useful in cases when you sink data to `data-YYYY.parquet`
    but want to read `data-*.parquet`.
    Provide tuple with patten as first element and replacement
    as second one. Example:

    ```
    ('-\\d{4}-', '*')
    ```

    This will create wildcard for `data-YYYY-base.parquet` as
    `data*base.parquet`.

    |  Wildcard  |                        Description                        |
    |------------|-----------------------------------------------------------|
    | `*`        | matches any number of any characters (including none)     |
    | `**`       | matches any number of subdirectories (including none)     |
    | `[abc]`    | matches one character given in the bracket                |
    | `[a-z]`    | matches one character from the range given in the bracket |

    Wildcard is automatically applied in tap and show_schema operations.
    """
    region: str | None = None
    """"""


class S3Config(BucketConfig):
    """
    S3-specific bucket configuration including optional authentication.
    """

    access_key_id: t.Optional[str] = None
    """
    AWS access key ID for private S3 access.
    """
    secret_access_key: t.Optional[str] = None
    """
    AWS secret access key for private S3 access.
    """


class GSConfig(BucketConfig):
    """
    Google Cloud Storage (GCS) configuration.

    - HMAC credentials allow reading/writing Parquet, JSON, and CSV.
    - Delta table writes require a full service account credential file.
    """

    hmac_key: t.Optional[str] = None
    """
    GCS HMAC key ID for authenticated access.
    """
    hmac_secret: t.Optional[str] = None
    """
    GCS HMAC secret for authenticated access.
    """
    credential_file: t.Optional[str] = None
    """
    Path to GCP service account credential file (JSON).
    Required for certain write operations (e.g., Delta tables).
    """


class BigQueryConfig(BaseModel):
    """
    Configuration for accessing BigQuery datasets.
    """

    project: t.Optional[str] = None
    """
    GCP project ID containing the BigQuery dataset.
    """
    dataset: t.Optional[str] = None
    """
    BigQuery dataset name.
    """

    region: t.Optional[str] = None
    """
    BigQuery region (e.g., "us-central1").
    """
    credential_file: t.Optional[str] = None
    """
    Path to service account JSON credentials for BigQuery access.
    """


class DatabaseConfig(BaseModel):
    """
    Base class for SQL database connection configurations.
    """

    database: t.Optional[str] = None
    """
    Database name.
    """
    host: t.Optional[str] = None
    """
    Hostname or IP address of the database server.
    """
    cluster: t.Optional[str] = None
    """
    Optional cluster name or identifier
    (used in ClickHouse and other distributed systems).
    """
    port: t.Optional[int | str] = None
    """
    Database port.
    """
    username: t.Optional[str] = None
    """
    Username for authentication.
    """
    password: t.Optional[str] = None
    """
    Password for authentication.
    """


class ClickhouseConfig(DatabaseConfig):
    """
    Configuration for ClickHouse database, extending generic SQL config.
    """

    blocksize: int = 50000
    """
    Number of rows to process per block for batch operations.
    """


class ConnectionConfiguration(BaseModel):
    """
    Top-level container for all connection configurations in a pipeline.

    Includes default configuration blocks for supported sources and sinks like:
    - Local file systems
    - S3 and GCS buckets
    - BigQuery datasets
    - ClickHouse databases

    These can be customized per pipeline or extended with new sources.

    Example:
        To support MySQL, you can define a new model like this:

        ```python
        class MySQLConfig(DatabaseConfig):
            pass  # You can add MySQL-specific fields here if needed

        class ExtendedConnectionConfiguration(ConnectionConfiguration):
            mysql: MySQLConfig = PydanticField(default_factory=MySQLConfig)
        ```

        This will allow you to specify MySQL connection settings in your YAML:

        ```yaml
        connection:
          kind: MySQL
          config:
            host: localhost
            port: 3306
            database: sales
            username: admin
            password: secret
        ```

    """

    local: BucketConfig = PydanticField(default_factory=BucketConfig)
    """Local file system configuration."""
    s3: S3Config = PydanticField(default_factory=S3Config)
    """S3 cloud storage configuration."""
    gs: GSConfig = PydanticField(default_factory=GSConfig)
    """Google Cloud Storage configuration."""
    bigquery: BigQueryConfig = PydanticField(default_factory=BigQueryConfig)
    """BigQuery connection configuration."""
    clickhouse: ClickhouseConfig = PydanticField(default_factory=ClickhouseConfig)
    """ClickHouse database configuration."""


class Connection(BaseModel):
    kind: str  # Deltatable, Bigquery, Clickhouse, Parquet, File, Rest
    """
    Model type e.g. Deltatable, Bigquery, Clickhouse, Parquet, File
    Custom connections can be loaded from module.
    """
    locator: str
    """
    Table name or file name or URI, or other identifier
    required by the connection.
    """
    config: t.Optional[ConnectionConfiguration] = None
    """
    Optional configuration for the current connection.
    If not present then global configuration will be used.
    """
    params: t.Optional[dict] = PydanticField(default_factory=dict)
    """
    Any parameters that can be passed to connection.
    """
    fields: t.Optional[Fields] = None
    """
    If set then schema is used to generate source structure
    in case actual source does not provide data in which case
    generation of ephemeral view fails.
    """
    version: t.Optional[str] = None
    """
    Field for record version timestamp
    """
    key: t.Optional[t.List[str]] = None
    """
    List of versioned fields
    """
    partition: t.Optional[t.List[str]] = None
    """
    If set then delta table is partitioned using
    specified fields for faster reads
    """
    data_mode: str = "error"
    """
    Data mode for write operation. For Deltatable
    valid options are:

    - `append` adds new data
    - `overwrite` replaces all data
    - `error` fails (table is read-only)
    """
    schema_mode: t.Optional[str] = None
    """
    Deltatable schema behaviour. If not set then write fails
    with error in case the data schema does not match existing
    table schema.

    Schema evolution options:

    - `merge` adds new columns from data
    - `overwrite` adds new columns, drops missing
    """
    optimize: t.Optional[t.Union[str, int]] = 1
    """
    Use with Deltatable and other engines whether to optimize
    after each sink operation. With larger tables this may be a
    lengthy synchronous operation.

    Default value is optimize and vacuum with 7 day retention.

    # Deltatable

      Values are `optimize,vacuum,all,Literal[int]`.
      If value is literal int is provided then parts older than
      number of days will be removed. Note that this will override
      default retention days.

    String options `vacuum,all` are equivalent to 0.
    """
    create_statement: t.Optional[str] = None
    """
    Create statement for given table. Must be in requested dialect.
    """
    show_schema: t.Optional[bool] = None
    """
    If true then schema is automatically detected from
    the input data and logged
    '"""


class Deltatable(Connection):
    """"""

    kind: t.Literal["Deltatable"]  # type: ignore[assignment]


class Parquet(Connection):
    """"""

    kind: t.Literal["Parquet"]  # type: ignore[assignment]


class JSON(Connection):
    """"""

    kind: t.Literal["JSON"]  # type: ignore[assignment]


class CSV(Connection):
    """"""

    kind: t.Literal["CSV"]  # type: ignore[assignment]


class Clickhouse(Connection):
    """"""

    kind: t.Literal["Clickhouse"]  # type: ignore[assignment]


class BigQuery(Connection):
    """"""

    kind: t.Literal["BigQuery"]  # type: ignore[assignment]


class File(Connection):
    """"""

    kind: t.Literal["File"]  # type: ignore[assignment]


class Variable(Connection):
    """"""

    kind: t.Literal["Variable"]  # type: ignore[assignment]


class CustomConnection(BaseModel):
    """Custom connection provider. Custom connection may implement
    its own logic but must derive from base Connection class, and expose
    tap(), sink(), sql() and show_schema() even if they are no-op.
    """

    kind: t.Literal["CustomConnection"] = "CustomConnection"
    module: str
    """Python module where the connection class is defined"""
    classname: str
    """Name of the class to load from the module"""
    params: dict = PydanticField(default_factory=dict)
    """Free-form configuration parameters passed to the loaded class"""
    # Following attributes are rquired by base class.
    # TODO: Refactor to derive from BaseConnection model
    config: ConnectionConfiguration | None = None
    fields: t.Optional[t.List[Field]] = None
    locator: t.Optional[str] = None

    def load(self) -> t.Type[Connection]:
        """
        Dynamically load the connection class from the given module and member.

        Returns:
            A subclass of Connection.

        Raises:
            ImportError: If the module or member can't be imported.
        """
        mod = importlib.import_module(self.module)
        try:
            cls = getattr(mod, self.classname)
        except AttributeError as e:
            raise ImportError(
                f"Could not load '{self.classname}' from module '{self.module}'"
            ) from e
        # TODO: Refactor imports to facilitate this check early
        # if not issubclass(cls, Connection):
        #     raise TypeError(
        #         f"{cls.__name__} is not a subclass of Connection"
        #     )

        return cls


class Dimension(BaseModel):
    kind: t.Literal["Dimension"]  # type: ignore[assignment]
    module: str
    config: t.Optional[t.Any] = None
    fields: t.Optional[t.List[Field]] = None  # type: ignore[assignment]
    locator: t.Optional[str] = None  # type: ignore[assignment]

    def load(self) -> t.Type[Connection]:
        """
        Dynamically load the connection class from the given module and member.

        Returns:
            A subclass of Connection.

        Raises:
            ImportError: If the module or member can't be imported.
        """
        mod = importlib.import_module(self.module)
        try:
            cls = getattr(mod, self.kind)
        except AttributeError as e:
            raise ImportError(
                f"Could not load '{self.kind}' from module '{self.module}'"
            ) from e

        return cls


class SchemaItem(BaseModel):
    table: str
    locator: t.Optional[str] = None
    kind: str
    fields: t.List[Field]


class ResponseHandlerTypes:
    """"""

    BASIC = "Basic"
    PAGINATOR = "Pagination"
    URLPOLLING = "URLPolling"
    STATEPOLLING = "StatePolling"


class DataType(Enum):
    """"""

    JSON = "application/json"
    JSONL = "application/jsonl"
    CSV = "text/csv"
    PARQUET = "application/vnd.apache.parquet"


class ContentType(Enum):
    """"""

    JSON = "application/json"
    FORM = "application/x-www-form-urlencoded"


class AuthType(Enum):
    """"""

    BASIC = "basic"
    DIGEST = "digest"
    HEADER = "header"
    OAUTH2 = "oauth2"


class ParameterDisposition(Enum):
    """"""

    QUERY = "query"
    BODY = "body"


class RequestMethod(Enum):
    """"""

    GET = "get"
    POST = "post"
    PUT = "put"
    PATCH = "patch"


class RestAuth(BaseModel):
    """
    Authenctication configuration for Rest connection.

    NOTE: Not all authentication methods may not work
    in browser due to limitations in the network API.
    """

    method: AuthType
    """Specifies authentiation type."""
    values: StringDict
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


class RestResponse(BaseModel):
    """
    Response configuration. Response can be paged,
    polled URL or in body.
    """

    handler: t.Union[BasicHandler, Paginator, URLPoller, StatePoller, None] = (
        PydanticField(None, discriminator="kind")
    )  # noqa:E501

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
    errorhandler: RestErrorHandler = PydanticField(default_factory=RestErrorHandler)
    """
    Custom error handler e.g. for searching conditions in response
    or custom status codes
    """
    response: RestResponse
    """
    Response handling configuration
    """


class Rest(BaseModel):
    """
    Configuration for a REST-based data connection.

    This model defines how to configure and execute REST API requests
    as part of a pipeline step. It includes the request definition,
    client behavior (e.g., retries, headers), and optional schema discovery.
    """

    kind: t.Literal["Rest"]
    """Specifies the connection type as "Rest"."""
    client: RestClientConfig
    """Configuration for the REST client (e.g., base URL, headers, auth)."""
    request: Request
    """Request template specifying method, path, body, etc."""
    fields: t.Optional[t.List[Field]] = None
    """Optional schema definition used to validate or transform
    response data.

    It is recommended to manually specify the schema after initial
    discovery. This ensures downstream pipeline stages remain stable,
    even when the remote API returns no results (e.g., due to no updates
    in an incremental fetch). Explicit schema prevents silent failures
    or schema drift in such cases.
    """
    show_schema: t.Optional[bool] = None
    """
    If True, the connector will attempt to infer
    or display the response schema automatically.
    """
    # Following properties are required in base connection
    module: t.Optional[str] = None
    config: t.Optional[t.Any] = None
    locator: t.Optional[str] = None


class Datablock(BaseModel):
    kind: str
    """
    Defines which action will be performed:

    - source
    - transform
    - sink
    - pipeline
    - sql

    Sink
    ---
    Sink reads output from previous stage and stores in specified
    location.

    NOTE: If query is supplied with the stage then sink uses output
    of the query rather than preceding stage directly.
    Subsequent sink will use preceding stage. If supplied query must
    create either view or table with same name as current stage.

        - name: my_sink
        kind: sink
        connection:
            kind: Variable
        query: >
            CREATE VIEW my_sink AS
            SELECT
            1 as foo

    Pipeline
    ---
    If pipeline is preceded by any stage then the subpipeline will be executed
    as many times as there are rows in the previous stage output.
    This is useful if you want to run same pipeline with different parameters.
    Make sure the pipeline is preceded by source or transform producing
    required number of rows. If you need to run subpipeline only once there
    are two options:

    1. Place it to the top
    1. Preced with tranform producing single row only

    Each row is then passed to subpipeline in a special variable.

    Example pipeline iterating subpipeline 5 times:

      - kind: transform
          name: generate_5
          # Generate 5 rows
          query: >
              select unnest as current_variable from unnest(generate_series(1,5))
          show: 5
      - kind: pipeline
        name: looped_5x
        stages:
            - kind: transform
              name: inside_loop
              # In query we can reference the value passed from parent pipeline
              query: >
                  select 'Currently running iteration: {API.look('loop_control.current_variable', variables)}' as value
              show: 5
    """  # noqa:E501
    name: str
    """
    Name of the stage, must be unique across all stages in the pipeline
    and conform to the rules: Must start with letter, may contain lowercase
    letters, number and underscores.
    Name is used to reference this stage by other subsequent stages.
    """
    connection: t.Optional[
        t.Union[
            "Rest",
            Variable,
            BigQuery,
            Deltatable,
            Parquet,
            Clickhouse,
            CustomConnection,
            JSON,
            CSV,
            File,
            Dimension,
        ]
    ] = PydanticField(None, discriminator="kind")
    """
    Defines how the data is read from / written to the target.

    Connection fields may contain templates and they will be
    recursively.

    Special construct is JSON> which allows dynamically generating
    parameters as runtime:

    ```
    - kind: source
      name: source_name
      connection:
        kind: File
        params: >
          JSON>{
            "key": "value",
            "dynkey": <<API.property>>,
          }
    ```

    In the above app `params` are constructed as JSON string.
    It is possible to even construct parameter keys dynamically:

    ```
    params: >
      JSON>
      {
        <% for number in [1,2,3] %>
        "key_<< number >>":<< number >><% if not loop.last %>,<% endif %>
        <% endfor %>
      }
    ```

    Above example results the following:

    ```
    params: >
      {
        "key_1": 1,
        "key_2": 2,
        "key_3": 3
      }
    ```

    JSON> structure cannot contain nested JSON> structures, the entire
    string following the JSON> header must result a valid JSON.


    Inbuilt connections include:

    - Deltatable (read)

      **If connection is `Deltatable` then query is required to narrow
      down data stored in the delta table. `FROM` clause must be `Deltatable`:**
      ```
      - kind: source
        name: delta_tap
        connection:
          kind: Deltatable
          locator: delta_table
        query: >
          select * from Deltatable
      ```

    - Deltatable (write)

      *See also StorageOptions*

      The following example writes data from preceding stage
      to delta table, appending the data, partitions using
      `part` column, and optimizes and vacuums immediately without
      retention after write.
      ```
      - kind: sink
        name: delta_sink
        connection:
          kind: Deltatable
          locator: delta_table
          optimize: 0
          data_mode: append
          partition:
          - part_field
      ```

    - Parquet (read/write)
    - JSON (read/write; NOTE: write operation generates newline-delimited JSON)
    - CSV (read/write)
    - Variable (read/write)


    - File (read)

      File can be read from a connected filesystem (including s3). File name
      and file type must be specified in the pipeline context:

        - `context.FileName`: file name relative to `file_prefix`
        - `context.FileType`: file type, one of CSV, XLSX, JSON, HTML

      Any file reader configuration must be passed in `params`.


    - Rest (bidirectional)

      Rest connections consist of two parts: Client and Request. Client contains
      base URL and authentication (basic, digest, header and oauth2 are supported).

      ```
      - name: TheRest
        kind: source
        connection:
          kind: Rest
          client:
            base_url: https://api.example.com
            auth:  # Every request to given endpoint share the same authentication
              method: basic
              values:
                username: TheName
                password: ThePassword
          request:
            endpoint: /v1/some-path
            content_type: application/json
            method: post
            query:  # Query parameters
              date: <<API.dt(None).isoformat()>>
            body:  # JSON payload
              param: 1
            response:
              content_type: application/json
              locator: "JMESPath.to.data"
      ```

    Any custom source (API) can be used as long as available via module.
    """  # noqa:E501
    locator: t.Optional[str] = None
    """
    Currently unused: Name for the connection configuration: name, or URI.
    """
    skip_if: t.Optional[t.Any] = None
    """
    Any value that can evaluated using bool().
    or template string e.g. `<< True >>`.
    When the expression evaluates to True then the stage is skipped.
    """
    query: t.Optional[str] = None
    """
    SQL Query or dictionary with custom source parameters.
    May contain {dynamic variables}.
    """
    context: t.Optional[FlowContext] = None
    """ @private
    Global context passed to given stage
    """
    show: int = 0
    """
    If set to positive integer then given number of rows from
    this stage will get logged. If set to -1 then all rows
    will be loggged. Set to 0 to disable logging.
    """
    show_schema: t.Optional[bool] = None
    """
    If True then schema is logged
    """
    explain: t.Optional[bool] = None
    """
    If set to true then SQL query explanation will be logged.
    """
    stages: t.Optional["Stages"] = None
    """
    Used when kind is `Flow`
    """
    on_error: str = "fail"
    """
    If set to 'continue' then pipeline will not fail.
    Subsequent stages referring to failed one must handle
    missing data.
    """
    throttle: t.Optional[t.Union[int, float]] = None
    """
    If set to positive value then flow execution will be paused
    after the stage for given number of seconds.

    Useful when dealing with rate limits or otherwise spreading the load
    over time.    
    """
    log_level: t.Optional[LogLevel] = None
    """
    Set logging level. All stages after (including current) will log
    with specified level. Possible values: INFO (default), DEBUG, WARNING.
    Log level will be reset to INFO after each pipeline
    (including nested pipelines).
    """
    fields: t.Optional[t.List[Field]] = None
    """
    Explicitly defined output fields.
    """

    model_config = {"arbitrary_types_allowed": True}

    @field_validator("stages", mode="before")
    @classmethod
    def _wrap_stages(cls, v):
        if isinstance(v, list):
            return Stages.model_validate(v)
        return v


class Stages(RootModel[t.List[Datablock]]):
    """A sequence of processing stages in a data pipeline.

    Each `Datablock` in `root` represents one discrete step or transformation
    in an end-to-end business workflow (e.g., tap, transform, sink, validate).

    Attributes:
        root (List[Datablock]): Ordered list of pipeline stages to execute.
    """

    root: t.List[Datablock]

    def steps(self) -> t.Iterator[Datablock]:
        """Yield each stage in execution order.

        Returns:
            Iterator[Datablock]: An iterator over the stages,
            from first to last.
        """
        return iter(self.root)

    def enumerate_steps(self) -> t.Iterator[tuple[int, Datablock]]:
        """Yield each stage along with its 0-based position.

        Use this when you need both the stage and its index for logging,
        metrics, or conditional branching.

        Returns:
            Iterator[Tuple[int, Datablock]]: Pairs of (index, stage).
        """
        return enumerate(self.root)

    def __iter__(self) -> t.Iterator[Datablock]:  # type: ignore deprecated
        import warnings

        warnings.warn(
            "Use Stages().steps() instead of direct iteration",
            DeprecationWarning,
            stacklevel=2,
        )  # noqa:B028,E501
        return iter(self.root)

    @classmethod
    def load(
        cls,
        source: t.Union[str, Path, t.IO[str], Loadable],
    ) -> "Stages":
        """Load a pipeline from YAML (path, YAML-string, file-like or Loadable).

        Args:
            source (str | Path | IO[str] | Loadable):
                - Path to a .yaml file
                - Raw YAML content
                - File-like object returning YAML
                - Any object with a `.load()` method returning Python data

        Returns:
            Stages: a validated `Stages` instance.
        """
        # 1) If it’s a loader-object, pull Python data directly
        if isinstance(source, Loadable):
            data = source.load()

        else:
            # 2) Read text for YAML parsing:
            if hasattr(source, "read"):
                text = t.cast(t.IO[str], source).read()
            else:
                text = str(source)

            # 3) First, try parsing as raw YAML
            try:
                data = yaml.safe_load(text)
            except yaml.YAMLError:
                data = None

            # 4) Only if that parse returned a `str` we treat it as a filename
            if isinstance(data, str):
                try:
                    text = Path(data).read_text()
                    data = yaml.safe_load(text)
                except (OSError, yaml.YAMLError) as e:
                    raise ValueError(
                        f"Could not interpret {data!r} as YAML or file path"
                    ) from e

        # 5) Validate final shape
        if not isinstance(data, list):
            raise ValueError(
                f"Expected a list of pipeline stages, got {type(data).__name__}"
            )

        # 5) Finally, validate into our model
        return cls.model_validate(data)

    @classmethod
    def from_stages_list(cls, raw: t.Any) -> "Stages":
        if isinstance(raw, cls):
            return raw
        if isinstance(raw, list):
            return cls(root=[Datablock.model_validate(it) for it in raw])
        raise TypeError("Expected a list of stages or a Stages instance")

    @classmethod
    def from_yaml(cls, yaml_str: str) -> "Stages":
        import yaml

        raw = yaml.safe_load(yaml_str)
        return cls.from_stages_list(raw)

    @classmethod
    def parse_obj(cls, obj: t.Any) -> "Stages":
        # This will ensure parse_obj still returns a Stages, not a list
        return cls.model_validate(obj)


Datablock.model_rebuild()
