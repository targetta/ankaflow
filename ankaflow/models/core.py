import typing as t
from pathlib import Path
import yaml

from pydantic import (
    BaseModel,
    RootModel,
    Field,
    field_validator,
)

from ..common.types import ImmutableMap

from .components import Column
from .enums import LogLevel
from .connections import (
    RestConnection,
    VariableConnection,
    BigQueryConnection,
    ClickhouseConnection,
    DeltatableConnection,
    ParquetConnection,
    CustomConnection,
    JSONConnection,
    CSVConnection,
    FileConnection,
    SQLGenConnection,
    Dimension
)

# define a simple Protocol so Stages.load can accept any loader
@t.runtime_checkable
class Loadable(t.Protocol):
    def load(self) -> t.Any: ...


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


class SchemaItem(BaseModel):
    table: str
    locator: t.Optional[str] = None
    kind: str
    fields: t.List[Column]


class Stage(BaseModel):
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
            "RestConnection",
            VariableConnection,
            BigQueryConnection,
            DeltatableConnection,
            ParquetConnection,
            ClickhouseConnection,
            CustomConnection,
            JSONConnection,
            CSVConnection,
            FileConnection,
            SQLGenConnection,
            Dimension,
        ]
    ] = Field(None, discriminator="kind")
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
    fields: t.Optional[t.List[Column]] = None
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


class Stages(RootModel[t.List[Stage]]):
    """A sequence of processing stages in a data pipeline.

    Each `Datablock` in `root` represents one discrete step or transformation
    in an end-to-end business workflow (e.g., tap, transform, sink, validate).

    Attributes:
        root (List[Datablock]): Ordered list of pipeline stages to execute.
    """

    root: t.List[Stage]

    def steps(self) -> t.Iterator[Stage]:
        """Yield each stage in execution order.

        Returns:
            Iterator[Datablock]: An iterator over the stages,
            from first to last.
        """
        return iter(self.root)

    def enumerate_steps(self) -> t.Iterator[tuple[int, Stage]]:
        """Yield each stage along with its 0-based position.

        Use this when you need both the stage and its index for logging,
        metrics, or conditional branching.

        Returns:
            Iterator[Tuple[int, Datablock]]: Pairs of (index, stage).
        """
        return enumerate(self.root)

    def __iter__(self) -> t.Iterator[Stage]:  # type: ignore deprecated
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
            return cls(root=[Stage.model_validate(it) for it in raw])
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


Stage.model_rebuild()
