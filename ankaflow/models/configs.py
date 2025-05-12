import typing as t
from pydantic import BaseModel, Field as PydanticField

from .llm import LLMConfig

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
    clickhouse: ClickhouseConfig = PydanticField(
        default_factory=ClickhouseConfig
    )
    """Language model configuration."""
    llm: LLMConfig = PydanticField(
        default_factory=LLMConfig
    )
    """Language model configuration."""

