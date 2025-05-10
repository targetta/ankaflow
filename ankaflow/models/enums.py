from enum import Enum


class SinkStrategy(str, Enum):
    SKIP = "skip"
    CREATE = "create"
    WRITE = "write"


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
