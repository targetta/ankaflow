import pandas as pd
import logging
import asyncio
import typing as t
import time
import re
import pyarrow as pa
from pypika import Field as Field, Order
from pypika.analytics import RowNumber
from sqlglot import parse_one

try:
    import psutil

    _HAS_PSUTIL = True
except ImportError:
    _HAS_PSUTIL = False

from .. import models as m


class ConfigResolver:
    """
    Helper class to extract specific field from configured connection.

    If kind is known then object of given kind is returned.

    If kind is not known then first object with populated fields
    will be returned.
    """

    def __init__(self, connection: m.ConnectionConfiguration) -> None:
        self.cfg = connection

    def _first_not_none(self, field_name: str) -> m.BucketConfig | None:
        root = self.cfg
        for name in root.model_fields.keys():
            field = getattr(root, name)
            if isinstance(field, m.BucketConfig):
                if getattr(field, field_name):
                    return field

    def resolve(self, field: str, kind: str | None = None):
        """
        Connection may contain one or more configurations: s3, gs &c.
        This helper will find

        If `kind` is set then resolver will extract field from that kind.


        Args:
            field (str): field name eg `bucket`
            kind (str | None, optional): connection kind eg `s3`.
                Defaults to None.

        Returns:
            str | None: field value or None
        """
        if kind:
            try:
                conn = getattr(self.cfg, kind)
                found = getattr(conn, field)
            except AttributeError:
                raise ValueError(f"Field '{field}' not found in '{kind}'")
        # Fall back to walker
        found = self._first_not_none(field)
        if found:
            return getattr(found, field)


class ProgressLogger:
    def __init__(
        self,
        event: str | None = None,
        logger: t.Optional[logging.Logger] = None,
    ):
        self.logger = logger or logging.getLogger("null")
        self.event = event
        self._start = time.monotonic()
        self.total = 0
        self.show_memory: bool = False
        self._rss_baseline: float | None = (
            None  # Only set if psutil is available
        )

    def _get_rss(self) -> float:
        """Get current process RSS in MB."""
        if _HAS_PSUTIL:
            return psutil.Process().memory_info().rss / (1024 * 1024)  # type: ignore
        return 0.0

    def reset(self, event: str, show_memory: t.Optional[bool] = None):
        """Reset timer and update event description and memory baseline."""
        self.event = event
        self._start = time.monotonic()
        if _HAS_PSUTIL:
            self._rss_baseline = self._get_rss()
        if show_memory is not None:
            self.show_memory = show_memory

        if self.show_memory:
            rss_display = "0.0MB" if _HAS_PSUTIL else "- MB"
            self.logger.debug(f"{self.event} 0 in 0.0s, RSS: {rss_display}")

    def log(self, count: int):
        """Log count and elapsed time, with RSS delta since reset if enabled."""
        self.total += count
        elapsed = time.monotonic() - self._start
        mem = ""

        if self.show_memory:
            if _HAS_PSUTIL and self._rss_baseline is not None:
                current_rss = self._get_rss()
                delta_rss = max(current_rss - self._rss_baseline, 0.0)
                mem = f", RSS: {delta_rss:.1f}MB"
            else:
                mem = ", RSS: - MB"

        self.logger.debug(
            f"{self.event} {self.total} in {round(elapsed, 1)}s{mem}"
        )


def console_logger(
    level: str = logging.INFO,  # type: ignore[assignment]
    name: str = "duct",
) -> logging.Logger:
    """
    Quick logger to attach to pipeline

    Args:
        level (str, optional): Level name. Defaults to logging.INFO.
        name (str, optional): Logger name. Defaults to "duct".

    Returns:
        logging.Logger
    """
    formatter = logging.Formatter(
        "%(filename)-12s:%(lineno)d: %(levelname)-8s %(message)s \n----"
    )

    console = logging.StreamHandler()
    console.setFormatter(formatter)

    log = logging.getLogger(name)
    log.setLevel(level)
    # prevent multiple handlers
    if not log.handlers:
        log.addHandler(console)
    return log


def null_logger() -> logging.Logger:
    """Return a logger with a NullHandler that discards all logging messages.
    Extra guart CRITICAL+1 to reduce all message processing

    Returns:
        logging.Logger: A logger instance with a NullHandler attached.
    """
    logger = logging.getLogger(f"null:{__name__}")
    if not logger.handlers:
        logger.addHandler(logging.NullHandler())
    logger.setLevel(logging.CRITICAL + 1)
    logger.propagate = False
    return logger


def string_to_bool(value: str) -> bool:
    """
    Converts a string representation of a boolean or a Falsy value
    (including float '0.0') to its corresponding boolean value.
    Returns None if the string cannot be clearly interpreted as True or False.

    Args:
        value: The string to convert.

    Returns:
        False if the string represents False or a Falsy value
              (including '0', '0.0', 'false', 'none', '', '[]', '{}', '()'),
        True if the string cannot be clearly interpreted.
    """
    lower_value = str(value).lower()

    if lower_value in ("false", "0", "0.0", "none", "", "[]", "{}", "()"):
        return False
    else:
        return True


def print_error(*args):
    return "\n".join([str(it) for it in args])


def print_df(df: pd.DataFrame, all_rows: bool = False):
    """Formats DataFrame into a readable string, optionally displaying all rows."""  # noqa:E501

    pd.set_option("display.max_columns", None)
    pd.set_option("display.max_colwidth", None)

    if all_rows:
        pd.set_option("display.max_rows", None)  # Show all rows
    else:
        pd.set_option("display.max_rows", 10)  # Limit to 10 rows by default

    # Create output string
    out = f"\n{df}"

    # Reset to pandas' default options
    pd.reset_option("display.max_columns")
    pd.reset_option("display.max_colwidth")
    pd.reset_option("display.max_rows")

    return out


def print_fields(fields: t.Union[m.Columns, t.List[m.Column]]):
    out = []
    for field in fields:
        out.append(f"- {field.name}")
        out.append(f"  {field.type}")
    return ("\n").join(out)


def asyncio_run(coro):
    """A synchronous wrapper for asyncio.run.

    Args:
        coro: An awaitable object (e.g., the result of calling
            an async function).

    Returns:
        The result of awaiting the coroutine.

    Raises:
        RuntimeError: If no running event loop is found.
    """
    if not asyncio.iscoroutine(coro):
        raise TypeError("A coroutine object is required")

    try:
        loop = asyncio.get_running_loop()
        if loop.is_running():
            # Schedule execution in the running loop
            future = asyncio.ensure_future(coro)
            return future.result()  # This will raise an error because we can't directly block in the same thread  # noqa: E501
    except RuntimeError:
        # No event loop is running, so we can safely use asyncio.run()
        return asyncio.run(coro)

    raise RuntimeError(
        "Cannot run coroutine in a running event loop synchronously without proper context"  # noqa: E501
    )


def duckdb_to_pyarrow_type(duckdb_type: str):
    """Convert DuckDB data types to PyArrow data types."""
    if isinstance(duckdb_type, pa.DataType):
        return duckdb_type  # passthrough for already-arrow types
    type_mapping = {
        "VARCHAR": pa.string(),
        "TEXT": pa.string(),  # Alias for VARCHAR
        "STRING": pa.string(),  # Alias for VARCHAR
        "BIGINT": pa.int64(),
        "INT8": pa.int64(),  # Alias for BIGINT
        "INTEGER": pa.int32(),
        "INT4": pa.int32(),  # Alias for INTEGER
        "SMALLINT": pa.int16(),
        "INT2": pa.int16(),  # Alias for SMALLINT
        "TINYINT": pa.int8(),
        "DOUBLE": pa.float64(),
        "FLOAT": pa.float64(),  # Alias for DOUBLE
        "FLOAT8": pa.float64(),  # Alias for DOUBLE
        "REAL": pa.float32(),
        "FLOAT4": pa.float32(),  # Alias for REAL
        "BOOLEAN": pa.bool_(),
        "BOOL": pa.bool_(),  # Alias for BOOLEAN
        "TIMESTAMP": pa.timestamp("ns"),
        "DATETIME": pa.timestamp("ns"),  # Alias for TIMESTAMP
        "DATE": pa.date32(),
        "TIME": pa.time64("ns"),
        "INTERVAL": pa.duration("ns"),
        "JSON": pa.string(),  # Store JSON as a string
        "BLOB": pa.binary(),  # Binary large object
        "UUID": pa.string(),  # Store UUID as a string
        "DECIMAL": pa.decimal128(38, 18),  # Default precision and scale
        "NUMERIC": pa.decimal128(38, 18),  # Alias for DECIMAL
    }

    # Handle LIST[] types (bracket notation)
    list_bracket_match = re.match(r"(\w+)\[\]", duckdb_type)
    if list_bracket_match:
        inner_type = list_bracket_match.group(1)
        return pa.list_(duckdb_to_pyarrow_type(inner_type))

    # Handle LIST(...) types
    list_match = re.match(r"LIST\((.*?)\)", duckdb_type)
    if list_match:
        inner_type = list_match.group(1)
        return pa.list_(duckdb_to_pyarrow_type(inner_type))

    # Handle STRUCT[] types (bracket notation)
    struct_bracket_match = re.match(r"STRUCT\((.*?)\)\[\]", duckdb_type)
    if struct_bracket_match:
        struct_fields = struct_bracket_match.group(1)
        struct_schema = []
        for field in struct_fields.split(", "):
            field_name, field_type = field.split(" ", 1)
            struct_schema.append((
                field_name.strip('"'),
                duckdb_to_pyarrow_type(field_type),
            ))
        return pa.list_(pa.struct(struct_schema))

    # Handle STRUCT(...) types
    struct_match = re.match(r"STRUCT\((.*?)\)", duckdb_type)
    if struct_match:
        struct_fields = struct_match.group(1)
        struct_schema = []
        for field in struct_fields.split(", "):
            field_name, field_type = field.split(" ", 1)
            struct_schema.append((
                field_name.strip('"'),
                duckdb_to_pyarrow_type(field_type),
            ))
        return pa.struct(struct_schema)

    # Handle simple types
    if duckdb_type in type_mapping:
        return type_mapping[duckdb_type]

    raise ValueError(f"Unsupported DuckDB type: {duckdb_type}")


# TODO: Split into separate QueryRenderer class
def build_ranked_query(
    query: str,
    selectable: str,
    version: t.Optional[str],
    keys: t.Optional[t.List[str]],
    dialect: str,
) -> tuple[str, str]:
    """
    Helper function to inject row ranking logic into a SQL query.

    Args:
        query: Base SQL query (SELECT ...)
        selectable: FROM clause target (e.g., table or alias)
        version: Column used for ranking (e.g., timestamp)
        keys: List of partitioning keys
        dialect: SQL dialect for sqlglot

    Returns:
        tuple[str, str]: (transformed SQL query, optional WHERE clause)
    """
    apply_ranking = bool(version and keys)
    where_clause = f"WHERE {Field('__rank__') == 1}" if apply_ranking else ""

    base_query = parse_one(query)

    if apply_ranking:
        rank_filter_expr = Field("__rank__") == 1
        parsed_filter = parse_one(str(rank_filter_expr))
        where_clause = f"WHERE {parsed_filter.sql(dialect, identify=True)}"
        base_query = base_query.from_(selectable)  # type: ignore[attr-defined]
        # Build ROW_NUMBER() OVER (PARTITION BY ...) ORDER BY ... using PyPika
        rank_expr = RowNumber()
        for key in keys:  # type: ignore
            rank_expr = rank_expr.over(Field(key))
        rank_expr = rank_expr.orderby(Field(version), order=Order.desc)  # type: ignore

        # Inject the __rank__ column using SQLGlot-compatible string
        base_query = base_query.select(f"{rank_expr.get_sql()} AS __rank__")  # type: ignore[attr-defined]

        # Wrap in a subquery for dialects like BigQuery
        base_query = base_query.subquery(alias="ranked")  # type: ignore[attr-defined]
        sql = f"SELECT * FROM {base_query.sql(dialect, identify=True)}"
    else:
        base_query = base_query.from_(selectable)  # type: ignore[attr-defined]
        sql = base_query.sql(dialect, identify=True)
        where_clause = ""

    return sql, where_clause


def validate_simple_query(query: str, ranking_enabled: bool) -> None:
    """
    Validates that the input query is safe to use with delta_scan() and ranking.

    Args:
        query: The raw SQL query.
        ranking_enabled: Whether ranking (ROW_NUMBER) will be applied.

    Raises:
        ValueError: If query contains CTEs, aggregates, or disallowed structures.
    """
    tree = parse_one(query)

    if tree.args.get("with"):
        raise ValueError("CTEs are not supported in delta scan source queries.")

    if ranking_enabled:
        if tree.args.get("group"):
            raise ValueError(
                "GROUP BY is not supported when ranking is applied."
            )
        # Optional: look for aggregate functions via regex or AST walk
        lowered = query.lower()
        if any(
            func in lowered
            for func in ["avg(", "sum(", "count(", "min(", "max("]
        ):
            raise ValueError(
                "Aggregate functions are not allowed when ranking is applied."
            )
