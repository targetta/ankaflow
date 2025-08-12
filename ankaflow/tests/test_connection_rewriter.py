# tests/test_raw_sql_rewriter_unittest.py
import types
import unittest
from unittest.mock import patch

from ..connections.connection import Connection


class _PFObj:
    """Minimal Pathlike that mirrors PathFactory contract for tests."""

    def __init__(self, path: str) -> None:
        self.path = path

    def is_absolute(self) -> bool:
        """Return True when path is local-absolute or contains a scheme."""
        return self.path.startswith("/") or "://" in self.path


def _mk_conn(long_locator: str) -> "Connection":
    """Create a Connection instance without running __init__ and stub locate()."""
    from ..connections.connection import Connection  # noqa: F401  # replace module name

    c = object.__new__(Connection)

    def _locate(
        self, name: str | None = None, use_wildcard: bool = False
    ) -> str:
        """Stubbed locate that returns a deterministic 'long' locator."""
        return long_locator

    # attach stubbed locate
    c.locate = types.MethodType(_locate, c)
    return c


class TestRawSqlRewriter(unittest.TestCase):
    """Unit tests for Connection._raw_sql_rewriter()."""

    def setUp(self) -> None:
        """Patch PathFactory.make() to use the minimal _PFObj."""
        self._pathfactory_patcher = patch(
            "ankaflow.common.path.PathFactory",
            new=types.SimpleNamespace(make=lambda s: _PFObj(s)),
        )
        self._pathfactory_patcher.start()

    def tearDown(self) -> None:
        """Stop PathFactory patch."""
        self._pathfactory_patcher.stop()

    def test_delta_scan_short_to_long(self) -> None:
        """Replaces delta_scan('short') with resolved locator; preserves rest."""
        conn = _mk_conn("gs://bucket/a/long/prefix/orders")
        sql_in = "SELECT * FROM delta_scan('orders') WHERE day > 0"
        sql_out = conn._raw_sql_rewriter(sql_in)
        self.assertIn("delta_scan('gs://bucket/a/long/prefix/orders')", sql_out)
        self.assertIn("WHERE day > 0", sql_out)

    def test_read_parquet_kwargs_preserved(self) -> None:
        """Keeps kwargs as-is while swapping the locator."""
        conn = _mk_conn("gs://bucket/landing/products/*.parquet")
        sql_in = "SELECT * FROM read_parquet('products', union_by_name=true)"
        sql_out = conn._raw_sql_rewriter(sql_in)
        self.assertIn(
            "read_parquet('gs://bucket/landing/products/*.parquet', union_by_name=true)",
            sql_out,
        )

    def test_absolute_remote_is_noop(self) -> None:
        """Absolute remote path must not be rewritten."""
        conn = _mk_conn("gs://anything/should/not/matter")
        sql_in = "SELECT * FROM read_parquet('s3://x/y/z.parquet')"
        sql_out = conn._raw_sql_rewriter(sql_in)
        self.assertEqual(sql_in, sql_out)

    def test_absolute_local_is_noop(self) -> None:
        """Local absolute path must not be rewritten."""
        conn = _mk_conn("gs://anything/should/not/matter")
        sql_in = "SELECT * FROM read_parquet('/mnt/data/file.parquet')"
        sql_out = conn._raw_sql_rewriter(sql_in)
        self.assertEqual(sql_in, sql_out)

    def test_containment_check_failure_raises(self) -> None:
        """If short is not a substring of long locator, raise ValueError."""
        conn = _mk_conn("gs://bucket/long/prefix/customers")
        sql_in = "SELECT * FROM delta_scan('orders')"
        with self.assertRaises(ValueError) as ctx:
            conn._raw_sql_rewriter(sql_in)
        self.assertIn("orders", str(ctx.exception))
        self.assertIn("customers", str(ctx.exception))

    def test_multiple_occurrences_are_all_rewritten(self) -> None:
        """Every occurrence of supported functions gets rewritten."""
        conn = _mk_conn("gs://bucket/prefix/orders")
        sql_in = """
            WITH a AS (SELECT * FROM delta_scan('orders')),
                 b AS (SELECT * FROM delta_scan('orders'))
            SELECT * FROM a JOIN b USING(id)
        """
        sql_out = conn._raw_sql_rewriter(sql_in)
        self.assertEqual(
            sql_out.count("delta_scan('gs://bucket/prefix/orders')"),
            2,
        )

    def test_trailing_semicolon_and_double_quotes(self) -> None:
        """Supports trailing semicolons; accepts double quotes."""
        conn = _mk_conn("gs://bucket/prefix/orders")
        sql_in = 'SELECT * FROM delta_scan("orders");'
        sql_out = conn._raw_sql_rewriter(sql_in)

        # Accept either single- or double-quoted outcome
        ok_single = "delta_scan('gs://bucket/prefix/orders')" in sql_out
        ok_double = 'delta_scan("gs://bucket/prefix/orders")' in sql_out
        self.assertTrue(ok_single or ok_double)
        self.assertTrue(sql_out.strip().endswith(";"))


if __name__ == "__main__":
    unittest.main()
