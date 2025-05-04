# ruff: noqa
import unittest
import duckdb
import typing as t
from datetime import datetime, timezone

from ..internal.macros import Fn


class TestFnMacros(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.con = duckdb.connect(database=":memory:")
        cls.con.execute("CREATE SCHEMA Fn;")
        for fn_name in [k for k in Fn.__dict__ if not k.startswith("__")]:
            macro_def = f"CREATE MACRO Fn.{fn_name}" + getattr(Fn, fn_name)
            cls.con.execute(macro_def)

    def assertFn(self, sql, expected, caster=None):
        rel = self.con.execute(sql)
        result: t.Optional[t.Tuple] = rel.fetchone()
        self.assertIsNotNone(result, f"Query returned no result: {sql}")
        result = t.cast(t.Tuple, result)
        if caster:
            result = caster(result[0])
            expected = caster(expected)
        else:
            result = result[0]
        try:
            self.assertEqual(result, expected)
        except AssertionError:
            print(f"{sql}=={expected}")
            raise

    def test_add(self):
        self.assertFn("SELECT Fn.add(2, 3)", 5)

    def test_plus(self):
        self.assertFn("SELECT Fn.plus(10, 5)", 15)

    def test_minus(self):
        self.assertFn("SELECT Fn.minus(10, 3)", 7)

    def test_div(self):
        self.assertFn("SELECT Fn.div(10, 2)", 5.0)
        self.assertFn("SELECT Fn.div(10, 0)", 0.0)

    def test_mult(self):
        self.assertFn("SELECT Fn.mult(4, 5)", 20)

    def test_float(self):
        self.assertFn("SELECT Fn.float('12.5')", 12.5)
        self.assertFn("SELECT Fn.float('abc')", 0.0)

    def test_int(self):
        self.assertFn("SELECT Fn.int('123')", 123)
        self.assertFn("SELECT Fn.int(NULL)", 0)

    def test_str(self):
        self.assertFn("SELECT Fn.str(456)", "456")
        self.assertFn("SELECT Fn.str(NULL)", "")

    def test_bool(self):
        self.assertFn("SELECT Fn.bool(NULL)", False)
        self.assertFn("SELECT Fn.bool(TRUE)", True)
        self.assertFn("SELECT Fn.bool(FALSE)", False)
        self.assertFn("SELECT Fn.bool('yes')", True)
        self.assertFn("SELECT Fn.bool('')", False)

    def test_and_or_not(self):
        self.assertFn("SELECT Fn.and_(TRUE, FALSE)", False)
        self.assertFn("SELECT Fn.or_(TRUE, FALSE)", True)
        self.assertFn("SELECT Fn.not_(TRUE)", False)

    def test_ifelse(self):
        self.assertFn("SELECT Fn.ifelse(TRUE, 'yes', 'no')", "yes")
        self.assertFn("SELECT Fn.ifelse(FALSE, 'yes', 'no')", "no")

    def test_when(self):
        self.assertFn("SELECT Fn.when(TRUE, 'ok', 'fail')", "ok")

    def test_eq_ne_gt_lt_gte_lte(self):
        self.assertFn("SELECT Fn.eq(1, 1)", True)
        self.assertFn("SELECT Fn.ne(1, 2)", True)
        self.assertFn("SELECT Fn.gt(3, 2)", True)
        self.assertFn("SELECT Fn.lt(2, 3)", True)
        self.assertFn("SELECT Fn.gte(3, 3)", True)
        self.assertFn("SELECT Fn.lte(2, 3)", True)

    def test_includes_extract_trim(self):
        self.assertFn("SELECT Fn.includes('hello', 'ell')", True)
        self.assertFn("SELECT Fn.extract('abc123', '[0-9]+')", "123")
        self.assertFn("SELECT Fn.trim('  padded  ')", "padded")

    def test_uniquelist_has(self):
        self.assertFn("""
            SELECT Fn.uniquelist(x) 
            FROM (VALUES (1), (2), (2), (3)) AS t(x)
        """, [1, 2, 3], caster=set)

    def test_dt_YYYY(self):
        self.assertFn("SELECT Fn.dt_YYYY(TIMESTAMP '2025-01-01')", "2025")

    def test_dt_MM(self):
        self.assertFn("SELECT Fn.dt_MM(TIMESTAMP '2025-03-01')", "03")

    def test_dt_DD(self):
        self.assertFn("SELECT Fn.dt_DD(TIMESTAMP '2025-12-25')", "25")


    def test_dt_add(self):
        self.assertFn("SELECT Fn.dt_add(TIMESTAMP '2024-01-01', 7)", datetime.fromisoformat("2024-01-08T00:00:00"))

    def test_dt_boy(self):
        self.assertFn("SELECT Fn.dt_boy(TIMESTAMP '2024-04-15')", datetime.fromisoformat("2024-01-01T00:00:00"))

    def test_dt_bom(self):
        self.assertFn("SELECT Fn.dt_bom(TIMESTAMP '2024-04-15')", datetime.fromisoformat("2024-04-01T00:00:00"))

    def test_dt_eom(self):
        self.assertFn("SELECT Fn.dt_eom(TIMESTAMP '2024-04-15')", datetime.fromisoformat("2024-04-30T00:00:00"))

    def test_dt_monday(self):
        self.assertFn("SELECT Fn.dt_monday(TIMESTAMP '2024-04-17')", datetime.fromisoformat("2024-04-15T00:00:00"))

    def test_dt_macro(self):
        # timestamp in nanoseconds
        value_ns = 1712361600000000000  # nanoseconds for 2024-04-06T00:00:00
        expected = datetime.fromtimestamp(value_ns / 1_000_000_000, timezone.utc).replace(tzinfo=None)
        query = f"SELECT Fn.dt({value_ns})"
        self.assertFn(query, expected)

        # unix milliseconds
        value_ms = 1712361600000
        expected = datetime.fromtimestamp(value_ms / 1_000, timezone.utc).replace(tzinfo=None)
        query = f"SELECT Fn.dt({value_ms})"
        self.assertFn(query, expected)

        # unix seconds
        value_s = 1712361600
        expected = datetime.fromtimestamp(value_s, timezone.utc).replace(tzinfo=None)
        query = f"SELECT Fn.dt({value_s})"
        self.assertFn(query, expected)
        # unix seconds with fractions
        value_s = 1712361600.5
        expected = datetime.fromtimestamp(value_s, timezone.utc).replace(tzinfo=None)
        query = f"SELECT Fn.dt({value_s})"
        self.assertFn(query, expected)
        value_s = 1712361600.255
        expected = datetime.fromtimestamp(value_s, timezone.utc).replace(tzinfo=None)
        query = f"SELECT Fn.dt({value_s})"
        self.assertFn(query, expected)

        # iso string that can be directly cast
        self.assertFn("SELECT Fn.dt('2025-03-10 17:24:41')", datetime.fromisoformat("2025-03-10T17:24:41"))
        self.assertFn("SELECT Fn.dt(concat('2025-03-10',' ','17:24:41'))", datetime.fromisoformat("2025-03-10T17:24:41"))
        # unknown string fallback to 1970
        self.assertFn("SELECT Fn.dt('not a date')", datetime.fromisoformat("1970-01-01T00:00:00"))
        with self.assertRaises(duckdb.ConversionException):
            self.assertFn("SELECT Fn.dt('not a date', fail_on_error:=TRUE)", datetime.fromisoformat("1970-01-01T00:00:00"))

        # iso string that can be formatted
        self.assertFn("SELECT Fn.dt('2024/04/06', '%Y/%m/%d')", datetime.fromisoformat("2024-04-06T00:00:00"))

    def test_dt_isoformat(self):
        self.assertFn("SELECT Fn.dt_isoformat(TIMESTAMP '2024-04-01 00:00:00')", "2024-04-01 00:00:00")

    def test_dt_quarter(self):
        self.assertFn("SELECT Fn.dt_quarter('2024-01-01')", 1)
        self.assertFn("SELECT Fn.dt_quarter('2024-06-15')", 2)
        self.assertFn("SELECT Fn.dt_quarter('2024-10-10')", 4)

    def test_calendar_structure(self):
        rows = self.con.execute("SELECT * FROM Fn.calendar() LIMIT 5").fetchall()
        self.assertEqual(len(rows[0]), 15)  # calendar returns 15 fields


if __name__ == "__main__":
    unittest.main()
