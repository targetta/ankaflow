import unittest
import sqlglot
from sqlglot import exp

from ..common.util import build_ranked_query, validate_simple_query


class TestBuildRankedQuery(unittest.TestCase):
    def test_no_ranking_applied(self):
        sql, where = build_ranked_query(
            query="SELECT id FROM users",
            selectable="t",
            version=None,
            keys=None,
            dialect="duckdb",
        )
        self.assertIn('FROM "t"', sql)
        self.assertNotIn("__rank__", sql)
        self.assertEqual(where, "")

    def test_ranking_injected_correctly(self):
        sql, where = build_ranked_query(
            query="SELECT id FROM users",
            selectable="t",
            version="updated_at",
            keys=["id"],
            dialect="duckdb",
        )
        self.assertIn("ROW_NUMBER()", sql)
        self.assertIn("OVER (PARTITION BY", sql)
        self.assertIn('ORDER BY "updated_at" DESC', sql)
        self.assertIn("SELECT * FROM", sql)
        self.assertEqual(where, 'WHERE "__rank__" = 1')

    def test_ranking_with_multiple_keys(self):
        sql, where = build_ranked_query(
            query="SELECT id FROM users",
            selectable="t",
            version="ts",
            keys=["id", "region"],
            dialect="duckdb",
        )
        self.assertIn('PARTITION BY "id", "region"', sql)
        self.assertEqual(where, 'WHERE "__rank__" = 1')


class TestValidateSimpleQuery(unittest.TestCase):
    def test_valid_simple_select_passes(self):
        tree = sqlglot.parse_one("SELECT id, name FROM users")
        try:
            validate_simple_query(tree, ranking_enabled=True)
        except ValueError:
            self.fail("validate_simple_query raised unexpectedly.")

    def test_cte_raises_error(self):
        tree = sqlglot.parse_one(
            "WITH temp AS (SELECT * FROM users) SELECT * FROM temp")
        with self.assertRaises(ValueError) as context:
            validate_simple_query(
                tree,
                ranking_enabled=True,
            )
        self.assertIn("CTEs are not allowed", str(context.exception))

    def test_group_by_raises_error(self):
        tree = sqlglot.parse_one("SELECT id, COUNT(*) FROM users GROUP BY id")
        with self.assertRaises(ValueError) as context:
            validate_simple_query(
                tree,
                ranking_enabled=True,
            )
        self.assertIn("GROUP BY are not allowed", str(context.exception))

    def test_correct_table_simple(self):
        """Should successfully return the table node for a basic SELECT."""
        sql = "SELECT * FROM tbl"
        parsed = sqlglot.parse_one(sql)

        res = validate_simple_query(parsed, ranking_enabled=False)

        self.assertIsInstance(res, exp.Table)
        self.assertEqual(res.name, "tbl")
        # Alternative verification using the generated SQL
        self.assertEqual(res.sql(), "tbl")

    def test_correct_table_with_where_clause(self):
        """Should extract the table correctly even when filters are present."""
        sql = "SELECT a, b FROM acute_data WHERE c = 3 AND d IS NOT NULL"
        parsed = sqlglot.parse_one(sql)

        res = validate_simple_query(parsed, ranking_enabled=False)

        self.assertEqual(res.name, "acute_data")

    # ==========================================
    # 2. Tests for Correct Errors Raised
    # ==========================================

    def test_raises_error_for_ctes(self):
        """Should raise ValueError if the query uses a WITH clause."""
        sql = "WITH cte AS (SELECT * FROM tbl) SELECT * FROM cte"
        parsed = sqlglot.parse_one(sql)

        with self.assertRaises(ValueError) as ctx:
            validate_simple_query(parsed, ranking_enabled=False)

        self.assertIn(
            "CTEs are not allowed in delta scan source queries.",
            str(ctx.exception),
        )

    def test_raises_error_for_no_tables(self):
        """Should raise ValueError if the query selects from nothing."""
        sql = "SELECT 1 + 1"
        parsed = sqlglot.parse_one(sql)

        with self.assertRaises(ValueError) as ctx:
            validate_simple_query(parsed, ranking_enabled=False)

        self.assertIn("Query must specify a source table to swap.", str(ctx.exception))  # noqa: E501

    def test_raises_error_for_multiple_tables_join(self):
        """Should raise ValueError if the query joins multiple tables."""
        sql = "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id"
        parsed = sqlglot.parse_one(sql)

        with self.assertRaises(ValueError) as ctx:
            validate_simple_query(parsed, ranking_enabled=False)

        self.assertIn(
            "Multi-table queries (joins/subqueries) are not allowed.",
            str(ctx.exception),
        )

    def test_raises_error_for_multiple_tables_subquery(self):
        """Should raise ValueError if a table is nested inside a subquery."""
        sql = "SELECT * FROM (SELECT * FROM inner_tbl) AS outer_tbl"
        parsed = sqlglot.parse_one(sql)
        # Note: tree.find_all(exp.Table) will find both 'inner' and 'outer'

        with self.assertRaises(ValueError) as ctx:
            validate_simple_query(parsed, ranking_enabled=False)

        self.assertIn(
            "Multi-table queries (joins/subqueries) are not allowed.",
            str(ctx.exception),
        )

    def test_raises_error_for_groupby_when_ranking_enabled(self):
        """Should raise if GROUP BY is used while ranking_enabled is True."""
        sql = "SELECT a, COUNT(*) FROM tbl GROUP BY a"
        parsed = sqlglot.parse_one(sql)

        with self.assertRaises(ValueError) as ctx:
            validate_simple_query(parsed, ranking_enabled=True)

        self.assertIn(
            "Aggregations and GROUP BY are not allowed when ranking is applied.",  # noqa: E501
            str(ctx.exception),
        )

    def test_allows_groupby_when_ranking_disabled(self):
        """Should pass with a GROUP BY if ranking_enabled is False."""
        sql = "SELECT a, COUNT(*) FROM tbl GROUP BY a"
        parsed = sqlglot.parse_one(sql)

        res = validate_simple_query(parsed, ranking_enabled=False)
        self.assertEqual(res.name, "tbl")

    def test_raises_error_for_aggregates_when_ranking_enabled(self):
        """Should raise if an aggr func is used while ranking_enabled is True."""
        sql = "SELECT MAX(version) FROM tbl"
        parsed = sqlglot.parse_one(sql)

        with self.assertRaises(ValueError) as ctx:
            validate_simple_query(parsed, ranking_enabled=True)

        self.assertIn(
            "Aggregations and GROUP BY are not allowed when ranking is applied.",  # noqa: E501
            str(ctx.exception),
        )

    def test_allows_aggregates_when_ranking_disabled(self):
        """Should pass with an aggregate if ranking_enabled is False."""
        sql = "SELECT MAX(version) FROM tbl"
        parsed = sqlglot.parse_one(sql)

        res = validate_simple_query(parsed, ranking_enabled=False)
        self.assertEqual(res.name, "tbl")