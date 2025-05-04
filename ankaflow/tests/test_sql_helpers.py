import unittest
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
        try:
            validate_simple_query(
                "SELECT id, name FROM users", ranking_enabled=True
            )
        except ValueError:
            self.fail("validate_simple_query raised unexpectedly.")

    def test_cte_raises_error(self):
        with self.assertRaises(ValueError) as context:
            validate_simple_query(
                "WITH temp AS (SELECT * FROM users) SELECT * FROM temp",
                ranking_enabled=True,
            )
        self.assertIn("CTEs are not supported", str(context.exception))

    def test_group_by_raises_error(self):
        with self.assertRaises(ValueError) as context:
            validate_simple_query(
                "SELECT id, COUNT(*) FROM users GROUP BY id",
                ranking_enabled=True,
            )
        self.assertIn("GROUP BY is not supported", str(context.exception))

    def test_aggregate_function_raises_error(self):
        for agg in ["SUM", "AVG", "COUNT", "MIN", "MAX"]:
            query = f"SELECT {agg}(amount) FROM users"
            with self.subTest(agg=agg):
                with self.assertRaises(ValueError) as context:
                    validate_simple_query(query, ranking_enabled=True)
                self.assertIn(
                    "Aggregate functions are not allowed",
                    str(context.exception),
                )

    def test_ranking_disabled_allows_aggregates(self):
        try:
            validate_simple_query(
                "SELECT SUM(amount) FROM users", ranking_enabled=False
            )
        except ValueError:
            self.fail(
                "validate_simple_query raised unexpectedly for aggregates without ranking."  # noqa: E501
            )
