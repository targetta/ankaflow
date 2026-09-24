import unittest
import pyarrow as pa
from ..common.util import (
    duckdb_to_pyarrow_type,
    enforce_and_qualify_sql,
    set_mapping_value,
    parse_rfc8288_link,
)


class TestDuckDBToPyArrowType(unittest.TestCase):
    def test_simple_types(self):
        # Test simple types
        self.assertEqual(duckdb_to_pyarrow_type("VARCHAR"), pa.string())
        self.assertEqual(duckdb_to_pyarrow_type("INTEGER"), pa.int32())
        self.assertEqual(duckdb_to_pyarrow_type("BIGINT"), pa.int64())
        self.assertEqual(duckdb_to_pyarrow_type("DOUBLE"), pa.float64())
        self.assertEqual(duckdb_to_pyarrow_type("BOOLEAN"), pa.bool_())
        self.assertEqual(duckdb_to_pyarrow_type("DATE"), pa.date32())
        self.assertEqual(
            duckdb_to_pyarrow_type("TIMESTAMP"), pa.timestamp("ns")
        )  # noqa: E501
        self.assertEqual(duckdb_to_pyarrow_type("JSON"), pa.string())
        self.assertEqual(duckdb_to_pyarrow_type("UUID"), pa.string())
        self.assertEqual(
            duckdb_to_pyarrow_type("DECIMAL"), pa.decimal128(38, 18)
        )  # noqa: E501

    def test_list_types(self):
        # Test LIST types
        self.assertEqual(
            duckdb_to_pyarrow_type("LIST(INTEGER)"), pa.list_(pa.int32())
        )  # noqa: E501
        self.assertEqual(
            duckdb_to_pyarrow_type("LIST(VARCHAR)"), pa.list_(pa.string())
        )  # noqa: E501
        # TODO: non-recursive regex patterns is insufficient
        # self.assertEqual(duckdb_to_pyarrow_type("LIST(LIST(INTEGER))"), pa.list_(pa.list_(pa.int32())))  # noqa: E501

    def test_list_bracket_notation(self):
        # Test LIST[] bracket notation
        self.assertEqual(
            duckdb_to_pyarrow_type("INTEGER[]"), pa.list_(pa.int32())
        )  # noqa: E501
        self.assertEqual(
            duckdb_to_pyarrow_type("VARCHAR[]"), pa.list_(pa.string())
        )  # noqa: E501
        # TODO: non-recursive regex patterns is insufficient
        # self.assertEqual(duckdb_to_pyarrow_type("LIST(INTEGER)[]"), pa.list_(pa.list_(pa.int32())))  # noqa: E501

    def test_struct_types(self):
        # Test STRUCT types
        expected_struct = pa.struct([
            ("field1", pa.int32()),
            ("field2", pa.string()),
        ])
        self.assertEqual(
            duckdb_to_pyarrow_type(
                'STRUCT("field1" INTEGER, "field2" VARCHAR)'
            ),  # noqa: E501
            expected_struct,
        )

    def test_struct_bracket_notation(self):
        # Test STRUCT[] bracket notation
        expected_struct_list = pa.list_(
            pa.struct([("field1", pa.int32()), ("field2", pa.string())])
        )
        self.assertEqual(
            duckdb_to_pyarrow_type(
                'STRUCT("field1" INTEGER, "field2" VARCHAR)[]'
            ),  # noqa: E501
            expected_struct_list,
        )

    # TODO: non-recursive regex patterns is insufficient
    # def test_nested_list_and_struct(self):
    #     # Test nested LIST and STRUCT
    #     expected_nested = pa.list_(pa.struct([
    #         ("field1", pa.int32()),
    #         ("field2", pa.string())
    #     ]))
    #     self.assertEqual(
    #         duckdb_to_pyarrow_type('LIST(STRUCT("field1" INTEGER, "field2" VARCHAR))'),  # noqa: E501
    #         expected_nested
    #     )

    def test_unsupported_type(self):
        # Test unsupported type
        with self.assertRaises(ValueError):
            duckdb_to_pyarrow_type("UNSUPPORTED_TYPE")


class TestEnforceTableQualification(unittest.TestCase):
    def test_qualified_table_raises(self):
        """Should raise ValueError when query includes explicit project (p.d.t)."""  # noqa: E501
        sql = "SELECT x FROM p.d.t"
        with self.assertRaises(ValueError) as ctx:
            enforce_and_qualify_sql(sql, "d", "bigquery")

        self.assertIn("Cross-catalog isolation violation", str(ctx.exception))  # noqa: E501

    def test_partially_qualified_table_raises(self):
        """Should raise ValueError when query includes explicit project (p.d.t)."""  # noqa: E501
        sql = "SELECT x FROM bad.t"
        with self.assertRaises(ValueError) as ctx:
            enforce_and_qualify_sql(sql, "d", "bigquery")

        self.assertIn("Cross-database isolation violation", str(ctx.exception))  # noqa: E501

    def test_nested_qualification_raises(self):
        """Should raise ValueError when query includes explicit project (p.d.t)."""  # noqa: E501
        sql = "CREATE OR REPLACE VIEW my_view AS SELECT x FROM bad.my_table"
        with self.assertRaises(ValueError) as ctx:
            enforce_and_qualify_sql(sql, "d", "bigquery")

        self.assertIn("Cross-database isolation violation", str(ctx.exception))  # noqa: E501

    def test_table_qualified(self):
        """Should raise ValueError when DDL includes explicit project."""
        sql = "CREATE OR REPLACE VIEW my_view AS SELECT x FROM my_table"
        expected = (
            "CREATE OR REPLACE VIEW d.my_view AS SELECT x FROM d.my_table"
        )
        result = enforce_and_qualify_sql(sql, "d", "bigquery")

        self.assertEqual(result, expected)

    def test_validate_unqualified_tables_pass(self):
        """Should pass without error when only dataset.table or table is specified."""  # noqa: E501
        valid_queries = [
            "SELECT x FROM t",
            "CREATE OR REPLACE VIEW my_view AS SELECT x FROM _dim_catalog",
            "INSERT INTO target_table SELECT * FROM source_table",
        ]
        for sql in valid_queries:
            with self.subTest(sql=sql):
                try:
                    enforce_and_qualify_sql(sql, "d", "bigquery")
                except ValueError as e:
                    self.fail(f"validate_no_explicit_project raised ValueError unexpectedly for '{sql}': {e}")  # noqa: E501


class TestMappingUtils(unittest.TestCase):
    def test_set_value_on_none_target_creates_dict(self):
        result = set_mapping_value(None, "cursor", "zoo")
        self.assertEqual(result, {"cursor": "zoo"})

    def test_set_nested_value(self):
        target = {}
        result = set_mapping_value(
            target, "meta.pagination.cursor", "zoo"
        )
        self.assertEqual(
            result, {"meta": {"pagination": {"cursor": "zoo"}}}
        )


class TestRFC8288Parser(unittest.TestCase):
    def test_parse_valid_rel_next_header(self):
        header = '<https://api.example.com/v1/items?page=2>; rel="next"'
        url = parse_rfc8288_link(header, rel="next")
        self.assertEqual(url, "https://api.example.com/v1/items?page=2")

    def test_returns_none_when_rel_next_missing(self):
        header = '<https://api.example.com/v1/items?page=1>; rel="prev"'
        url = parse_rfc8288_link(header, rel="next")
        self.assertIsNone(url)


if __name__ == "__main__":
    unittest.main()
