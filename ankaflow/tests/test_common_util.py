import unittest
import pyarrow as pa
from ..common.util import duckdb_to_pyarrow_type


class TestDuckDBToPyArrowType(unittest.TestCase):
    def test_simple_types(self):
        # Test simple types
        self.assertEqual(duckdb_to_pyarrow_type("VARCHAR"), pa.string())
        self.assertEqual(duckdb_to_pyarrow_type("INTEGER"), pa.int32())
        self.assertEqual(duckdb_to_pyarrow_type("BIGINT"), pa.int64())
        self.assertEqual(duckdb_to_pyarrow_type("DOUBLE"), pa.float64())
        self.assertEqual(duckdb_to_pyarrow_type("BOOLEAN"), pa.bool_())
        self.assertEqual(duckdb_to_pyarrow_type("DATE"), pa.date32())
        self.assertEqual(duckdb_to_pyarrow_type("TIMESTAMP"), pa.timestamp("ns"))
        self.assertEqual(duckdb_to_pyarrow_type("JSON"), pa.string())
        self.assertEqual(duckdb_to_pyarrow_type("UUID"), pa.string())
        self.assertEqual(duckdb_to_pyarrow_type("DECIMAL"), pa.decimal128(38, 18))

    def test_list_types(self):
        # Test LIST types
        self.assertEqual(duckdb_to_pyarrow_type("LIST(INTEGER)"), pa.list_(pa.int32()))
        self.assertEqual(duckdb_to_pyarrow_type("LIST(VARCHAR)"), pa.list_(pa.string()))
        # TODO: non-recursive regex patterns is insufficient 
        # self.assertEqual(duckdb_to_pyarrow_type("LIST(LIST(INTEGER))"), pa.list_(pa.list_(pa.int32())))  # noqa: E501

    def test_list_bracket_notation(self):
        # Test LIST[] bracket notation
        self.assertEqual(duckdb_to_pyarrow_type("INTEGER[]"), pa.list_(pa.int32()))
        self.assertEqual(duckdb_to_pyarrow_type("VARCHAR[]"), pa.list_(pa.string()))
        # TODO: non-recursive regex patterns is insufficient 
        # self.assertEqual(duckdb_to_pyarrow_type("LIST(INTEGER)[]"), pa.list_(pa.list_(pa.int32())))  # noqa: E501

    def test_struct_types(self):
        # Test STRUCT types
        expected_struct = pa.struct([
            ("field1", pa.int32()),
            ("field2", pa.string())
        ])
        self.assertEqual(
            duckdb_to_pyarrow_type('STRUCT("field1" INTEGER, "field2" VARCHAR)'),
            expected_struct
        )

    def test_struct_bracket_notation(self):
        # Test STRUCT[] bracket notation
        expected_struct_list = pa.list_(pa.struct([
            ("field1", pa.int32()),
            ("field2", pa.string())
        ]))
        self.assertEqual(
            duckdb_to_pyarrow_type('STRUCT("field1" INTEGER, "field2" VARCHAR)[]'),
            expected_struct_list
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
