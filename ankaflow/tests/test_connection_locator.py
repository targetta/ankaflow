import unittest

from ..connections.connection import Locator  # Adjust import as needed
from ..common.path import S3Path, LocalPath
from ..models.configs import ConnectionConfiguration, S3Config


class LocatorTest(unittest.TestCase):
    def make_cfg(self, bucket, prefix="", wildcard=None):
        return ConnectionConfiguration(
            s3=S3Config(
                bucket=bucket, data_prefix=prefix, locator_wildcard=wildcard
            )
        )

    def test_relative_path_with_prefix(self):
        cfg = self.make_cfg("s3://my-bucket", "sales")
        locator = Locator(cfg)
        result = locator.locate("2024/region.parquet")
        self.assertIsInstance(result, S3Path)
        self.assertEqual(
            str(result), "s3://my-bucket/sales/2024/region.parquet"
        )

    def test_absolute_s3_path(self):
        cfg = self.make_cfg("s3://my-bucket", "ignored-prefix")
        locator = Locator(cfg)
        result = locator.locate("s3://my-bucket/path/to/data.parquet")
        self.assertIsInstance(result, S3Path)
        self.assertEqual(str(result), "s3://my-bucket/path/to/data.parquet")

    def test_absolute_local_path_converted_to_relative(self):
        cfg = self.make_cfg("/data/bucket", "prefix")
        locator = Locator(cfg)
        result = locator.locate("/absolute/path.csv")
        self.assertIsInstance(result, LocalPath)
        self.assertEqual(str(result), "/data/bucket/prefix/absolute/path.csv")

    def test_relative_path_no_prefix(self):
        cfg = self.make_cfg("s3://my-bucket")
        locator = Locator(cfg)
        result = locator.locate("simple.csv")
        self.assertEqual(str(result), "s3://my-bucket/simple.csv")

    def test_wildcard_replacement_applied(self):
        cfg = self.make_cfg(
            "s3://my-bucket", "prefix", wildcard=(r"\$", "2024_")
        )
        locator = Locator(cfg)
        result = locator.locate("sales$.parquet", use_wildcard=True)
        self.assertEqual(
            str(result), "s3://my-bucket/prefix/sales2024_.parquet"
        )

    def test_invalid_bucket_raises(self):
        cfg = self.make_cfg("relative/path")
        locator = Locator(cfg)
        with self.assertRaises(ValueError):
            locator.locate("foo.csv")


if __name__ == "__main__":
    unittest.main()
