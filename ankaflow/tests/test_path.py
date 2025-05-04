import unittest
from ..common.path import (
    LocalPath,
    S3Path,
    GSPath,
    HTTPPath,
    FTPPath,
    PathFactory
)


class TestUniversalPath(unittest.TestCase):
    def test_local_path(self):
        path = PathFactory.make("my_file.txt")
        self.assertIsInstance(path, LocalPath)
        self.assertEqual(str(path), "my_file.txt")

    def test_s3_path(self):
        path = PathFactory.make("s3://my-bucket/my-folder/my_file.txt")
        self.assertIsInstance(path, S3Path)
        self.assertEqual(str(path), "s3://my-bucket/my-folder/my_file.txt")
        self.assertEqual(path.bucket, "my-bucket") # type: ignore
        self.assertEqual(path.key, "my-folder/my_file.txt") # type: ignore
        self.assertEqual(path.name, "my_file.txt")

    def test_gs_path(self):
        path = PathFactory.make("gs://my-bucket/my-folder/my_file.txt")
        self.assertIsInstance(path, GSPath)
        self.assertEqual(str(path), "gs://my-bucket/my-folder/my_file.txt")
        self.assertEqual(path.bucket, "my-bucket") # type: ignore
        self.assertEqual(path.key, "my-folder/my_file.txt") # type: ignore
        self.assertEqual(path.name, "my_file.txt")

    def test_http_path(self):
        path = PathFactory.make("http://example.com/my_file.txt")
        self.assertIsInstance(path, HTTPPath)
        self.assertEqual(str(path), "http://example.com/my_file.txt")
        self.assertEqual(path.netloc, "example.com") # type: ignore
        self.assertEqual(path.path_part, "/my_file.txt") # type: ignore
        self.assertEqual(path.name, "my_file.txt")

    def test_ftp_path(self):
        path = PathFactory.make("ftp://example.com/my_file.txt")
        self.assertIsInstance(path, FTPPath)
        self.assertEqual(str(path), "ftp://example.com/my_file.txt")
        self.assertEqual(path.bucket, "example.com") # type: ignore
        self.assertEqual(path.key, "my_file.txt") # type: ignore
        self.assertEqual(path.name, "my_file.txt")

    def test_file_uri_path(self):
        path = PathFactory.make("file:///tmp/test.txt")
        self.assertIsInstance(path, LocalPath)
        self.assertEqual(str(path), "/tmp/test.txt")

    def test_relative_local_path(self):
        path = PathFactory.make("relative/path/file.txt")
        self.assertIsInstance(path, LocalPath)
        self.assertEqual(str(path), "relative/path/file.txt")

    def test_s3_path_div(self):
        path = PathFactory.make("s3://my-bucket/my-folder") / "my_file.txt" # type: ignore
        self.assertIsInstance(path, S3Path)
        self.assertEqual(str(path), "s3://my-bucket/my-folder/my_file.txt")

    def test_gs_path_div(self):
        path = PathFactory.make("gs://my-bucket/my-folder") / "my_file.txt" # type: ignore
        self.assertIsInstance(path, GSPath)
        self.assertEqual(str(path), "gs://my-bucket/my-folder/my_file.txt")

    def test_http_path_div(self):
        path = PathFactory.make("http://example.com/my-folder") / "my_file.txt" # type: ignore
        self.assertIsInstance(path, HTTPPath)
        self.assertEqual(str(path), "http://example.com/my-folder/my_file.txt")

    def test_ftp_path_div(self):
        path = PathFactory.make("ftp://example.com/my-folder") / "my_file.txt" # type: ignore
        self.assertIsInstance(path, FTPPath)
        self.assertEqual(str(path), "ftp://example.com/my-folder/my_file.txt")

    def test_s3_path_parent(self):
        path = PathFactory.make("s3://my-bucket/my-folder/my_file.txt")
        self.assertEqual(str(path.parent), "s3://my-bucket/my-folder")

    def test_gs_path_parent(self):
        path = PathFactory.make("gs://my-bucket/my-folder/my_file.txt")
        self.assertEqual(str(path.parent), "gs://my-bucket/my-folder")

    def test_http_path_parent(self):
        path = PathFactory.make("http://example.com/my-folder/my_file.txt")
        self.assertEqual(str(path.parent), "http://example.com/my-folder")

    def test_ftp_path_parent(self):
        path = PathFactory.make("ftp://example.com/my-folder/my_file.txt")
        self.assertEqual(str(path.parent), "ftp://example.com/my-folder")

    def test_s3_path_parts(self):
        path = PathFactory.make("s3://my-bucket/my-folder/my_file.txt")
        self.assertEqual(
            path.parts, ("s3:/", "my-bucket", "my-folder", "my_file.txt")
        )

    def test_gs_path_parts(self):
        path = PathFactory.make("gs://my-bucket/my-folder/my_file.txt")
        self.assertEqual(
            path.parts, ("gs:/", "my-bucket", "my-folder", "my_file.txt")
        )

    def test_http_path_parts(self):
        path = PathFactory.make("http://example.com/my-folder/my_file.txt")
        self.assertEqual(
            path.parts, ("http:/", "example.com", "my-folder", "my_file.txt")
        )

    def test_ftp_path_parts(self):
        path = PathFactory.make("ftp://example.com/my-folder/my_file.txt")
        self.assertEqual(
            path.parts, ("ftp:/", "example.com", "my-folder", "my_file.txt")
        )

    def test_s3_path_joinpath(self):
        path = PathFactory.make("s3://my-bucket/my-folder").joinpath(
            "subfolder", "my_file.txt"
        )
        self.assertEqual(
            str(path), "s3://my-bucket/my-folder/subfolder/my_file.txt"
        )

    def test_gs_path_joinpath(self):
        path = PathFactory.make("gs://my-bucket/my-folder").joinpath(
            "subfolder", "my_file.txt"
        )
        self.assertEqual(
            str(path), "gs://my-bucket/my-folder/subfolder/my_file.txt"
        )

    def test_http_path_joinpath(self):
        path = PathFactory.make("http://example.com/my-folder").joinpath(
            "subfolder", "my_file.txt"
        )
        self.assertEqual(
            str(path), "http://example.com/my-folder/subfolder/my_file.txt"
        )

    def test_ftp_path_joinpath(self):
        path = PathFactory.make("ftp://example.com/my-folder").joinpath(
            "subfolder", "my_file.txt"
        )
        self.assertEqual(
            str(path), "ftp://example.com/my-folder/subfolder/my_file.txt"
        )

    def test_s3_path_anchor(self):
        path = PathFactory.make("s3://my-bucket/my-folder/my_file.txt")
        self.assertEqual(path.anchor, "s3://my-bucket")

    def test_gs_path_anchor(self):
        path = PathFactory.make("gs://my-bucket/my-folder/my_file.txt")
        self.assertEqual(path.anchor, "gs://my-bucket")

    def test_s3_get_local(self):
        path = PathFactory.make("s3://bucket/folder/file.txt")
        self.assertEqual(path.get_local("/test"), "/test/bucket/folder/file.txt")

    def test_gs_get_local(self):
        path = PathFactory.make("gs://bucket/folder/file.txt")
        self.assertEqual(path.get_local("/test"), "/test/bucket/folder/file.txt")

    def test_http_get_local(self):
        path = PathFactory.make("https://ex.com/folder/file.txt")
        self.assertEqual(path.get_local("/test"), "/test/ex.com/folder/file.txt")
