# type: ignore
import unittest
import tempfile
from io import StringIO, FileIO

from ..common.filesystem import LocalFileSystem, FileSystem


class TestLocalFileSystem(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.fs = LocalFileSystem(self.temp_dir.name)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_save_and_get_file_text(self):
        filename = "test.txt"
        content = "Hello, world!"
        metadata = self.fs.save_file(filename, content, "text/plain")
        self.assertEqual(metadata["name"], filename)
        file_data = self.fs.get_file(filename)
        self.assertIsNotNone(file_data)
        self.assertEqual(file_data["content"], content)
        self.assertEqual(file_data["mime_type"], "text/plain")

    def test_save_and_get_file_bytes(self):
        filename = "binary.bin"
        content = b"\x00\x01\x02"
        metadata = self.fs.save_file(
            filename, content, "application/octet-stream"
        )
        self.assertEqual(metadata["name"], filename)
        file_data = self.fs.get_file(filename, mode="application/octet-stream")
        self.assertIsNotNone(file_data)
        self.assertEqual(file_data["content"], content)
        self.assertEqual(file_data["mime_type"], "application/octet-stream")

    def test_get_string_io(self):
        filename = "test_io.txt"
        content = "Sample text content."
        self.fs.save_file(filename, content, "text/plain")
        sio = self.fs.get_string_io(filename)
        self.assertIsInstance(sio, StringIO)
        self.assertEqual(sio.getvalue(), content)

    def test_get_file_io(self):
        filename = "test_io.bin"
        content = b"binary data"
        self.fs.save_file(filename, content, "application/octet-stream")
        fio = self.fs.get_file_io(filename)
        self.assertIsInstance(fio, FileIO)
        data = fio.read()
        self.assertEqual(data, content)
        fio.close()

    def test_list_files(self):
        files = {
            "file1.txt": "Content 1",
            "file2.txt": "Content 2",
            "file3.bin": b"Data",
        }
        for fname, content in files.items():
            mime = (
                "text/plain"
                if isinstance(content, str)
                else "application/octet-stream"
            )
            self.fs.save_file(fname, content, mime)
        listed_files = self.fs.list_files("*.txt")
        self.assertEqual(len(listed_files), 2)
        names = [f["name"] for f in listed_files]
        self.assertIn("file1.txt", names)
        self.assertIn("file2.txt", names)

    def test_delete_file(self):
        filename = "delete_me.txt"
        content = "Delete me!"
        self.fs.save_file(filename, content, "text/plain")
        self.assertTrue(self.fs.delete_file(filename))
        # Deleting a non-existent file should return False.
        self.assertFalse(self.fs.delete_file(filename))

    def test_delete_files(self):
        files = {
            "file1.txt": "Content 1",
            "file2.txt": "Content 2",
            "file3.bin": b"Data",
        }
        for fname, content in files.items():
            mime = (
                "text/plain"
                if isinstance(content, str)
                else "application/octet-stream"
            )
            self.fs.save_file(fname, content, mime)
        count = self.fs.delete_files("*.txt")
        self.assertEqual(count, 2)
        remaining = self.fs.list_files("*")
        self.assertEqual(len(remaining), 1)
        self.assertEqual(remaining[0]["name"], "file3.bin")


class TestFileSystem(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        # Ensure local behavior for testing.
        global IS_PYODIDE
        IS_PYODIDE = False
        self.fs = FileSystem(self.temp_dir.name)

    def tearDown(self):
        self.temp_dir.cleanup()

    async def test_save_and_get_file_text(self):
        filename = "async_test.txt"
        content = "Async Hello, world!"
        metadata = await self.fs.save_file(filename, content, "text/plain")
        self.assertEqual(metadata["name"], filename)
        file_data = await self.fs.get_file(filename)
        self.assertIsNotNone(file_data)
        self.assertEqual(file_data["content"], content)
        self.assertEqual(file_data["mime_type"], "text/plain")

    async def test_save_and_get_file_bytes(self):
        filename = "async_binary.bin"
        content = b"\x10\x20\x30"
        metadata = await self.fs.save_file(
            filename, content, "application/octet-stream"
        )
        self.assertEqual(metadata["name"], filename)
        file_data = await self.fs.get_file(filename,
                                           mode="application/octet-stream")
        self.assertIsNotNone(file_data)
        self.assertEqual(file_data["content"], content)
        self.assertEqual(file_data["mime_type"], "application/octet-stream")

    async def test_get_string_io(self):
        filename = "async_io.txt"
        content = "Async sample text."
        await self.fs.save_file(filename, content, "text/plain")
        sio = await self.fs.get_string_io(filename)
        self.assertIsInstance(sio, StringIO)
        self.assertEqual(sio.getvalue(), content)

    async def test_get_file_io(self):
        filename = "async_io.bin"
        content = b"Async binary data"
        await self.fs.save_file(filename, content, "application/octet-stream")
        fio = await self.fs.get_file_io(filename)
        self.assertIsInstance(fio, FileIO)
        data = fio.read()
        self.assertEqual(data, content)
        fio.close()

    async def test_list_files(self):
        files = {
            "async_file1.txt": "Content A",
            "async_file2.txt": "Content B",
            "async_file3.bin": b"Data",
        }
        for fname, content in files.items():
            mime = (
                "text/plain"
                if isinstance(content, str)
                else "application/octet-stream"
            )
            await self.fs.save_file(fname, content, mime)
        listed_files = await self.fs.list_files("*.txt")
        self.assertEqual(len(listed_files), 2)
        names = [f["name"] for f in listed_files]
        self.assertIn("async_file1.txt", names)
        self.assertIn("async_file2.txt", names)

    async def test_delete_file(self):
        filename = "async_delete.txt"
        content = "Delete me async!"
        await self.fs.save_file(filename, content, "text/plain")
        self.assertTrue(await self.fs.delete_file(filename))
        self.assertFalse(await self.fs.delete_file(filename))

    async def test_delete_files(self):
        files = {
            "async_file1.txt": "Content A",
            "async_file2.txt": "Content B",
            "async_file3.bin": b"Data",
        }
        for fname, content in files.items():
            mime = (
                "text/plain"
                if isinstance(content, str)
                else "application/octet-stream"
            )
            await self.fs.save_file(fname, content, mime)
        count = await self.fs.delete_files("*.txt")
        self.assertEqual(count, 2)
        remaining = await self.fs.list_files("*")
        self.assertEqual(len(remaining), 1)
        self.assertEqual(remaining[0]["name"], "async_file3.bin")


if __name__ == "__main__":
    unittest.main()
