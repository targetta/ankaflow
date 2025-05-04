import typing as t
import pathlib
import sys
from io import FileIO, StringIO
from typing import Optional, Dict, List, Union

IS_PYODIDE = sys.platform == "emscripten"


@t.runtime_checkable
class BaseFileSystem(t.Protocol):
    @property
    def root_path(self) -> pathlib.Path:
        """The root path under which all file operations are scoped."""
        ...

    def save_file(
        self,
        filename: str,
        content: Union[str, bytes],
        mime_type: str = "application/octet-stream",
    ) -> Dict[str, Union[str, float]]:
        """Saves content to a file and returns metadata."""
        ...

    def get_file(
        self, filename: str, mode: str = "text/plain"
    ) -> Optional[Dict[str, Union[str, bytes, float]]]:
        """Returns file content and metadata, or None if not found."""
        ...

    def list_files(self, glob_pattern: str = "*") -> List[Dict[str, Union[str, float]]]:
        """Returns a list of file metadata matching the pattern."""
        ...

    def get_string_io(self, filename: str) -> StringIO:
        """Returns a StringIO for the specified file."""
        ...

    def get_file_io(self, filename: str) -> FileIO:
        """Returns a FileIO object for the specified file."""
        ...

    def delete_file(self, filename: str) -> bool:
        """Deletes a single file and returns whether it was successful."""
        ...

    def delete_files(self, glob_pattern: str) -> int:
        """Deletes all files matching the pattern and returns the count."""
        ...


class BrowserFileSystem:
    """
    A class for managing files within Pyodide's virtual filesystem.
    Currently uses /tmp as the working root.
    Future versions may switch to IndexedDB or native File System Access API.
    """

    def __init__(self, root_path: str = "/tmp"):
        self.root_path = pathlib.Path(root_path)
        self.root_path.mkdir(parents=True, exist_ok=True)

    def _get_full_path(self, filename: str) -> pathlib.Path:
        return self.root_path / filename

    def save_file(
        self,
        filename: str,
        content: Union[str, bytes],
        mime_type: str = "application/octet-stream",
    ) -> Dict[str, Union[str, float]]:
        full_path = self._get_full_path(filename)
        full_path.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(content, str):
            full_path.write_text(content, encoding="utf-8")
        elif isinstance(content, bytes):
            full_path.write_bytes(content)
        else:
            raise TypeError(f"Unsupported content type: {type(content)}")
        stat_result = full_path.stat()
        return {
            "name": str(full_path.relative_to(self.root_path)),
            "created": stat_result.st_ctime,
            "modified": stat_result.st_mtime,
            "mime_type": mime_type,
        }

    def get_file(
        self, filename: str, mode: str = "text/plain"
    ) -> Optional[Dict[str, Union[str, bytes, float]]]:
        full_path = self._get_full_path(filename)
        if not full_path.is_file():
            return None

        stat_result = full_path.stat()
        if mode == "application/octet-stream":
            content = full_path.read_bytes()
        elif mode == "text/plain":
            try:
                content = full_path.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                content = full_path.read_bytes()
                mode = "application/octet-stream"
        else:
            raise ValueError(f"Invalid mode: {mode}")

        return {
            "name": str(full_path.relative_to(self.root_path)),
            "content": content,
            "created": stat_result.st_ctime,
            "modified": stat_result.st_mtime,
            "mime_type": mode,
        }

    def get_string_io(self, filename: str) -> StringIO:
        full_path = self._get_full_path(filename)
        if full_path.is_file():
            return StringIO(full_path.read_text(encoding="utf-8"))
        raise FileNotFoundError(f"File '{filename}' not found under '{self.root_path}'")

    def get_file_io(self, filename: str) -> FileIO:
        full_path = self._get_full_path(filename)
        if full_path.is_file():
            return FileIO(str(full_path), "rb")
        raise FileNotFoundError(f"File '{filename}' not found under '{self.root_path}'")

    def list_files(self, glob_pattern: str = "*") -> List[Dict[str, Union[str, float]]]:
        files = []
        for path in self.root_path.glob(glob_pattern):
            if path.is_file():
                stat_result = path.stat()
                try:
                    path.read_text(encoding="utf-8")
                    file_type = "text/plain"
                except UnicodeDecodeError:
                    file_type = "application/octet-stream"
                files.append(
                    {
                        "name": str(path.relative_to(self.root_path)),
                        "created": stat_result.st_ctime,
                        "modified": stat_result.st_mtime,
                        "mime_type": file_type,
                    }
                )
        return files

    def delete_file(self, filename: str) -> bool:
        full_path = self._get_full_path(filename)
        if full_path.exists():
            full_path.unlink()
            return True
        return False

    def delete_files(self, glob_pattern: str) -> int:
        deleted_count = 0
        for path in self.root_path.glob(glob_pattern):
            if path.is_file():
                path.unlink()
                deleted_count += 1
        return deleted_count


class LocalFileSystem:
    """
    A class for managing files using the local filesystem
    (pathlib) relative to a root.
    """

    def __init__(self, root_path: str) -> None:
        """
        Initializes the LocalFileSystem.

        Args:
            root_path: The root path on the local filesystem to operate under.
        """
        self.root_path: pathlib.Path = pathlib.Path(root_path).resolve()
        self.root_path.mkdir(parents=True, exist_ok=True)

    def _get_full_path(self, filename: str) -> pathlib.Path:
        """
        Constructs the full path on the local filesystem.

        Args:
            filename: The relative filename.

        Returns:
            The full path as a pathlib.Path object.
        """
        return self.root_path / filename

    def save_file(
        self,
        filename: str,
        content: Union[str, bytes],
        mime_type: str = "application/octet-stream",
    ) -> Dict[str, Union[str, float]]:
        """
        Saves a file to the local filesystem.

        Args:
            filename: The name of the file to save (relative to root).
            content: The file content (string or bytes).
            type: The MIME type of the file.

        Returns:
            A dictionary containing file metadata
                (name, created, modified, type).
        """
        full_path = self._get_full_path(filename)
        full_path.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(content, str):
            full_path.write_text(content, encoding="utf-8")
        elif isinstance(content, bytes):
            full_path.write_bytes(content)
        else:
            raise TypeError(f"Unsupported content type: {type(content)}")
        stat_result = full_path.stat()
        return {
            "name": str(full_path.relative_to(self.root_path)),
            "created": stat_result.st_ctime,
            "modified": stat_result.st_mtime,
            "mime_type": mime_type,
        }

    def get_file(
        self, filename: str, mode: str = "text/plain"
    ) -> Optional[Dict[str, Union[str, bytes, float]]]:
        """
        Retrieves a file from the local filesystem.

        Args:
            filename: The name of the file to retrieve (relative to root).
            mode: The mode in which to return the content.
                  "string": Attempts to decode as UTF-8 and returns a string.
                  "bytes": Returns the raw bytes.

        Returns:
            A dictionary containing file data
                (name, content, created, modified, type),
                or None if the file does not exist.
        """
        full_path = self._get_full_path(filename)
        if not full_path.is_file():
            return None

        stat_result = full_path.stat()

        if mode == "application/octet-stream":
            content = full_path.read_bytes()
            return {
                "name": str(full_path.relative_to(self.root_path)),
                "content": content,
                "created": stat_result.st_ctime,
                "modified": stat_result.st_mtime,
                "mime_type": "application/octet-stream",
            }
        elif mode == "text/plain":
            try:
                content = full_path.read_text(encoding="utf-8")
                return {
                    "name": str(full_path.relative_to(self.root_path)),
                    "content": content,
                    "created": stat_result.st_ctime,
                    "modified": stat_result.st_mtime,
                    "mime_type": "text/plain",
                }
            except UnicodeDecodeError:
                content = full_path.read_bytes()
                return {
                    "name": str(full_path.relative_to(self.root_path)),
                    "content": content,
                    "created": stat_result.st_ctime,
                    "modified": stat_result.st_mtime,
                    "mime_type": "application/octet-stream",
                }
        else:
            raise ValueError(f"Invalid mode: '{mode}'. Must be 'string' or 'bytes'.")

    def get_string_io(self, filename: str) -> StringIO:
        """
        Retrieves a file as a StringIO object for text processing.

        Args:
            filename: The name of the file to retrieve (relative to root).

        Returns:
            A StringIO object containing the file's text content.

        Raises:
            FileNotFoundError: If the file does not exist.
            UnicodeDecodeError: If the file content cannot be decoded
                as UTF-8 text.
        """
        full_path = self._get_full_path(filename)
        if full_path.is_file():
            text = full_path.read_text(encoding="utf-8")
            return StringIO(text)
        else:
            raise FileNotFoundError(
                f"File '{filename}' not found under '{self.root_path}'"
            )  # noqa:E501

    def get_file_io(self, filename: str) -> FileIO:
        """
        Retrieves a file as a FileIO object for binary access.

        Args:
            filename: The name of the file to retrieve (relative to root).

        Returns:
            A FileIO object opened in binary read mode.

        Raises:
            FileNotFoundError: If the file does not exist.
        """
        full_path = self._get_full_path(filename)
        if full_path.is_file():
            # FileIO expects a string path, so we convert it.
            return FileIO(str(full_path), "rb")
        else:
            raise FileNotFoundError(
                f"File '{filename}' not found under '{self.root_path}'"
            )  # noqa:E501

    def list_files(self, glob_pattern: str = "*") -> List[Dict[str, Union[str, float]]]:
        """
        Lists files in the local filesystem matching the glob pattern
        (relative to root).

        Args:
            glob_pattern: The glob pattern to filter files.

        Returns:
            A list of dictionaries, each containing file metadata
                (name, created, modified, type).
                The 'name' is relative to the specified root path.
        """
        files: List[Dict[str, Union[str, float]]] = []
        for path in self.root_path.glob(glob_pattern):
            if path.is_file():
                stat_result = path.stat()
                try:
                    path.read_text(encoding="utf-8")
                    file_type = "text/plain"
                except UnicodeDecodeError:
                    file_type = "application/octet-stream"
                files.append(
                    {
                        "name": str(path.relative_to(self.root_path)),
                        "created": stat_result.st_ctime,
                        "modified": stat_result.st_mtime,
                        "mime_type": file_type,
                    }
                )
        return files

    def delete_file(self, filename: str) -> bool:
        """
        Deletes a file from the local filesystem.

        Args:
            filename: The name of the file to delete (relative to root).

        Returns:
            True if the file was deleted, False otherwise.
        """
        full_path = self._get_full_path(filename)
        if full_path.exists():
            full_path.unlink()
            return True
        return False

    def delete_files(self, glob_pattern: str) -> int:
        """
        Deletes files from the local filesystem matching the glob pattern
        (relative to root).

        Args:
            glob_pattern: The glob pattern to filter files for deletion.

        Returns:
            The number of files deleted.
        """
        deleted_count: int = 0
        for path in self.root_path.glob(glob_pattern):
            if path.is_file():
                path.unlink()
                deleted_count += 1
        return deleted_count


class FileSystem:
    """
    A wrapper class that uses BrowserFileSystem in Pyodide
    and LocalFileSystem locally,
    both relative to a specified root path.
    """

    def __init__(self, root_path: str = ".") -> None:
        """
        Initializes the FileSystem wrapper.

        Args:
            root_path: The root path to operate under.
        """
        self._root_path: pathlib.Path = pathlib.Path(root_path)
        if IS_PYODIDE:
            # Use Pyodide's in-memory or future IndexedDB-backed FS
            self._fs: BaseFileSystem = BrowserFileSystem(str(self._root_path))
        else:
            # Use local file system via pathlib
            self._fs: BaseFileSystem = LocalFileSystem(str(self._root_path))

    @property
    def root_path(self):
        # Expose underlying root path for compatibility
        return self._fs.root_path

    async def save_file(
        self,
        filename: str,
        content: Union[str, bytes],
        mime_type: str = "application/octet-stream",
    ) -> Dict[str, Union[str, float]]:
        """
        Saves a file.

        Args:
            filename: The name of the file to save (relative to root).
            content: The file content (string or bytes).
            mime_type: The MIME type of the file.

        Returns:
            A dictionary containing file metadata
                (name, created, modified, type).
        """
        return self._fs.save_file(filename, content, mime_type)

    async def get_file(
        self, filename: str, mode="text/plain"
    ) -> Optional[Dict[str, Union[str, bytes, float]]]:
        """
        Retrieves a file.

        Args:
            filename: The name of the file to retrieve (relative to root).
            mode: File mode: "text/plain" or "application/octet-stream"

        Returns:
            A dictionary containing file data
                (name, content, created, modified, type),
                or None if the file does not exist.
        """
        return self._fs.get_file(filename, mode=mode)

    async def list_files(
        self, glob_pattern: str = "*"
    ) -> List[Dict[str, Union[str, float]]]:
        """
        Lists files matching the glob pattern.

        Args:
            glob_pattern: The glob pattern to filter files (relative to root).

        Returns:
            A list of dictionaries, each containing file metadata
                (name, created, modified, type).
        """
        return self._fs.list_files(glob_pattern)

    async def get_string_io(self, filename: str):
        """
        Retrieves a file as a StringIO object for text processing.

        Args:
            filename: The name of the file to retrieve (relative to root).

        Returns:
            A StringIO object containing the file's text content.

        Raises:
            FileNotFoundError: If the file does not exist.
            UnicodeDecodeError: If the file content cannot be decoded
                as UTF-8 text.
        """
        return self._fs.get_string_io(filename)

    async def get_file_io(self, filename: str):
        """
        Retrieves a file as a FileIO object for binary access.

        Args:
            filename: The name of the file to retrieve (relative to root).

        Returns:
            A FileIO object opened in binary read mode.

        Raises:
            FileNotFoundError: If the file does not exist.
        """
        return self._fs.get_file_io(filename)

    async def delete_file(self, filename: str) -> bool:
        """
        Deletes a file.

        Args:
            filename: The name of the file to delete (relative to root).

        Returns:
            True if the file was deleted, False otherwise.
        """
        return self._fs.delete_file(filename)

    async def delete_files(self, glob_pattern: str) -> int:
        """
        Deletes files matching the glob pattern.

        Args:
            glob_pattern: The glob pattern to filter files for deletion
                (relative to root).

        Returns:
            The number of files deleted.
        """
        return self._fs.delete_files(glob_pattern)
