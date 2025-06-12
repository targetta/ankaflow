import pathlib
import urllib.parse
import os
import typing as t


class CommonPath:
    """Base class for all path types, providing common functionality."""

    def __init__(self, path: t.Union[str, "CommonPath", pathlib.Path]):
        self.path = str(path)

    @property
    def is_glob(self) -> bool:
        """Returns True if the path represents a glob pattern."""
        return any(char in self.path for char in "*?[]")

    def is_absolute(self) -> bool:
        """Returns True if the path is considered absolute (remote or rooted)."""  # noqa: E501
        return self.path.startswith("/") or "://" in self.path

    def __str__(self) -> str:
        """Returns the string representation of the path."""
        return self.path

    # --- Shared interface stubs (to satisfy IDEs and type checkers) ---

    def get_endpoint(self, region: t.Optional[str] = None) -> str:
        """
        Returns the network endpoint for a remote object.

        RemotePath subclasses override this. LocalPath may skip or raise.
        """
        raise NotImplementedError("get_endpoint() must be implemented by remote paths")  # noqa: E501

    def get_local(self, root: str = "/tmp") -> str:
        """
        Returns a local filesystem path that mirrors this remote path.
        """
        raise NotImplementedError("get_local() must be implemented by remote paths")  # noqa: E501

    @property
    def scheme(self) -> str:
        """
        Returns the scheme (e.g. s3, gs, ftp, http, file).
        """
        raise NotImplementedError("scheme must be implemented by subclasses")

    def as_uri(self) -> str:
        """
        Returns the path as a URI.
        """
        return self.path

    def as_url(self) -> str:
        """
        Returns the path as a URL string (same as URI by default).
        """
        return self.path

    @property
    def name(self) -> str:
        """Returns the name component of the path."""
        return os.path.basename(self.path)

    @property
    def suffix(self) -> str:
        """Returns the file extension (e.g. .csv)."""
        return os.path.splitext(self.name)[1]

    @property
    def suffixes(self) -> list[str]:
        """Returns all extensions (e.g. .tar.gz → ['.tar', '.gz'])."""
        name = self.name
        suffixes = []
        while "." in name:
            name, suffix = os.path.splitext(name)
            suffixes.insert(0, suffix)
        return suffixes

    @property
    def stem(self) -> str:
        """Returns the filename without extension."""
        return os.path.splitext(self.name)[0]

    @property
    def anchor(self) -> str:
        """Returns the root/authority part of the path."""
        raise NotImplementedError("anchor must be implemented by subclasses")

    @property
    def parent(self) -> t.Self:
        """Returns the parent path."""
        raise NotImplementedError("parent must be implemented by subclasses")

    @property
    def parts(self) -> tuple[str, ...]:
        """Returns the path split into parts."""
        raise NotImplementedError("parts must be implemented by subclasses")

    def joinpath(self, *others: str) -> "CommonPath":
        raise NotImplementedError("joinpath() must be implemented in subclasses")  # noqa: E501


class LocalPath(pathlib.Path, CommonPath):
    def __init__(self, *args, **kwargs):
        """Initializes a LocalPath object."""
        pathlib.Path.__init__(self, *args, **kwargs)
        CommonPath.__init__(self, str(self))  # Initialize CommonPath part

    def __new__(cls, *args, **kwargs):
        """Creates a new LocalPath object."""
        return super().__new__(cls, *args, **kwargs)

    def __str__(self) -> str:
        """Returns the string representation of the path."""
        return str(pathlib.Path(self))

    def joinpath(self, *other: str | os.PathLike[str]) -> t.Self:
        return super().joinpath(*other)


class RemotePath(CommonPath):
    """Base class for remote path types."""

    def __init__(self, path: str):
        """Initializes a RemotePath object."""
        super().__init__(path)
        parsed = urllib.parse.urlparse(path)
        self.bucket = parsed.netloc
        self.key = parsed.path[1:].lstrip("/")  # Remove leading /

    def __truediv__(self, other: t.Union[str, "RemotePath"]) -> "RemotePath":
        """Joins the RemotePath with another path component."""
        if isinstance(other, CommonPath) and other.is_absolute():
            raise ValueError("Cannot join an absolute path to another path.")
        return type(self)(
            f"{self.scheme}://{self.bucket}/{os.path.join(self.key, str(other))}"  # noqa:E501
        )

    @property
    def name(self) -> str:
        """Returns the base filename of the remote key."""
        return os.path.basename(self.key)

    @property
    def parent(self) -> "RemotePath":
        """Returns the parent directory of the remote key."""
        parent_key = os.path.dirname(self.key)
        if parent_key == "":
            return type(self)(f"{self.scheme}://{self.bucket}/")
        return type(self)(f"{self.scheme}://{self.bucket}/{parent_key}")

    @property
    def parts(self) -> tuple[str, ...]:
        """Returns a tuple of the remote path components."""
        return (f"{self.scheme}:/", self.bucket, *self.key.split("/"))

    # Method "joinpath" overrides class "CommonPath" in an incompatible manner
    #  Keyword parameter "other" type mismatch: base parameter is
    # type "str | CommonPath", override parameter is type "str | RemotePath"
    def joinpath(self, *other: t.Union[str, "RemotePath"]) -> "RemotePath":
        """Joins the RemotePath with multiple path components."""
        new_path = self
        for part in other:
            new_path = new_path / part
        return new_path

    @property
    def anchor(self) -> str:
        """Returns the anchor part of the remote path."""
        return f"{self.scheme}://{self.bucket}"

    @property
    def fspath(self) -> str:
        """Returns the file system path representation of the remote path."""
        return self.path

    @property
    def stem(self) -> str:
        """Returns the stem (filename without suffix) of the remote key."""
        return os.path.splitext(self.name)[0]

    @property
    def suffix(self) -> str:
        """Returns the file suffix of the remote key."""
        return os.path.splitext(self.name)[1]

    @property
    def suffixes(self) -> t.List[str]:
        """Returns a list of file suffixes of the remote key."""
        name = self.name
        suffixes = []
        while "." in name:
            name, suffix = os.path.splitext(name)
            suffixes.insert(0, suffix)
        return suffixes

    def as_uri(self) -> str:
        """Returns the remote path as a URI."""
        return self.path

    def as_url(self) -> str:
        """Returns the remote path as a URL."""
        return self.path

    def get_endpoint(self, region: t.Optional[str] = None) -> str:
        return self.path

    @property
    def scheme(self) -> str:
        """Returns the scheme of the remote path."""
        raise NotImplementedError("Scheme must be implemented by subclasses.")

    def get_local(self, root: str = "/tmp") -> str:
        """
        Returns a corresponding local path for this
        remote object under the given root.

        Args:
            root (str): The base directory (e.g., /tmp).

        Returns:
            str: A full local path like /tmp/bucket/key
        """
        return os.path.join(root, self.bucket, self.key)


class S3Path(RemotePath):
    """Represents an S3 path."""

    @property
    def scheme(self) -> str:
        """Returns the scheme of the S3 path."""
        return "s3"

    def get_endpoint(self, region: t.Optional[str] = None) -> str:
        """
        Returns the HTTP endpoint URL for the S3 object.

        Args:
            region (str, optional): The AWS region.
                If None, returns the global endpoint.

        Returns:
            The HTTP endpoint URL.
        """
        if region:
            return f"https://{self.bucket}.s3.{region}.amazonaws.com/{self.key}"
        else:
            return f"https://s3.amazonaws.com/{self.bucket}/{self.key}"


class GSPath(RemotePath):
    """Represents a Google Cloud Storage (GS) path."""

    @property
    def scheme(self) -> str:
        """Returns the scheme of the GS path."""
        return "gs"

    def get_endpoint(self, region: t.Optional[str] = None) -> str:
        """
        Returns the HTTP endpoint URL for the GS object.

        Args:
            region (str, optional): The Google Cloud region.
                If None, uses the global endpoint.

        Returns:
            The HTTP endpoint URL.
        """
        if region:
            return (
                f"https://storage{region}.rep.googleapis.com/{self.bucket}/{self.key}"
            )
        else:
            return f"https://storage.googleapis.com/{self.bucket}/{self.key}"


class HTTPPath(RemotePath):
    """Represents an HTTP/HTTPS path."""

    def __init__(self, path: str):
        """Initializes an HTTPPath object."""
        super().__init__(path)
        parsed = urllib.parse.urlparse(path)
        self.netloc = parsed.netloc
        self.path_part = parsed.path
        self._scheme = parsed.scheme

    @property
    def scheme(self) -> str:
        """Returns the scheme of the HTTP/HTTPS path."""
        return self._scheme

    def get_endpoint(self, region: str | None = None) -> str:
        """
        Returns the HTTP endpoint URL for the HTTP/HTTPS object.

        Returns:
            The HTTP endpoint URL.
        """
        return self.path


class FTPPath(RemotePath):
    """Represents an FTP path."""

    @property
    def scheme(self) -> str:
        """Returns the scheme of the FTP path."""
        return "ftp"

    def get_endpoint(self, region: t.Optional[str] = None) -> str:
        """
        Returns the FTP endpoint URL for the FTP object.

        Returns:
            The FTP endpoint URL.
        """
        return self.path


class PathFactory:
    """Factory class to create CommonPath objects."""

    @staticmethod
    def make(
        path: t.Union[str, "CommonPath", pathlib.Path, None],
    ) -> "CommonPath":
        """
        Creates a CommonPath object based on the input.

        Args:
            path (Union[str, "CommonPath", pathlib.Path]):
                The path string or path-like object.

        Returns:
            A CommonPath object or a subclass.
        """
        if isinstance(path, CommonPath):
            return path
        if isinstance(path, pathlib.Path):
            return LocalPath(path)
        if isinstance(path, str):
            if PathFactory._is_absolute_s3(path):
                return S3Path(path)
            elif PathFactory._is_absolute_gs(path):
                return GSPath(path)
            elif PathFactory._is_absolute_http(path):
                return HTTPPath(path)
            elif PathFactory._is_absolute_ftp(path):
                return FTPPath(path)
            elif PathFactory._is_absolute_file(path):
                return LocalPath(urllib.parse.urlparse(path).path)
            else:
                return LocalPath(path)
        else:
            raise TypeError(f"Unsupported path type: {type(path)}")

    @staticmethod
    def _is_absolute_s3(path_str: str) -> bool:
        """Checks if a string represents an absolute S3 path."""
        return path_str.startswith("s3://")

    @staticmethod
    def _is_absolute_gs(path_str: str) -> bool:
        """Checks if a string represents an absolute GS path."""
        return path_str.startswith("gs://")

    @staticmethod
    def _is_absolute_http(path_str: str) -> bool:
        """Checks if a string represents an absolute HTTP/HTTPS path."""
        return path_str.startswith("http://") or path_str.startswith("https://")

    @staticmethod
    def _is_absolute_ftp(path_str: str) -> bool:
        """Checks if a string represents an absolute FTP path."""
        return path_str.startswith("ftp://")

    @staticmethod
    def _is_absolute_file(path_str: str) -> bool:
        """Checks if a string represents an absolute file:// path."""
        return path_str.startswith("file://")
