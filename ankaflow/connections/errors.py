class ConnectionException(Exception):
    """Base class for connection errors with optional message redaction."""


class DataModeConflict(ConnectionException): ...


class SchemaModeConflict(ConnectionException): ...


class UnrecoverableSinkError(ConnectionException): ...


class UnrecoverableTapError(ConnectionException): ...
