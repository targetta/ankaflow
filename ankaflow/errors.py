class ConfigurationError(Exception):
    pass


class TapSourceMissingError(Exception):
    pass


class FlowError(Exception):
    pass


class FlowRunError(FlowError):
    pass


class SchemaDiscoveryError(FlowError):
    pass
