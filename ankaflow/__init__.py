import os

from .common.security import install_environment_protection, secure_context
from .core.flow import Flow, AsyncFlow, FlowControl
from .internal.macros import register_macro

from .models import (
    Stages,
    ConnectionConfiguration,
    Variables,
    FlowContext,
    S3Config,
    GSConfig,
    ClickhouseConfig,
    BigQueryConfig,
    BucketConfig,
)

from .errors import FlowRunError, FlowError

# Auto-init the environment protection
if os.getenv("ANKAFLOW_SECURITY", "").lower() in ["1","true","y","yes"]:
    install_environment_protection()

__all__ = [
    "Flow",
    "AsyncFlow",
    "FlowControl",
    "Stages",
    "ConnectionConfiguration",
    "Variables",
    "FlowContext",
    "FlowRunError",
    "FlowError",
    "S3Config",
    "GSConfig",
    "ClickhouseConfig",
    "BigQueryConfig",
    "BucketConfig",
    "register_macro",
    "secure_context"
]
