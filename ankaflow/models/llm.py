from enum import Enum
from pydantic import BaseModel

from .rest import RestClientConfig, Request

class LLMKind(str, Enum):
    """Supported LLM protocol backends."""
    OPENAI = "openai"
    """"""
    MOCK = "mock"
    """"""


class LLMProxy(BaseModel):
    """Configuration for proxy-based LLM usage."""
    client: RestClientConfig  # Expected to be RestClientConfig
    """"""
    request: Request  # Expected to be Request
    """"""


class LLMConfig(BaseModel):
    """Confuration for Language Model"""
    kind: LLMKind = LLMKind.MOCK
    """Language model provider: `mock`(default)|`openai`"""
    model: str | None = None
    """Language model, if not set then uses default"""
    temperature: float | None = None
    proxy: LLMProxy | None = None
    """Reverse proxy used to connect to provider (optional)"""