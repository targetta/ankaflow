import os
import json
from contextvars import ContextVar
from contextlib import contextmanager
from jinja2.sandbox import SandboxedEnvironment
import typing as t
import jmespath


# The 'Air-gap' switch: True only during a template render
is_in_sandbox = ContextVar("is_in_sandbox", default=False)

_real_getenv = os.getenv
_real_environ_obj = os.environ


def guarded_getenv(key, default=None):
    """Intercepts os.getenv calls."""
    if is_in_sandbox.get():
        # Masking: Secrets stay in the host, not the template
        return default
    return _real_getenv(key, default)


class GuardedEnviron(dict):
    """
    A proxy for os.environ that blocks access when the sandbox is active.
    We inherit from dict to ensure compatibility with libs expecting a mapping.
    """

    def __init__(self, original_environ):
        self._raw = original_environ

    def __getitem__(self, key):
        if is_in_sandbox.get():
            raise KeyError(f"Environment access restricted: '{key}'")
        return self._raw[key]

    def get(self, key, default=None):
        if is_in_sandbox.get():
            return default
        return self._raw.get(key, default)

    def __contains__(self, key):
        if is_in_sandbox.get():
            return False
        return key in self._raw

    def __repr__(self):
        if is_in_sandbox.get():
            return "{'REDACTED': 'In Sandbox Context'}"
        return repr(self._raw)


# --- THE INSTALLER ---

_is_patched = False


def install_environment_protection():
    """
    Globally patches os.getenv and os.environ.
    Safe to call multiple times, but only performs the patch once.
    """
    global _is_patched
    if _is_patched:
        return

    # 1. Patch the getenv function
    os.getenv = guarded_getenv

    # 2. Patch the environ object
    # Instead of class-patching __getitem__, we replace the instance
    # with our Guarded wrapper.
    os.environ = GuardedEnviron(_real_environ_obj)  # noqa: B003

    _is_patched = True


@contextmanager
def secure_context():
    """
    Context manager to activate the Logical Air-gap.
    Usage:
        with secure_context():
            renderer.render(template)
    """
    token = is_in_sandbox.set(True)
    try:
        yield
    finally:
        is_in_sandbox.reset(token)


class BaseSafeDict:
    def __init__(self, data=None, **kwargs):
        self._raw_data = {}
        self.update(data or {})
        self.update(kwargs)

    def __str__(self):
        return self._raw_data.__str__()

    def __repr__(self):
        return self._raw_data.__repr__()

    def __getitem__(self, key):
        val = self._raw_data[key]
        # CONVENTION: Single underscore = Logic-Only
        if str(key).startswith("__") and str(key).endswith("__"):
            raise KeyError(
                f"Access to internal attribute '{key}' is forbidden."
            )
        return val

    def __getattr__(self, key):
        try:
            return self.__getitem__(key)
        except KeyError:
            raise AttributeError(key)

    def __contains__(self, key):
        """Enables 'if key in Variables' logic in templates."""
        return key in self._raw_data

    def __setitem__(self, key, value):
        # 1. Dunder-Gate: No entry for internal attributes
        if str(key).startswith("__") and str(key).endswith("__"):
            # TODO: warnings.warn(f"Key '{key}' is a dunder and will be
            # inaccessible in templates.", UserWarning)
            pass

        # 2. Strict Recursive Wrapping: Only for dicts/lists
        if isinstance(value, dict) and not isinstance(value, BaseSafeDict):
            value = self.__class__(value)
        elif isinstance(value, list):
            value = [
                (
                    self.__class__(v)
                    if isinstance(v, dict) and not isinstance(v, BaseSafeDict)
                    else v
                )
                for v in value
            ]

        self._raw_data[key] = value

    def update(self, *args, **kwargs):
        data = dict(*args, **kwargs)
        for k, v in data.items():
            self[k] = v

    def get(self, key, default=None):
        """Dict-like get that respects the Shadow Metadata masking."""
        try:
            # We call __getitem__ to ensure logic-only masking applies
            if key in self._raw_data:
                return self[key]
            return default
        except KeyError:
            return default

    def to_dict(self):
        """Unwraps only our own containers for JSON export."""
        out = {}
        for k, v in self._raw_data.items():
            if str(k).startswith("__") and str(k).endswith("__"):
                continue

            if isinstance(v, BaseSafeDict):
                out[k] = v.to_dict()
            elif isinstance(v, list):
                out[k] = [
                    i.to_dict() if isinstance(i, BaseSafeDict) else i for i in v
                ]
            else:
                out[k] = v
        return out
    
    # Make more compatible - full support for jmepath protocol
    def keys(self):
        return self._raw_data.keys()

    def values(self):
        # Note: These values might be BaseSafeDicts, which is fine!
        return self._raw_data.values()

    def items(self):
        return self._raw_data.items()
        
    def __iter__(self):
        return iter(self._raw_data)

    def __len__(self):
        return len(self._raw_data)
    
    def look(self, expression: str):
        """Allow JMESPath searching across the container."""
        result = jmespath.search(expression, data=self)
        
        # If the result is a dict/list, wrap it back so it stays "Safe"
        if isinstance(result, (dict, list)):
            return self.__class__({"_tmp": result})["_tmp"]
        return result


class StrictEnvironment(SandboxedEnvironment):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.globals.clear()  # Purge range, dict, help, etc.
        self.filters["tojson"] = lambda o: json.dumps(
            o, default=self._json_encoder
        )

    def _json_encoder(self, obj):
        if isinstance(obj, BaseSafeDict):
            return obj.to_dict()
        if isinstance(obj, str) and obj.startswith("__"):
            return ""
        return str(obj)

    def is_safe_attribute(self, obj, attr, value):
        if attr.startswith("__"):
            return False
        if isinstance(obj, BaseSafeDict) and attr.startswith("_"):
            return True
        return super().is_safe_attribute(obj, attr, value)


def jinja_sanitize(
    obj: t.Any,
):
    """Recursively wraps data in BaseSafeDict proxies."""
    if hasattr(obj, "model_dump") and callable(obj.model_dump):
        obj = obj.model_dump()
    if hasattr(obj, "to_dict") and callable(obj.to_dict):
        obj = (
            obj.to_dict(orient="records")
            if hasattr(obj, "columns")
            else obj.to_dict()
        )

    if isinstance(obj, dict):
        return BaseSafeDict({k: jinja_sanitize(v) for k, v in obj.items()})
    if isinstance(obj, (list, tuple)):
        return [jinja_sanitize(i) for i in obj]
    return obj
