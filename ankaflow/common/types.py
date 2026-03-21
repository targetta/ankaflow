import typing as t

from .security import BaseSafeDict


class StringDict(dict[str, str]):
    """A dictionary that coerces all values to strings.

    Useful for HTTP headers, query params, or any place where string-only
    key/value pairs are required.

    Coercion is enforced during initialization, assignment, and update.
    """

    def __init__(
        self,
        data: t.Optional[t.Dict[str, t.Any]] = None,
        *,
        allow_none: bool = False,
    ):
        """Initialize the dictionary with optional string coercion.

        Args:
            data (Dict[str, Any] | None): Input dictionary to wrap.
            allow_none (bool): If False, keys with `None` values will raise an error.
        """
        self.allow_none = allow_none
        super().__init__()
        if data:
            self.update(data)

    def __setitem__(self, key: str, value: t.Any) -> None:
        if value is None and not self.allow_none:
            raise ValueError(f"None value not allowed for key: '{key}'")
        super().__setitem__(key, str(value) if value is not None else "None")

    # def update(self, __m: Optional[SupportsKeysAndGetItem[str, str]] = ...
    def update(self, *args: t.Any, **kwargs: t.Any) -> None:  # type: ignore[override]
        if args:
            [arg] = args
            if isinstance(arg, t.Mapping):
                for k, v in arg.items():
                    self[k] = v
            else:
                for k, v in arg:
                    self[k] = v
        for k, v in kwargs.items():
            self[k] = v

    @classmethod
    def from_model_dump(
        cls, data: t.Dict[str, t.Any], allow_none: bool = False
    ) -> "StringDict":
        """Create a StringDict from a model dump or similar structure."""
        return cls(data, allow_none=allow_none)

    def as_dict(self) -> t.Dict[str, str]:
        """Returns a plain dictionary with stringified values."""
        return dict(self)


class ImmutableMap:
    """
    A dictionary-like immutable class.

    Instances can only be populated during creation using keyword arguments
    or an existing dictionary. Supports both bracket and dot notation for
    accessing values.
    """

    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
        self._data: t.Mapping[str, t.Any]

        if args and not kwargs:
            if len(args) == 1 and isinstance(args[0], t.Mapping):
                self._data = dict(args[0])
            else:
                raise TypeError(
                    "ImmutableMap can only be initialized with a single mapping argument or keyword arguments."  # noqa: E501
                )
        elif not args and kwargs:
            self._data = dict(kwargs)
        elif not args and not kwargs:
            self._data = {}
        else:
            raise TypeError(
                "ImmutableMap can only be initialized with a single mapping argument or keyword arguments, not both positional and keyword arguments."  # noqa: E501
            )

    def __getitem__(self, key: str) -> t.Any:
        return self._data[key]

    def __getattr__(self, key: str) -> t.Any:
        if key in self._data:
            return self._data[key]
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{key}'")

    def __setitem__(self, key: str, value: t.Any) -> None:
        raise TypeError("'ImmutableMap' object does not support item assignment")

    def __setattr__(self, key: str, value: t.Any) -> None:
        if not key.startswith("_"):
            raise TypeError(
                "'ImmutableMap' object does not support attribute assignment"
            )
        super().__setattr__(key, value)

    def __delitem__(self, key: str) -> None:
        raise TypeError("'ImmutableMap' object does not support item deletion")

    def __delattr__(self, key: str) -> None:
        if not key.startswith("_"):
            raise TypeError("'ImmutableMap' object does not support attribute deletion")
        super().__delattr__(key)

    def __len__(self) -> int:
        return len(self._data)

    def __iter__(self) -> t.Iterator[str]:
        return iter(self._data)

    def __contains__(self, key: object) -> bool:
        return key in self._data

    def __repr__(self) -> str:
        return f"{type(self).__name__}({repr(self._data)})"

    def __eq__(self, other: t.Any) -> bool:
        if isinstance(other, ImmutableMap):
            return self._data == other._data
        return self._data == other

    def __hash__(self) -> int:
        return hash(tuple(sorted(self._data.items())))

    def keys(self) -> t.KeysView[str]:
        return self._data.keys()

    def values(self) -> t.ValuesView[t.Any]:
        return self._data.values()

    def items(self) -> t.ItemsView[str, t.Any]:
        return self._data.items()

    def get(self, key: str, default: t.Any = None) -> t.Any:
        return self._data.get(key, default)

class Variables(BaseSafeDict):
    """
    Variables is a `dict`-based collection
    storing arbitrary data under keys. Variable can be populated via:

    - Dictionary passed to pipeline:
      `Flow(defs, context, variables={'foo': 'bar'})`
    - Sink operation:

            - kind: sink\n
                name: my_sink\n
                connection:\n
                    kind: Variable\n
                    locator: variable_name

      This step will place a list of records from preceding step in
      variables as `{'variable_name': t.List[dict]}`

    In most cases sainking to variable is not needed as all preceding stages
    in a pipeline can be referenced via `name`. Sinking to variable is
    useful when you need to share data with subpipeline.

    Special variable is `loop_control` which is populated
    dynamically from previous step and current stage type is pipeline.
    In case previous step generates multiple records then the new pipeline
    is called for each record, and control variable holds a `dict`
    representing the current record.
    """


class FlowContext(BaseSafeDict):
    """
    Context dictionary can be used to supply arbitrary data
    to pipeline that can be referenced in templates much
    much like variables with the difference that they cannot
    be modified at runtime.

    Context can be initiated via dictionary:

    `context = FlowContext(**{'foo':'Bar'}])`

    Items can be reference using both bracket and dot notation:

    `context.foo == context['foo'] == 'Bar'`

    """

    def __init__(self, data=None, **kwargs):
        self._is_fozen=False
        super().__init__(data, **kwargs)
        self._is_fozen=True

    def __setitem__(self, key, value):
        if self._is_fozen:
            raise TypeError(f"'{self.__class__.__name__}' is immutable. Changes are not allowed after initialization.")  # noqa: E501
        super().__setitem__(key, value)
            

    def update(self, *args, **kwargs):
        # Prevent the base class update from working
        if self._is_fozen:
            raise TypeError(f"'{self.__class__.__name__}' is immutable.")
        super().update(*args, **kwargs)
