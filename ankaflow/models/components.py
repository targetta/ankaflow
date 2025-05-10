import typing as t
from pydantic import BaseModel, RootModel


class Column(BaseModel):
    """Data column (equivalent to database column)"""

    name: str
    """
    Column name must follow rules set to SQL engine column names
    """
    type: str
    """
    Any data type support by SQL engine
    """


class Columns(RootModel[t.List[Column]]):
    """
    Iterable list-like collection of Fields.
    """

    root: t.List[Column]
    _error: t.Optional[str] = None  # not part of validation

    def values(self):
        return self.root

    def __getitem__(self, item):
        return self.root[item]

    def __iter__(self):  # type: ignore[override]
        return iter(self.root)

    @classmethod
    def error(cls, message: str) -> "Columns":
        fields = cls([])
        fields._error = message
        return fields

    def is_error(self) -> bool:
        return self._error is not None

    def print(self) -> str:
        if self._error:
            return f"⚠️ Schema Error: {self._error}"
        out = []
        for item in self.root:
            out.append(f"- name: {item.name}\n  type: {item.type}")
        return "\n".join(out)

