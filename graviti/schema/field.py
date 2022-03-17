#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#
"""Portex record field releated classes."""


from typing import Any, Dict, Iterable, List, Mapping, NamedTuple, Sequence, Tuple, Union, overload

from graviti.schema.base import _INDENT, PortexType


class Field(NamedTuple):
    """Represents a Portex ``record`` field which contains the field name and type."""

    name: str
    type: PortexType


class Fields(Sequence[Field]):
    """Represents a Portex ``record`` field list."""

    def __init__(
        self,
        fields: Union[Iterable[Tuple[str, PortexType]], Mapping[str, PortexType]],
    ):
        iterable = fields.items() if isinstance(fields, Mapping) else fields
        self._data = [Field(*field) for field in iterable]

    @overload
    def __getitem__(self, index: int) -> Field:
        ...

    @overload
    def __getitem__(self, index: slice) -> List[Field]:
        ...

    def __getitem__(self, index: Union[int, slice]) -> Union[Field, List[Field]]:
        return self._data.__getitem__(index)

    def __len__(self) -> int:
        return self._data.__len__()

    def __repr__(self) -> str:
        return self._repr1(0)

    def _repr1(self, level: int) -> str:
        indent = level * _INDENT
        lines = ["{"]
        for field in self._data:
            lines.append(
                f"{_INDENT}'{field.name}': "  # pylint: disable=protected-access
                f"{field.type._repr1(level + 1)},"
            )

        lines.append("}")
        return f"\n{indent}".join(lines)

    @classmethod
    def from_pyobj(cls, content: List[Dict[str, Any]]) -> "Fields":
        """Create Portex field list instance from python list.

        Arguments:
            content: A python list representing a Portex field list.

        Returns:
            A Portex field list instance created from the input python list.

        """
        return Fields((item["name"], PortexType.from_pyobj(item)) for item in content)

    def to_pyobj(self) -> List[Dict[str, Any]]:
        """Dump the instance to a python list.

        Returns:
            A Python List representation of the field list.

        """
        return [{"name": field.name, **field.type.to_pyobj()} for field in self._data]
