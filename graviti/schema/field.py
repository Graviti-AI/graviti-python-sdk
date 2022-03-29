#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#
"""Portex record field releated classes."""


from typing import Any, Dict, Iterable, List, Mapping, NamedTuple, Tuple, Union

import pyarrow as pa
from tensorbay.utility import UserSequence

from graviti.schema.base import _INDENT, PortexType
from graviti.schema.package import Imports


class Field(NamedTuple):
    """Represents a Portex ``record`` field which contains the field name and type."""

    name: str
    type: PortexType

    @property
    def imports(self) -> Imports:
        """Property imports to indicate the imports of this field.

        Returns:
            The imports of this field.

        """
        imports = Imports()
        imports.update_from_type(self.type)
        return imports

    @classmethod
    def from_pyobj(cls, content: Dict[str, Any], imports: Imports) -> "Field":
        """Create Portex field instance from python dict.

        Arguments:
            content: A python dict representing a Portex field.
            imports: The imports of the Portex field.

        Returns:
            A Portex field instance created from the input python dict.

        """
        return cls(content["name"], PortexType.from_pyobj(content, imports))

    def to_pyobj(self) -> Dict[str, Any]:
        """Dump the instance to a python dict.

        Returns:
            A Python dict representation of the field.

        """
        return {"name": self.name, **self.type.to_pyobj(False)}


class Fields(UserSequence[Field]):
    """Represents a Portex ``record`` field list."""

    def __init__(
        self,
        fields: Union[Iterable[Tuple[str, PortexType]], Mapping[str, PortexType]],
    ):
        iterable = fields.items() if isinstance(fields, Mapping) else fields
        self._data = [Field(*field) for field in iterable]

        imports = Imports()
        for field in self._data:
            imports.update_from_type(field.type)

        self.imports = imports

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
    def from_pyobj(cls, content: List[Dict[str, Any]], imports: Imports) -> "Fields":
        """Create Portex field list instance from python list.

        Arguments:
            content: A python list representing a Portex field list.
            imports: The imports of the Portex field list.

        Returns:
            A Portex field list instance created from the input python list.

        """
        return Fields(Field.from_pyobj(item, imports) for item in content)

    def to_pyobj(self) -> List[Dict[str, Any]]:
        """Dump the instance to a python list.

        Returns:
            A Python List representation of the field list.

        """
        return [field.to_pyobj() for field in self._data]

    def to_pyarrow(self) -> List[pa.Field]:
        """Convert the fields to a list of PyArrow fields.

        Returns:
            A list of PyArrow fields representing the fields of Portex record.

        """
        return [pa.field(field.name, field.type.to_pyarrow()) for field in self]
