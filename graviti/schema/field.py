#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#
"""Portex record field releated classes."""


from typing import Any, Dict, Iterable, List, Mapping, NamedTuple, Tuple, Union

import pyarrow as pa

from graviti.schema.base import _INDENT, PortexType
from graviti.schema.package import Imports
from graviti.utility import NameOrderedDict


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


class Fields(NameOrderedDict[PortexType]):
    """Represents a Portex ``record`` fields dict."""

    def __init__(self, fields: Union[Iterable[Tuple[str, PortexType]], Mapping[str, PortexType]]):
        super().__init__(fields)

        imports = Imports()
        for portex_type in self._data.values():
            imports.update_from_type(portex_type)

        self.imports = imports

    def __repr__(self) -> str:
        return self._repr1(0)

    def _repr1(self, level: int) -> str:
        indent = level * _INDENT
        lines = ["{"]
        for name, portex_type in self.items():
            lines.append(
                f"{_INDENT}'{name}': "  # pylint: disable=protected-access
                f"{portex_type._repr1(level + 1)},"
            )

        lines.append("}")
        return f"\n{indent}".join(lines)

    def insert(self, index: int, name: str, portex_type: PortexType) -> None:
        """Insert the name and portex_type at the index.

        Arguments:
            index: The index to insert the field.
            name: The name of the field to be inserted.
            portex_type: The portex_type of the field to be inserted.

        Raises:
            KeyError: When the name already exists in the Fields.

        """
        if self.__contains__(name):
            raise KeyError(f'"{name}" already exists in the Fields')

        self._keys.insert(index, name)
        self._data.__setitem__(name, portex_type)

    def astype(self, name: str, portex_type: PortexType) -> None:
        """Convert the type of the field with the given name to the new PortexType.

        Arguments:
            name: The name of the field to convert.
            portex_type: The new PortexType of the field to convert to.

        Raises:
            KeyError: When the name does not exist in the Fields.

        """
        if not self.__contains__(name):
            raise KeyError(f'"{name}" does not exist in the Fields')

        self._data.__setitem__(name, portex_type)

    @classmethod
    def from_pyobj(cls, content: List[Dict[str, Any]], imports: Imports) -> "Fields":
        """Create Portex fields dict instance from python list.

        Arguments:
            content: A python list representing a Portex fields dict.
            imports: The imports of the Portex fields dict.

        Returns:
            A Portex fields dict instance created from the input python list.

        """
        return Fields((item["name"], PortexType.from_pyobj(item, imports)) for item in content)

    def to_pyobj(self) -> List[Dict[str, Any]]:
        """Dump the instance to a python list.

        Returns:
            A Python List representation of the fields dict.

        """
        return [{"name": name, **portex_type.to_pyobj(False)} for name, portex_type in self.items()]

    def to_pyarrow(self) -> List[pa.Field]:
        """Convert the fields to a list of PyArrow fields.

        Returns:
            A list of PyArrow fields representing the fields of Portex record.

        """
        return [pa.field(name, portex_type.to_pyarrow()) for name, portex_type in self.items()]
