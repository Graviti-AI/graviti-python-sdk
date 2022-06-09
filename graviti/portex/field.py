#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#
"""Portex record field releated classes."""


from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple, TypeVar, Union

import pyarrow as pa

from graviti.portex.base import _INDENT, PortexType
from graviti.portex.package import Imports
from graviti.utility import FrozenNameOrderedDict, NameOrderedDict

_T = TypeVar("_T", bound="Fields")


class ImmutableFields(FrozenNameOrderedDict[PortexType]):
    """Represents an immutable fields dict."""

    @classmethod
    def _check_value(cls, value: PortexType) -> None:
        if not isinstance(value, PortexType):
            raise TypeError(f'The value in "{cls.__name__}" should be a PortexType')

    def _setitem(self, key: str, value: PortexType) -> None:
        self._check_value(value)
        super()._setitem(key, value)


class MutableFields(NameOrderedDict[PortexType], ImmutableFields):
    """Represents a mutable fields dict."""

    def insert(self, index: int, name: str, portex_type: PortexType) -> None:
        """Insert the name and portex_type at the index.

        Arguments:
            index: The index to insert the field.
            name: The name of the field to be inserted.
            portex_type: The portex_type of the field to be inserted.

        Raises:
            KeyError: When the name already exists in the Fields.

        """
        self._check_key(name)
        self._check_value(portex_type)

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
        self._check_value(portex_type)

        if not self.__contains__(name):
            raise KeyError(f'"{name}" does not exist in the Fields')

        self._data.__setitem__(name, portex_type)

    def rename(self, old_name: str, new_name: str) -> None:
        """Rename the name of a field.

        Arguments:
            old_name: The current name of the field to be renamed.
            new_name: The new name of the field to assign.

        """
        self._check_key(new_name)

        self._data.__setitem__(new_name, self._data.pop(old_name))
        self._keys.__setitem__(self._keys.index(old_name), new_name)


class Fields(NameOrderedDict[PortexType]):
    """Represents a Portex ``record`` fields dict."""

    def __init__(self, fields: Union[Iterable[Tuple[str, PortexType]], Mapping[str, PortexType]]):
        super().__init__(fields)

    def __repr__(self) -> str:
        return self._repr1(0)

    @classmethod
    def _check_value(cls, value: PortexType) -> None:
        if not isinstance(value, PortexType):
            raise TypeError(f'The value in "{cls.__name__}" should be a PortexType')

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

    def _setitem(self, key: str, value: PortexType) -> None:
        self._check_value(value)
        super()._setitem(key, value)

    @property
    def imports(self) -> Imports:
        """Get the Fields imports.

        Returns:
            The :class:`Imports` instance of this Fields.

        """
        imports = Imports()
        for portex_type in self._data.values():
            imports.update(portex_type.imports)

        return imports

    def insert(self, index: int, name: str, portex_type: PortexType) -> None:
        """Insert the name and portex_type at the index.

        Arguments:
            index: The index to insert the field.
            name: The name of the field to be inserted.
            portex_type: The portex_type of the field to be inserted.

        Raises:
            KeyError: When the name already exists in the Fields.

        """
        self._check_key(name)
        self._check_value(portex_type)

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
        self._check_value(portex_type)

        if not self.__contains__(name):
            raise KeyError(f'"{name}" does not exist in the Fields')

        self._data.__setitem__(name, portex_type)

    def rename(self, old_name: str, new_name: str) -> None:
        """Rename the name of a field.

        Arguments:
            old_name: The current name of the field to be renamed.
            new_name: The new name of the field to assign.

        """
        self._check_key(new_name)

        self._data.__setitem__(new_name, self._data.pop(old_name))
        self._keys.__setitem__(self._keys.index(old_name), new_name)

    @classmethod
    def from_pyobj(
        cls, content: List[Dict[str, Any]], imports: Optional[Imports] = None
    ) -> "Fields":
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
        """Convert the fields to a list of PyArrow Field.

        Returns:
            A list of PyArrow Field representing the fields of Portex record.

        """
        return [pa.field(name, portex_type.to_pyarrow()) for name, portex_type in self.items()]

    def copy(self: _T) -> _T:
        """Get a copy of the fields.

        Returns:
            A copy of the fields.

        """
        return self.__class__((name, portex_type.copy()) for name, portex_type in self.items())
