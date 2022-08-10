#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#
"""Portex record field releated classes."""


from collections import ChainMap
from itertools import chain
from typing import Any
from typing import ChainMap as ChainMapType
from typing import (
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
)

import pyarrow as pa

from graviti.portex.base import _INDENT, PortexType
from graviti.portex.package import Imports
from graviti.utility import FrozenNameOrderedDict, NameOrderedDict

_T = TypeVar("_T", bound="Fields")


class FrozenFields(FrozenNameOrderedDict[PortexType]):
    """Represents a frozen fields dict."""

    def __repr__(self) -> str:
        return self._repr1(0)

    def __setitem__(self, key: Union[int, str], value: PortexType) -> None:
        raise TypeError(f"Cannot set item '{key}' in {self.__class__.__name__}")

    def __delitem__(self, key: Union[int, str]) -> None:
        raise TypeError(f"Cannot delete item '{key}' in {self.__class__.__name__}")

    @classmethod
    def _check_value(cls, value: PortexType) -> None:
        if not isinstance(value, PortexType):
            raise TypeError(f'The value in "{cls.__name__}" should be a PortexType')

    def _setitem(self, key: str, value: PortexType) -> None:
        self._check_value(value)
        super()._setitem(key, value)

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
            TypeError: When calling this method of FrozenFields.

        """
        raise TypeError(f"Cannot insert in {self.__class__.__name__}")

    def astype(self, name: str, portex_type: PortexType) -> None:
        """Convert the type of the field with the given name to the new PortexType.

        Arguments:
            name: The name of the field to convert.
            portex_type: The new PortexType of the field to convert to.

        Raises:
            TypeError: When calling this method of FrozenFields.

        """
        raise TypeError(f"Cannot change the type of '{name}' in {self.__class__.__name__}")

    def rename(self, old_name: str, new_name: str) -> None:
        """Rename the name of a field.

        Arguments:
            old_name: The current name of the field to be renamed.
            new_name: The new name of the field to assign.

        Raises:
            TypeError: When calling this method of FrozenFields.

        """
        raise TypeError(f"Cannot rename '{old_name}' in {self.__class__.__name__}")


class Fields(NameOrderedDict[PortexType], FrozenFields):  # type: ignore[misc]
    """Represents a Portex ``record`` fields dict."""

    def __init__(
        self, fields: Union[Iterable[Tuple[str, PortexType]], Mapping[str, PortexType], None] = None
    ):
        super().__init__(fields)

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


UnionFields = Union[FrozenFields, Fields]


class ConnectedFields(MutableMapping[str, PortexType]):
    """Fields composed of FrozenFields and Fields.

    Raises:
        ValueError: When there as repeated field names.

    Arguments:
        multi_fields: The FrozenFields and Fields.

    """

    def __init__(self, multi_fields: Iterable[UnionFields]) -> None:
        multi_fields = list(multi_fields)
        field_keys: Set[str] = set()
        for fields in multi_fields:
            keys = fields.keys()
            repeated_keys = field_keys & keys
            if repeated_keys:
                raise ValueError(f"Repeated field names '{repeated_keys}'")
            field_keys.update(keys)

        self._mapping: ChainMapType[str, PortexType] = ChainMap()
        self._mapping.maps = multi_fields  # type: ignore[assignment]
        self._sequence = multi_fields

    def __len__(self) -> int:
        return sum(map(len, self._sequence))

    def __getitem__(self, key: Union[int, str]) -> PortexType:
        if isinstance(key, int):
            i, j = self._get_indices(key)
            return self._sequence[i][j]

        return self._mapping[key]

    def __setitem__(self, key: Union[int, str], value: PortexType) -> None:
        if isinstance(key, int):
            i, key = self._get_indices(key)
            fields = self._sequence[i]
        else:
            try:
                fields = self._get_fields(key)
            except KeyError:
                fields = self._sequence[-1]

        fields[key] = value

    def __delitem__(self, key: Union[int, str]) -> None:
        if isinstance(key, int):
            i, key = self._get_indices(key)
            fields = self._sequence[i]
        else:
            fields = self._get_fields(key)

        del fields[key]

    def __iter__(self) -> Iterator[str]:
        yield from chain(*self._sequence)

    def _get_fields(self, key: str) -> UnionFields:
        for fields in self._sequence:
            if key in fields:
                return fields

        raise KeyError(key)

    def _get_indices(self, index: int) -> Tuple[int, int]:
        if index < 0:
            index = self.__len__() + index

        offset = index
        for i, length in enumerate(map(len, self._sequence)):
            if offset < length:
                return i, offset
            offset -= length

        raise IndexError("Index out of range")

    def insert(self, index: int, name: str, portex_type: PortexType) -> None:
        """Insert the name and portex_type at the index.

        Arguments:
            index: The index to insert the field.
            name: The name of the field to be inserted.
            portex_type: The portex_type of the field to be inserted.

        Raises:
            ValueError: When the name already exists in the fields.
            TypeError: When trying to insert a field into FrozenFields.

        """
        if name in self._mapping:
            raise ValueError(f"The '{name}' field already exists")

        i, j = self._get_indices(index)

        try:
            self._sequence[i].insert(j, name, portex_type)
            return
        except TypeError:
            pass

        if j == 0 and i > 0:
            fields = self._sequence[i - 1]
            try:
                fields[name] = portex_type
                return
            except TypeError:
                pass

        raise TypeError(f"Cannot insert at index {index} due to frozen fields") from None

    def astype(self, name: str, portex_type: PortexType) -> None:
        """Convert the type of the field with the given name to the new PortexType.

        Arguments:
            name: The name of the field to convert.
            portex_type: The new PortexType of the field to convert to.

        """
        self._get_fields(name).astype(name, portex_type)

    def rename(self, old_name: str, new_name: str) -> None:
        """Rename the name of a field.

        Arguments:
            old_name: The current name of the field to be renamed.
            new_name: The new name of the field to assign.

        """
        self._get_fields(old_name).rename(old_name, new_name)
