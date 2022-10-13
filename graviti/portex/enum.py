#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#
"""Portex enum values releated classes."""


from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, Type, Union

import pyarrow as pa

from graviti.utility import INDENT, UserMapping, UserSequence

_Types = (str, int, bool, type(None))
EnumValueType = Union[str, int, bool, None]


class EnumValues:
    """The base class of portex enum values."""

    index_scope: Tuple[int, int]
    value_to_index: Dict[EnumValueType, Optional[int]]
    index_to_value: Dict[Optional[int], EnumValueType]

    def __repr__(self) -> str:
        return self._repr1(0)

    def _init_mapping(self, value_to_index: Dict[EnumValueType, Optional[int]]) -> None:
        if None not in value_to_index:
            value_to_index[None] = None

        self.value_to_index = value_to_index
        self.index_to_value = {value: key for key, value in value_to_index.items()}

    @staticmethod
    def _check_value_type(value: Any) -> EnumValueType:
        if not isinstance(value, _Types):
            raise TypeError(
                f"The type of enum value ({value}) must be 'str', 'int', 'bool' or None"
            )

        return value

    def _repr1(self, level: int) -> str:
        raise NotImplementedError

    def to_pyobj(self) -> Union[List[EnumValueType], Dict[int, EnumValueType]]:
        """Dump the instance to a python list or dict.

        Raises:
            NotImplementedError: The method of the base class should not be called.

        """
        raise NotImplementedError

    def to_pyarrow(self) -> pa.Array:
        """Dump the instance to a pyarrow array.

        Raises:
            NotImplementedError: The method of the base class should not be called.

        """
        raise NotImplementedError


class EnumValueList(EnumValues, UserSequence[EnumValueType]):
    """The portex enum values in list format.

    Arguments:
        values: The enum values.

    """

    _data: List[EnumValueType]

    def __init__(self, values: Iterable[EnumValueType]) -> None:
        checker = self._check_value_type
        self._data = [checker(value) for value in values]
        self._init()

    def _init(self) -> None:
        self.index_scope = (0, len(self._data) - 1)

        value_to_index: Dict[EnumValueType, Optional[int]] = {
            value: key for key, value in enumerate(self._data)
        }
        self._init_mapping(value_to_index)

    def _repr1(self, level: int) -> str:
        lines = ["["]
        lines.extend(f"{INDENT}{repr(value)}," for value in self._data)
        lines.append("]")

        return f"\n{level * INDENT}".join(lines)

    def to_pyobj(self) -> List[EnumValueType]:
        """Dump the instance to a python list.

        Returns:
            A python list representation of the enum values.

        """
        return self._data.copy()

    def to_pyarrow(self) -> pa.Array:
        """Dump the instance to a pyarrow array.

        Returns:
            A pyarrow array representation of the enum values.

        """
        try:
            return pa.array(self._data)
        except (pa.ArrowInvalid, pa.ArrowTypeError):
            length = len(self._data)
            type_ids: List[Optional[int]] = [None] * length
            children: List[List[EnumValueType]] = []
            type_to_id: Dict[Type[EnumValueType], int] = {}
            next_type_id = 0

            for i, item in enumerate(self._data):
                type_id = type_to_id.setdefault(type(item), next_type_id)
                if type_id == next_type_id:
                    next_type_id += 1
                    children.append([None] * length)

                type_ids[i] = type_id
                children[type_id][i] = item

            return pa.UnionArray.from_sparse(
                pa.array(type_ids, pa.int8()), list(map(pa.array, children))
            )


class EnumValueDict(EnumValues, UserMapping[int, EnumValueType]):
    """The portex enum values in dict format.

    Arguments:
        values: The enum values.

    """

    _data: Dict[int, EnumValueType]

    def __init__(self, values: Mapping[int, EnumValueType]) -> None:
        checker = self._check_value_type
        try:
            self._data = {int(key): checker(value) for key, value in values.items()}
        except ValueError as error:
            raise TypeError("The portex enum index type must be 'int'") from error

        self._init()

    def _init(self) -> None:
        value_to_index = {value: key for key, value in self._data.items()}

        self.index_scope = (min(value_to_index.values()), max(value_to_index.values()))

        self._init_mapping(value_to_index)  # type: ignore[arg-type]

    def _repr1(self, level: int) -> str:
        lines = ["{"]
        lines.extend(f"{INDENT}{key}: {repr(value)}," for key, value in self._data.items())
        lines.append("}")

        return f"\n{level * INDENT}".join(lines)

    def to_pyobj(self) -> Dict[int, EnumValueType]:
        """Dump the instance to a python dict.

        Returns:
            A python dict representation of the enum values.

        """
        return self._data.copy()

    def to_pyarrow(self) -> pa.Array:
        """Dump the instance to a pyarrow array.

        Raises:
            TypeError: EnumValueDict is not supported converting to pyarrow.

        """
        raise TypeError(
            "The enum values in 'dict' format is not supported converting to 'pandas' or 'pyarrow'"
        )


def create_enum_values(
    values: Union[Sequence[EnumValueType], Mapping[int, EnumValueType]]
) -> EnumValues:
    """The factory function of EnumValues.

    Arguments:
        values: The enum values.

    Returns:
        The EnumValues instance created by the input enum values.

    Raises:
        TypeError: When the input enum values is not in list or dict format.

    """
    if isinstance(values, Sequence):
        return EnumValueList(values)

    if isinstance(values, Mapping):
        return EnumValueDict(values)

    raise TypeError("portex enum values should be a list or a dict")
