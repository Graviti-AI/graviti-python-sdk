#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#
"""Portex enum values releated classes."""


from typing import Any, Dict, Iterable, Mapping, Optional, Sequence, Tuple, Union

from graviti.utility import UserMapping, UserSequence


class EnumValues:
    """The base class of portex enum values."""

    index_scope: Tuple[int, int]
    value_to_index: Dict[Any, Optional[int]]
    index_to_value: Dict[Optional[int], Any]

    def _init_mapping(self, value_to_index: Dict[Any, Optional[int]]) -> None:
        if None not in value_to_index:
            value_to_index[None] = None

        self.value_to_index = value_to_index
        self.index_to_value = {value: key for key, value in value_to_index.items()}


class EnumValueList(EnumValues, UserSequence[Any]):
    """The portex enum values in list format.

    Arguments:
        values: The enum values.

    """

    def __init__(self, values: Iterable[Any]) -> None:
        self._data = list(values)
        self._init()

    def _init(self) -> None:
        self.index_scope = (0, len(self._data) - 1)

        value_to_index: Dict[Any, Optional[int]] = {
            value: key for key, value in enumerate(self._data)
        }
        self._init_mapping(value_to_index)


class EnumValueDict(EnumValues, UserMapping[int, Any]):
    """The portex enum values in dict format.

    Arguments:
        values: The enum values.

    """

    def __init__(self, values: Mapping[int, Any]) -> None:
        self._data = dict(values)
        self._init()

    def _init(self) -> None:
        value_to_index = {value: key for key, value in self._data.items()}

        self.index_scope = (min(value_to_index.values()), max(value_to_index.values()))

        self._init_mapping(value_to_index)  # type: ignore[arg-type]


def create_enum_values(values: Union[Sequence[Any], Mapping[int, Any]]) -> EnumValues:
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
