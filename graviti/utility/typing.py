#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Graviti customized types."""

import inspect
from pathlib import Path
from typing import AbstractSet, Any, Tuple, Type, TypeVar, Union

from typing_extensions import Protocol

_K = TypeVar("_K")
_V = TypeVar("_V")

PathLike = Union[str, Path]


class NestedDict(Protocol[_K, _V]):
    """Typehint for nested dict."""

    def __contains__(self, key: _K) -> bool:
        ...

    def __getitem__(self, key: _K) -> Union["NestedDict[_K, _V]", _V]:
        ...

    def __setitem__(self, key: _K, value: Union["NestedDict[_K, _V]", _V]) -> None:
        ...

    def items(self) -> AbstractSet[Tuple[_K, Union["NestedDict[_K, _V]", _V]]]:
        """Return (key, value) pairs of the dict."""
        ...

    def setdefault(
        self, key: _K, default: Union["NestedDict[_K, _V]", _V]
    ) -> Union["NestedDict[_K, _V]", _V]:
        """Get the value of the key if exists, else set the value as default and return.

        Arguments:
            key: The key.
            default: The default value to set if the key does not exist.

        """
        ...


def check_type(name: str, value: Any, expected_type: Type[Any]) -> None:
    """Check the type of the argument.

    Arguments:
        name: The name of the argument.
        value: The value of the argument.
        expected_type: The type of the argument.

    Raises:
        TypeError: When the value is not of the type.

    """
    if not isinstance(value, expected_type):
        raise TypeError(
            f'{inspect.stack()[1][3]}(): "{name}" must be {expected_type}, not {type(value)}'
        )
