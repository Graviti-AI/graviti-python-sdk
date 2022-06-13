#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#
"""The table-structured data container related classes."""


from typing import Any, ClassVar, List, Type, TypeVar

import pyarrow as pa

from graviti.paging.base import LazyFactoryBase
from graviti.portex import PortexType

_T = TypeVar("_T", bound="Container")


class Container:
    """The base class for the table-structured data container."""

    has_keys: ClassVar[bool]
    schema: PortexType

    def __len__(self) -> int:
        raise NotImplementedError

    @classmethod
    def _from_factory(cls: Type[_T], factory: LazyFactoryBase, schema: PortexType) -> _T:
        raise NotImplementedError

    @classmethod
    def _from_pyarrow(cls: Type[_T], array: pa.Array, schema: PortexType) -> _T:
        raise NotImplementedError

    def _extend(self: _T, values: _T) -> None:
        raise NotImplementedError

    # TODO: Defines a base indexer for the iloc return type.
    @property
    def iloc(self) -> Any:
        """Purely integer-location based indexing for selection by position.

        Raises:
            NotImplementedError: The method of the base class should not be called.

        """
        raise NotImplementedError

    def to_pylist(self) -> List[Any]:
        """Convert the container to a python list.

        Raises:
            NotImplementedError: The method of the base class should not be called.

        """
        raise NotImplementedError

    def copy(self: _T) -> _T:
        """Get a copy of the container.

        Raises:
            NotImplementedError: The method of the base class should not be called.

        """
        raise NotImplementedError
