#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#
"""The table-structured data container related classes."""


from typing import TYPE_CHECKING, Any, List, Optional, Type, TypeVar

import pyarrow as pa

from graviti.paging import LazyFactoryBase
from graviti.portex import PortexType

if TYPE_CHECKING:
    from graviti.dataframe.frame import DataFrame

_T = TypeVar("_T", bound="Container")


class Container:
    """The base class for the table-structured data container."""

    schema: PortexType
    _parent: Optional["DataFrame"] = None

    def __len__(self) -> int:
        raise NotImplementedError

    @classmethod
    def _from_factory(
        cls: Type[_T],
        factory: LazyFactoryBase,
        schema: PortexType,
        parent: Optional["DataFrame"] = None,
    ) -> _T:
        raise NotImplementedError

    @classmethod
    def _from_pyarrow(
        cls: Type[_T], array: pa.Array, schema: PortexType, parent: Optional["DataFrame"] = None
    ) -> _T:
        raise NotImplementedError

    def _extend(self: _T, values: _T) -> None:
        raise NotImplementedError

    def _repr_folding(self) -> str:
        raise NotImplementedError

    def _copy(self: _T, schema: PortexType) -> _T:
        raise NotImplementedError

    def _set_item_by_slice(self, key: slice, value: Any) -> None:
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

        Returns:
            A copy of the container.

        """
        return self._copy(self.schema.copy())
