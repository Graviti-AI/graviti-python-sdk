#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#
"""The table-structured data container related classes."""


from typing import TYPE_CHECKING, Any, Iterable, List, Optional, Tuple, Type, TypeVar, Union

import pyarrow as pa

from graviti.paging import LazyFactoryBase
from graviti.portex import PortexType

if TYPE_CHECKING:
    import pandas

    from graviti.dataframe.frame import DataFrame
    from graviti.manager.permission import ObjectPermissionManager

_T = TypeVar("_T", bound="Container")


class Container:
    """The base class for the table-structured data container."""

    schema: PortexType
    _root: Optional["DataFrame"] = None
    _name: Tuple[str, ...] = ()

    def __len__(self) -> int:
        raise NotImplementedError

    @classmethod
    def _create(
        cls: Type[_T],
        schema: PortexType,
        root: Optional["DataFrame"] = None,
        name: Tuple[str, ...] = (),
    ) -> _T:
        obj: _T = object.__new__(cls)
        obj.schema = schema
        obj._root = root
        obj._name = name

        return obj

    @classmethod
    def _from_factory(
        cls: Type[_T],
        factory: LazyFactoryBase,
        schema: PortexType,
        root: Optional["DataFrame"] = None,
        name: Tuple[str, ...] = (),
        *,
        object_permission_manager: Optional["ObjectPermissionManager"] = None,
    ) -> _T:
        raise NotImplementedError

    @classmethod
    def _from_pyarrow(
        cls: Type[_T],
        array: pa.Array,
        schema: PortexType,
        root: Optional["DataFrame"] = None,
        name: Tuple[str, ...] = (),
        *,
        object_permission_manager: Optional["ObjectPermissionManager"] = None,
    ) -> _T:
        raise NotImplementedError

    @classmethod
    def _from_iterable(
        cls: Type[_T],
        array: Iterable[Any],
        schema: PortexType,
    ) -> _T:
        raise NotImplementedError

    def _extend(self: _T, values: _T) -> None:
        raise NotImplementedError

    def _repr_folding(self) -> str:
        raise NotImplementedError

    def _copy(
        self: _T, schema: PortexType, root: Optional["DataFrame"] = None, name: Tuple[str, ...] = ()
    ) -> _T:
        raise NotImplementedError

    def _get_item_by_location(self, key: int) -> Any:
        raise NotImplementedError

    def _get_slice_by_location(
        self: _T,
        key: slice,
        schema: PortexType,
        root: Optional["DataFrame"] = None,
        name: Tuple[str, ...] = (),
    ) -> _T:
        raise NotImplementedError

    def _del_item_by_location(self, key: Union[int, slice]) -> None:
        raise NotImplementedError

    def _refresh_data_from_factory(
        self,
        factory: LazyFactoryBase,
        object_permission_manager: Optional["ObjectPermissionManager"],
    ) -> None:
        raise NotImplementedError

    def _to_pandas_series(self) -> "pandas.Series":
        raise NotImplementedError

    # TODO: Defines a base indexer for the loc and iloc return type.
    @property
    def iloc(self) -> Any:
        """Purely integer-location based indexing for selection by position.

        Raises:
            NotImplementedError: The method of the base class should not be called.

        """
        raise NotImplementedError

    @property
    def loc(self) -> Any:
        """Access the row by index.

        Raises:
            NotImplementedError: The method of the base class should not be called.

        """
        raise NotImplementedError

    def to_pylist(self, *, _to_backend: bool = False) -> List[Any]:
        """Convert the container to a python list.

        Raises:
            NotImplementedError: The method of the base class should not be called.

        """
        raise NotImplementedError

    def to_pandas(self) -> Union["pandas.Series", "pandas.DataFrame"]:
        """Convert the graviti Container to a pandas Series or DataFrame.

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
