#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the Graviti indexing related class."""

from typing import TYPE_CHECKING, Any, Iterable, TypeVar, Union, overload

from graviti.utility import NestedDict

if TYPE_CHECKING:
    from graviti.dataframe.dataframe import DataFrame
    from graviti.dataframe.row.series import Series as RowSeries

_T = TypeVar("_T", int, str)


class DataFrameILocIndexer:
    """Index class for DataFrame.iloc."""

    def __init__(self, obj: "DataFrame") -> None:
        self.obj = obj

    # @overload
    # def __getitem__(self, key: Union[Tuple[int, str], Iterable[bool], slice]) -> Any:
    #     ...

    @overload
    def __getitem__(self, key: int) -> "RowSeries":
        ...

    @overload
    def __getitem__(self, key: Iterable[int]) -> "DataFrame":
        ...

    def __getitem__(self, key: Union[int, Iterable[int]]) -> Any:
        return self.obj._getitem_by_location(key)

    @overload
    def __setitem__(self, key: int, value: NestedDict[str, Any]) -> None:
        ...

    @overload
    def __setitem__(self, key: slice, value: Iterable[NestedDict[str, Any]]) -> None:
        ...

    def __setitem__(
        self,
        key: Union[int, slice],
        value: Union[NestedDict[str, Any], Iterable[NestedDict[str, Any]]],
    ) -> None:
        pass


class DataFrameLocIndexer:
    """Index class for DataFrame.loc."""

    def __init__(self, obj: "DataFrame") -> None:
        self.obj = obj

    # @overload
    # def __getitem__(self, key: Union[Tuple[int, str], Iterable[bool], slice]) -> Any:
    #     ...

    @overload
    def __getitem__(self, key: int) -> "RowSeries":
        ...

    @overload
    def __getitem__(self, key: Iterable[int]) -> "DataFrame":
        ...

    def __getitem__(self, key: Union[int, Iterable[int]]) -> Any:
        return self.obj._getitem_by_location(self.obj._get_location_by_index(key))

    @overload
    def __setitem__(self, key: _T, value: NestedDict[str, Any]) -> None:
        ...

    @overload
    def __setitem__(self, key: slice, value: Iterable[NestedDict[str, Any]]) -> None:
        ...

    def __setitem__(
        self,
        key: Union[_T, slice],
        value: Union[NestedDict[str, Any], Iterable[NestedDict[str, Any]]],
    ) -> None:
        pass
