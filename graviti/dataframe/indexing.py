#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the Graviti indexing related class."""

from typing import TYPE_CHECKING, Any, Generic, Iterable, TypeVar, Union, overload

if TYPE_CHECKING:
    from graviti.dataframe.dataframe import DataFrame
    from graviti.dataframe.row.series import Series as RowSeries
    from graviti.dataframe.series import SeriesBase

_T = TypeVar("_T", int, str)


class SeriesILocIndexer(Generic[_T]):
    """Index class for Series.iloc."""

    def __init__(self, obj: "SeriesBase[_T]") -> None:
        self.obj: "SeriesBase[_T]" = obj

    # @overload
    # def __getitem__(self, key: Union[Iterable[bool], slice]) -> "SeriesBase[_T]":
    #    ...

    @overload
    def __getitem__(self, key: int) -> Any:
        ...

    @overload
    def __getitem__(self, key: Iterable[int]) -> "SeriesBase[_T]":
        ...

    def __getitem__(self, key: Union[int, Iterable[int]]) -> Any:
        return self.obj._getitem_by_location(key)


class SeriesLocIndexer(Generic[_T]):
    """Index class for Series.loc."""

    def __init__(self, obj: "SeriesBase[_T]") -> None:
        self.obj: "SeriesBase[_T]" = obj

    # @overload
    # def __getitem__(self, key: Union[Iterable[bool], slice]) -> "SeriesBase[_T]":
    #    ...

    @overload
    def __getitem__(self, key: _T) -> Any:
        ...

    @overload
    def __getitem__(self, key: Iterable[_T]) -> "SeriesBase[_T]":
        ...

    def __getitem__(self, key: Union[_T, Iterable[_T]]) -> Any:
        return self.obj[key]


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
