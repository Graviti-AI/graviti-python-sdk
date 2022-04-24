#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the Graviti indexing related class."""

from typing import TYPE_CHECKING, Any, Iterable, Union, overload

if TYPE_CHECKING:
    from graviti.dataframe.row.series import Series as RowSeries


class RowSeriesILocIndexer:
    """Index class for RowSeries.iloc."""

    def __init__(self, obj: "RowSeries") -> None:
        self.obj: "RowSeries" = obj

    # @overload
    # def __getitem__(self, key: Union[Iterable[bool], slice]) -> "RowSeries":
    #    ...

    @overload
    def __getitem__(self, key: int) -> Any:
        ...

    @overload
    def __getitem__(self, key: Iterable[int]) -> "RowSeries":
        ...

    def __getitem__(self, key: Union[int, Iterable[int]]) -> Any:
        return self.obj._getitem_by_location(key)

    def __setitem__(self, key: int, value: Any) -> None:
        pass


class RowSeriesLocIndexer:
    """Index class for RowSeries.loc."""

    def __init__(self, obj: "RowSeries") -> None:
        self.obj: "RowSeries" = obj

    # @overload
    # def __getitem__(self, key: Union[Iterable[bool], slice]) -> "RowSeries":
    #    ...

    @overload
    def __getitem__(self, key: str) -> Any:
        ...

    @overload
    def __getitem__(self, key: Iterable[str]) -> "RowSeries":
        ...

    def __getitem__(self, key: Union[str, Iterable[str]]) -> Any:
        return self.obj[key]

    def __setitem__(self, key: str, value: Any) -> None:
        pass
