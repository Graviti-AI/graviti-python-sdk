#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the Graviti indexing related class."""

from typing import TYPE_CHECKING, Any, Iterable, Union, overload

if TYPE_CHECKING:
    from graviti.dataframe.column.series import Series as ColumnSeries


class ColumnSeriesILocIndexer:
    """Index class for ColumnSeries.iloc."""

    def __init__(self, obj: "ColumnSeries") -> None:
        self.obj: "ColumnSeries" = obj

    # @overload
    # def __getitem__(self, key: Union[Iterable[bool], slice]) -> "ColumnSeries":
    #    ...

    @overload
    def __getitem__(self, key: int) -> Any:
        ...

    @overload
    def __getitem__(self, key: Iterable[int]) -> "ColumnSeries":
        ...

    def __getitem__(self, key: Union[int, Iterable[int]]) -> Any:
        return self.obj._getitem_by_location(key)

    @overload
    def __setitem__(self, key: slice, value: Iterable[Any]) -> None:
        ...

    @overload
    def __setitem__(self, key: int, value: Any) -> None:
        ...

    def __setitem__(
        self,
        key: Union[int, slice],
        value: Union[Iterable[Any], Any],
    ) -> None:
        pass


class ColumnSeriesLocIndexer:
    """Index class for ColumnSeries.loc."""

    def __init__(self, obj: "ColumnSeries") -> None:
        self.obj: "ColumnSeries" = obj

    # @overload
    # def __getitem__(self, key: Union[Iterable[bool], slice]) -> "ColumnSeries":
    #    ...

    @overload
    def __getitem__(self, key: int) -> Any:
        ...

    @overload
    def __getitem__(self, key: Iterable[int]) -> "ColumnSeries":
        ...

    def __getitem__(self, key: Union[int, Iterable[int]]) -> Any:
        return self.obj[key]

    @overload
    def __setitem__(self, key: slice, value: Iterable[Any]) -> None:
        ...

    @overload
    def __setitem__(self, key: int, value: Any) -> None:
        ...

    def __setitem__(
        self,
        key: Union[int, slice],
        value: Union[Iterable[Any], Any],
    ) -> None:
        pass
