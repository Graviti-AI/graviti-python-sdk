#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the Graviti indexing related class."""

from typing import TYPE_CHECKING, Any, Iterable, Union, overload

if TYPE_CHECKING:
    from graviti.dataframe.column.series import SeriesBase


class ColumnSeriesILocIndexer:
    """Index class for ColumnSeries.iloc."""

    def __init__(self, obj: "SeriesBase") -> None:
        self.obj: "SeriesBase" = obj

    # @overload
    # def __getitem__(self, key: Union[Iterable[bool], slice]) -> "ColumnSeries":
    #    ...

    # @overload
    # def __getitem__(self, key: int) -> Any:
    #     ...

    # @overload
    # def __getitem__(self, key: Iterable[int]) -> "ColumnSeries":
    #     ...

    def __getitem__(self, key: int) -> Any:
        return self.obj._getitem_by_location(key)

    @overload
    def __setitem__(self, key: slice, value: Union[Iterable[Any], "SeriesBase"]) -> None:
        ...

    @overload
    def __setitem__(self, key: int, value: Any) -> None:
        ...

    def __setitem__(
        self,
        key: Union[int, slice],
        value: Union[Any, Iterable[Any], "SeriesBase"],
    ) -> None:
        self.obj.__setitem__(key, value)


class ColumnSeriesLocIndexer:
    """Index class for ColumnSeries.loc."""

    def __init__(self, obj: "SeriesBase") -> None:
        self.obj: "SeriesBase" = obj

    # @overload
    # def __getitem__(self, key: Union[Iterable[bool], slice]) -> "ColumnSeries":
    #    ...

    # @overload
    # def __getitem__(self, key: int) -> Any:
    #     ...

    # @overload
    # def __getitem__(self, key: Iterable[int]) -> "ColumnSeries":
    #     ...

    def __getitem__(self, key: int) -> Any:
        return self.obj[key]

    @overload
    def __setitem__(self, key: slice, value: Union[Iterable[Any], "SeriesBase"]) -> None:
        ...

    @overload
    def __setitem__(self, key: int, value: Any) -> None:
        ...

    def __setitem__(
        self,
        key: Union[int, slice],
        value: Union[Any, Iterable[Any], "SeriesBase"],
    ) -> None:
        self.obj.__setitem__(key, value)
