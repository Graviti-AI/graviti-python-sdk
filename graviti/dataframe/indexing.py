#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the Graviti indexing related class."""

from typing import TYPE_CHECKING, Any, Generic, Iterable, Tuple, TypeVar, Union, overload

if TYPE_CHECKING:
    from graviti.dataframe.dataframe import DataFrame
    from graviti.dataframe.row.series import Series
    from graviti.dataframe.series import SeriesBase

_T = TypeVar("_T", int, str)


class SeriesILocIndexer(Generic[_T]):  # pylint: disable=too-few-public-methods
    """Index class for Series.iloc."""

    def __init__(self, obj: "SeriesBase[_T]") -> None:
        pass

    @overload
    def __getitem__(self, key: int) -> Any:
        ...

    @overload
    def __getitem__(self, key: Union[Iterable[int], Iterable[bool], slice]) -> "SeriesBase[_T]":
        ...

    def __getitem__(self, key: Union[int, Iterable[int], Iterable[bool], slice]) -> Any:
        pass


class SeriesLocIndexer(Generic[_T]):  # pylint: disable=too-few-public-methods
    """Index class for Series.loc."""

    def __init__(self, obj: "SeriesBase[_T]") -> None:
        pass

    @overload
    def __getitem__(self, key: _T) -> Any:
        ...

    @overload
    def __getitem__(self, key: Union[Iterable[_T], Iterable[bool], slice]) -> "SeriesBase[_T]":
        ...

    def __getitem__(self, key: Union[_T, Iterable[_T], Iterable[bool], slice]) -> Any:
        pass


class DataFrameILocIndexer:  # pylint: disable=too-few-public-methods
    """Index class for DataFrame.iloc."""

    def __init__(self, obj: "DataFrame") -> None:
        pass

    @overload
    def __getitem__(self, key: Tuple[int, str]) -> Any:
        ...

    @overload
    def __getitem__(self, key: int) -> "Series[str]":
        ...

    @overload
    def __getitem__(self, key: Union[Iterable[bool], Iterable[int], slice]) -> "DataFrame":
        ...

    def __getitem__(
        self, key: Union[Tuple[int, str], int, Iterable[bool], Iterable[int], slice]
    ) -> Any:
        pass


class DataFrameLocIndexer:  # pylint: disable=too-few-public-methods
    """Index class for DataFrame.loc."""

    def __init__(self, obj: "DataFrame") -> None:
        pass

    @overload
    def __getitem__(self, key: Tuple[int, str]) -> Any:
        ...

    @overload
    def __getitem__(self, key: int) -> "Series[str]":
        ...

    @overload
    def __getitem__(self, key: Union[Iterable[bool], Iterable[int], slice]) -> "DataFrame":
        ...

    def __getitem__(
        self, key: Union[Tuple[int, str], int, Iterable[bool], Iterable[int], slice]
    ) -> Any:
        pass
