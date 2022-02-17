#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the Graviti Series."""

from typing import Any, Generic, Iterable, Optional, TypeVar, Union, overload

from graviti.dataframe.indexing import SeriesILocIndexer, SeriesLocIndexer

_T = TypeVar("_T", int, str)


class Series(Generic[_T]):
    """One-dimensional ndarray.

    Arguments:
        data: The data that needs to be stored in Series. Could be ndarray or Iterable.
        schema: Data type to force. Only a single dtype is allowed. If None, will be
            inferred from `data`.
        name: The name to the Series.
        copy: Copy input data. Only affects Series or 1d ndarray input.
        client: The client for getting a remote data.

    Examples:
        Constructing Series from a list.

        >>> d = [1, 2, 3]
        >>> series = Series(data=d)
        >>> series
        0   1
        1   2
        2   3
        schema: int64

    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        data: Optional[Iterable[Any]] = None,
        schema: Any = None,
        name: Optional[str] = None,
        copy: Optional[bool] = None,
        client: Any = None,
    ) -> None:
        pass

    @overload
    def __getitem__(self, key: _T) -> Any:
        ...

    @overload
    def __getitem__(self, key: Union[Iterable[_T], slice]) -> "Series[_T]":
        ...

    def __getitem__(self, key: Union[_T, Iterable[_T], slice]) -> Any:
        pass

    def __repr__(self) -> str:
        pass

    def __len__(self) -> int:
        pass

    @property
    def iloc(self) -> SeriesILocIndexer:
        """Purely integer-location based indexing for selection by position.

        Allowed inputs are:

        - An integer, e.g. ``5``.
        - A list or array of integers, e.g. ``[4, 3, 0]``.
        - A slice object with ints, e.g. ``1:7``.
        - A boolean array of the same length as the axis being sliced.

        Returns:
            The instance of the ILocIndexer.

        Examples:
            >>> series = Series([1, 2, 3])
            >>> series.loc[0]
            1
            >>> df.loc[[0]]
            0    1
            dtype: int64

        """
        return SeriesILocIndexer(self)

    @property
    def loc(self) -> SeriesLocIndexer[_T]:
        """Access a group of rows and columns by indexes or a boolean array.

        Allowed inputs are:

        - A single index, e.g. ``5``.
        - A list or array of indexes, e.g. ``[4, 3, 0]``.
        - A slice object with indexes, e.g. ``1:7``.
        - A boolean array of the same length as the axis being sliced.

        Returns:
            The instance of the LocIndexer.

        Examples:
            >>> series = Series([1, 2, 3])
            >>> series.loc[0]
            1
            >>> df.loc[[0]]
            0    1
            dtype: int64

        """
        return SeriesLocIndexer(self)
