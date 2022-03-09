#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the Graviti Series."""

from typing import Generic, TypeVar

from graviti.dataframe.indexing import SeriesILocIndexer, SeriesLocIndexer

_T = TypeVar("_T", int, str)


class SeriesBase(Generic[_T]):
    """Base of the series.

    Arguments:
        data: The data that needs to be stored in Series. Could be ndarray or Iterable.
        schema: Data type to force. Only a single dtype is allowed. If None, will be
            inferred from `data`.
        name: The name to the Series.

    """

    def __repr__(self) -> str:
        pass

    @property
    def iloc(self) -> SeriesILocIndexer[_T]:
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
