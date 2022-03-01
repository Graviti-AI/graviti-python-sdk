#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the Graviti Series."""

from typing import Any, Dict, Generic, Iterable, List, Optional, Tuple, TypeVar, Union, overload

from graviti.dataframe.indexing import SeriesILocIndexer, SeriesLocIndexer

_T = TypeVar("_T", int, str)


class Series(Generic[_T]):
    """One-dimensional ndarray.

    Arguments:
        data: The data that needs to be stored in Series. Could be ndarray or Iterable.
        schema: Data type to force. Only a single dtype is allowed. If None, will be
            inferred from `data`.
        name: The name to the Series.

    Examples:
        Constructing Series from a list.

        >>> d = {"filename": "a.jpg", "attributes": {"color": "red", "pose": "frontal"}}
        >>> series = Series(data=d)
        >>> series
        filename         a.jpg
        attributes color red
                   pose  frontal
        schema: string

    """

    _data: List[Any]
    _indices_data: Dict[_T, int]
    _indices: List[_T]

    def __init__(  # pylint: disable=too-many-arguments
        self,
        data: Optional[Dict[_T, Any]] = None,
        schema: Any = None,
        name: Optional[Union[str, int]] = None,
    ) -> None:
        if data is None:
            data = {}
        if schema is not None:
            # TODO: missing schema processing
            pass

        self._data = []
        self._indices_data = {}
        for key, value in data.items():
            if isinstance(value, dict):
                value = Series(value, name=name)
            self._data.append(value)
            self._indices_data[key] = value
        self._indices = list(data.keys())
        self.name: Optional[Union[str, int]] = name

    # @overload
    # def __getitem__(self, key: slice) -> "Series":
    #    ...

    @overload
    def __getitem__(self, key: Tuple[_T]) -> "Series[_T]":
        ...

    @overload
    def __getitem__(self, key: _T) -> Any:
        ...

    @overload
    def __getitem__(self, key: Iterable[_T]) -> "Series[_T]":
        ...

    def __getitem__(self, key: Union[_T, Iterable[_T]]) -> Any:
        if isinstance(key, (str, int)):
            return self._indices_data[key]

        new_data = {name: self._indices_data[name] for name in key}
        return Series(new_data, name=self.name)

    def __repr__(self) -> str:
        pass

    def __len__(self) -> int:
        return self._data.__len__()

    # @overload
    # def _getitem_by_location(self, key: slice) -> "Series":
    #    ...

    @overload
    def _getitem_by_location(self, key: int) -> Union["Series[_T]", Any]:
        ...

    @overload
    def _getitem_by_location(self, key: Iterable[int]) -> "Series[_T]":
        ...

    def _getitem_by_location(self, key: Union[int, Iterable[int]]) -> Union["Series[_T]", Any]:
        if isinstance(key, int):
            return self._data[key]

        new_data = {self._indices[index]: self._data[index] for index in key}
        return Series(new_data, name=self.name)

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
