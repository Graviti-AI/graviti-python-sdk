#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the Graviti Series."""

from typing import Any, Dict, Iterable, List, Optional, Tuple, TypeVar, Union, overload

from graviti.dataframe.series import SeriesBase

_T = TypeVar("_T", int, str)


class Series(SeriesBase[_T]):
    """One-dimensional array.

    Arguments:
        data: The data that needs to be stored in Series. Could be ndarray or Iterable.
        schema: Data type to force. If None, will be inferred from ``data``.
        name: The name to the Series.
        index: Index of the ``data``.

    Examples:
        Constructing Series from a list.

        >>> d = {"filename": "a.jpg", "attributes": {"color": "red", "pose": "frontal"}}
        >>> series = Series(data=d)
        >>> series
        filename           a.jpg
        attributes color     red
                    pose frontal
        schema: string

    """

    _data: List[Any]
    _indices_data: Dict[_T, int]
    _indices: List[_T]

    def __init__(
        self,
        data: Optional[Dict[_T, Any]] = None,
        schema: Any = None,
        name: Union[str, int, None] = None,
        index: Optional[List[_T]] = None,
    ) -> None:
        if data is None:
            data = {}
        if schema is not None:
            # TODO: missing schema processing
            pass
        if index is not None:
            # TODO: missing index processing
            pass

        self._data = []
        self._indices_data = {}
        for key, value in data.items():
            if isinstance(value, dict):
                value = Series(value, name=name)
            self._data.append(value)
            self._indices_data[key] = value
        self._indices = list(data.keys())
        self.name = name

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

    # @overload
    # def _getitem_by_location(self, key: slice) -> "Series":
    #    ...

    @overload
    def _getitem_by_location(self, key: int) -> Union["Series[_T]", Any]:
        ...

    @overload
    def _getitem_by_location(self, key: Iterable[int]) -> "Series[_T]":
        ...

    def _getitem_by_location(self, key: Union[int, Iterable[int]]) -> Any:
        if isinstance(key, int):
            return self._data[key]

        new_data = {self._indices[index]: self._data[index] for index in key}
        return Series(new_data, name=self.name)
