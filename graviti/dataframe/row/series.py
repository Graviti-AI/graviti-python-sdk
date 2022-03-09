#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the Graviti Series."""

from typing import Any, Dict, Iterable, List, Optional, Tuple, Union, overload

from graviti.dataframe.series import SeriesBase


class Series(SeriesBase[str]):
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

    _indices_data: Dict[str, Any]
    _indices: List[str]

    def __init__(
        self,
        data: Optional[Dict[str, Any]] = None,
        schema: Any = None,
        name: Union[str, int, None] = None,
        index: Optional[List[str]] = None,
    ) -> None:
        if data is None:
            data = {}
        if schema is not None:
            # TODO: missing schema processing
            pass
        if index is not None:
            # TODO: missing index processing
            pass

        self._indices_data, self._indices = {}, []
        for key, value in data.items():
            if isinstance(value, dict):
                value = Series(value, name=name)
            self._indices_data[key] = value
            self._indices.append(key)
        self.name = name

    # @overload
    # def __getitem__(self, key: slice) -> "Series":
    #    ...

    @overload
    def __getitem__(self, key: Tuple[str]) -> "Series":
        ...

    @overload
    def __getitem__(self, key: str) -> Any:
        ...

    @overload
    def __getitem__(self, key: Iterable[str]) -> "Series":
        ...

    def __getitem__(self, key: Union[str, Iterable[str]]) -> Any:
        if isinstance(key, str):
            return self._indices_data[key]

        new_data = {name: self._indices_data[name] for name in key}
        return Series(new_data, name=self.name)

    def __len__(self) -> int:
        return self._indices.__len__()

    @classmethod
    def _construct(cls, indices_data: Dict[str, Any], new_name: Union[str, int, None]) -> "Series":
        obj: Series = object.__new__(cls)
        # pylint: disable=protected-access
        obj._indices_data = indices_data
        obj._indices = list(indices_data.keys())
        obj.name = new_name
        return obj

    # @overload
    # def _getitem_by_location(self, key: slice) -> "Series":
    #    ...

    @overload
    def _getitem_by_location(self, key: int) -> Union["Series", Any]:
        ...

    @overload
    def _getitem_by_location(self, key: Iterable[int]) -> "Series":
        ...

    def _getitem_by_location(self, key: Union[int, Iterable[int]]) -> Any:
        if isinstance(key, int):
            return self._indices_data[self._indices[key]]

        indices_data = {
            self._indices[index]: self._indices_data[self._indices[index]] for index in key
        }
        return self._construct(indices_data, self.name)
