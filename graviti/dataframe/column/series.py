#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the Graviti Series."""


from itertools import islice
from typing import Any, Dict, Iterable, List, Optional, Sequence, Union, overload

from graviti.dataframe.series import SeriesBase
from graviti.utility import MAX_REPR_ROWS


class Series(SeriesBase[int]):
    """One-dimensional array.

    Arguments:
        data: The data that needs to be stored in Series. Could be ndarray or Iterable.
        schema: Data type to force. If None, will be inferred from ``data``.
        name: The name to the Series.
        index: Index of the data, must have the same length as ``data``.

    Examples:
        Constructing Series from a list.

        >>> d = [1,2,3,4]
        >>> series = Series(data=d)
        >>> series
        0 1
        1 2
        2 3
        3 4

    """

    _indices_data: Optional[Dict[int, int]]
    _indices: Optional[List[int]]

    def __init__(
        self,
        data: Sequence[Any],
        schema: Any = None,
        name: Union[str, int, None] = None,
        index: Optional[Iterable[int]] = None,
    ) -> None:
        if schema is not None:
            # TODO: missing schema processing
            pass

        self._data = data
        self.name = name
        if index is None:
            self._indices_data = index
            self._indices = index
        else:
            self._indices_data = {raw_index: location for location, raw_index in enumerate(index)}
            self._indices = list(index)

    # @overload
    # def __getitem__(self, key: slice) -> "Series":
    #    ...

    @overload
    def __getitem__(self, key: int) -> Any:
        ...

    @overload
    def __getitem__(self, key: Iterable[int]) -> "Series":
        ...

    def __getitem__(self, key: Union[int, Iterable[int]]) -> Any:
        integer_location = self._get_location_by_index(key)

        if isinstance(integer_location, list):
            # https://github.com/PyCQA/pylint/issues/3105
            new_data = [
                self._data[location]
                for location in integer_location  # pylint: disable=not-an-iterable
            ]
            return Series(new_data, name=self.name, index=integer_location)

        return self._data[integer_location]

    def __len__(self) -> int:
        return self._data.__len__()

    def __repr__(self) -> str:
        indices = list(self._get_repr_indices())
        indice_width = len(str(max(indices)))

        body = []
        body_item_width = 0
        for i in indices:
            item = self.loc[i]
            name = item._repr_folding() if hasattr(item, "_repr_folding") else str(item)
            body.append(name)
            body_item_width = max(len(name), body_item_width)

        lines = []
        for indice, value in zip(indices, body):
            lines.append(f"{indice:<{indice_width+2}}{value:<{body_item_width+2}}")
        if self.__len__() > MAX_REPR_ROWS:
            lines.append(f"...({self.__len__()})")
        if self.name:
            lines.append(f"Name: {self.name}")
        return "\n".join(lines)

    def _get_repr_indices(self) -> Iterable[int]:
        length = self.__len__()
        # pylint: disable=protected-access
        if self._indices is None:
            return range(min(length, MAX_REPR_ROWS))

        if length >= MAX_REPR_ROWS:
            return islice(self._indices, MAX_REPR_ROWS)

        return self._indices

    @overload
    def _get_location_by_index(self, key: Iterable[int]) -> List[int]:
        ...

    @overload
    def _get_location_by_index(self, key: int) -> int:
        ...

    def _get_location_by_index(self, key: Union[int, Iterable[int]]) -> Union[int, List[int]]:
        if self._indices_data is None:
            if isinstance(key, Iterable):
                return list(key)

            return key

        if isinstance(key, Iterable):
            return [self._indices_data[index] for index in key]

        return self._indices_data[key]

    # @overload
    # def _getitem_by_location(self, key: slice) -> "Series":
    #    ...

    @overload
    def _getitem_by_location(self, key: int) -> Any:
        ...

    @overload
    def _getitem_by_location(self, key: Iterable[int]) -> "Series":
        ...

    def _getitem_by_location(self, key: Union[int, Iterable[int]]) -> Any:
        if isinstance(key, int):
            return self._data[key]

        if self._indices is None:
            indices = list(key)
        else:
            indices = [self._indices[index] for index in key]
        new_data = [self._data[index] for index in key]
        return Series(new_data, name=self.name, index=indices)
