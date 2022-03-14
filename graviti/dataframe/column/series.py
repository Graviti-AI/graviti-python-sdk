#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the Graviti Series."""


from itertools import islice
from textwrap import indent
from typing import Any, Dict, Iterable, List, Optional, Sequence, Union, overload

from graviti.dataframe.series import SeriesBase

_INDENT = " "
_MAX_REPR_ROWS = 10


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
        schema: int

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

    def _get_repr_indices(self) -> Iterable[int]:
        length = self.__len__()
        # pylint: disable=protected-access
        if self._indices is None:
            return range(min(length, _MAX_REPR_ROWS))

        if length >= _MAX_REPR_ROWS:
            return islice(self._indices, _MAX_REPR_ROWS)

        return self._indices

    def __repr__(self) -> str:

        length = self.__len__()
        lines = []
        repr_indices = list(self._get_repr_indices())
        repr_indices = map(str, repr_indices)
        indice_width = len(max(repr_indices))

        repr_body = self._get_repr_body()

        if self.name:
            column_width = max(len(max(repr_indices)), len(self.name))
            lines.append(f"{_INDENT:<{indice_width+2}}{value:<{column_width+2}}")
        for indice, value in zip(repr_indices, repr_body):
            lines.append(
                f"{indice:<{indice_width+2}}{value:<{column_width+2}}"
                # pylint: disable=protected-access
            )
        if length > _MAX_REPR_ROWS:
            lines.append(f"...({self.__len__()})")
        return "\n".join(lines)

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
