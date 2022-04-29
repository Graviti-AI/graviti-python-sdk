#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the Graviti Series."""


from copy import copy
from itertools import islice
from typing import Any, Dict, Iterable, List, Optional, Sequence, Type, TypeVar, Union, overload

import pyarrow as pa

from graviti.dataframe.column.indexing import ColumnSeriesILocIndexer, ColumnSeriesLocIndexer
from graviti.portex import PortexType
from graviti.utility import MAX_REPR_ROWS
from graviti.utility.paging import PagingList

_T = TypeVar("_T", bound="Series")


class Series:
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

    schema: PortexType
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
        self.schema = schema
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

    @property
    def iloc(self) -> ColumnSeriesILocIndexer:
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
        return ColumnSeriesILocIndexer(self)

    @property
    def loc(self) -> ColumnSeriesLocIndexer:
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
        return ColumnSeriesLocIndexer(self)

    @classmethod
    def _from_pyarrow(
        cls: Type[_T], array: pa.Array, schema: PortexType, name: Optional[str] = None
    ) -> _T:
        obj: _T = object.__new__(cls)
        obj._data = PagingList(array)  # type: ignore[assignment]
        obj.schema = schema
        obj.name = name
        obj._indices_data = None
        obj._indices = None
        return obj

    @classmethod
    def from_pyarrow(
        cls: Type[_T],
        array: pa.Array,
        schema: Optional[PortexType] = None,
        name: Optional[str] = None,
    ) -> _T:
        """Instantiate a Series backed by an pyarrow array.

        Arguments:
            array: The input pyarrow array.
            schema: The schema of the series. If None, will be inferred from `array`.
            name: The name to the Series.

        Raises:
            TypeError: When the schema is mismatched with the pyarrow array type.

        Returns:
            The loaded :class:`~graviti.dataframe.column.Series` instance.

        """
        if schema is None:
            portex_type = PortexType.from_pyarrow(array.type)
        else:
            if not array.type.equals(schema.to_pyarrow()):
                raise TypeError("The schema is mismatched with the pyarrow array.")

            portex_type = schema
        return cls._from_pyarrow(array, portex_type, name)

    def copy(self: _T) -> _T:
        """Get a copy of the series.

        Returns:
            A copy of the series.

        """
        obj: _T = object.__new__(self.__class__)

        obj.name = self.name
        obj.schema = self.schema.copy()

        # pylint: disable=protected-access
        obj._data = self._data.copy() if isinstance(self._data, PagingList) else copy(self._data)
        obj._indices_data = copy(self._indices_data)
        obj._indices = copy(self._indices)

        return obj
