#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the Graviti Series."""

from itertools import chain, zip_longest
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union, overload

from graviti.dataframe.row.indexing import RowSeriesILocIndexer, RowSeriesLocIndexer
from graviti.utility import MAX_REPR_ROWS


class Series:
    """One-dimensional array.

    Arguments:
        data: The data that needs to be stored in Series. Could be ndarray or Iterable.
        schema: Data type to force. If None, will be inferred from ``data``.
        index: Index of the ``data``.

    Examples:
        Constructing Series from a list.

        >>> d = {"filename": "a.jpg", "attributes": {"color": "red", "pose": "frontal"}}
        >>> series = Series(data=d)
        >>> series
        filename           a.jpg
        attributes color     red
                    pose frontal

    """

    _indices_data: Dict[str, Any]
    _indices: List[str]

    def __init__(
        self,
        data: Optional[Dict[str, Any]] = None,
        schema: Any = None,
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
                value = Series(value)
            self._indices_data[key] = value
            self._indices.append(key)

    def __repr__(self) -> str:
        flatten_header, flatten_data = self._flatten()
        header = self._get_repr_header(flatten_header)
        body = [
            item._repr_folding() if hasattr(item, "_repr_folding") else str(item)
            for item in flatten_data
        ]
        column_widths = [len(max(row, key=len)) for row in chain(header, [body])]
        lines = [
            "".join(f"{item:<{column_widths[index]+2}}" for index, item in enumerate(line))
            for line in zip_longest(*header, body)
        ]
        if self.__len__() > MAX_REPR_ROWS:
            lines.append(f"...({self.__len__()})")
        return "\n".join(lines)

    # @overload
    # def __getitem__(self, key: Union[slice, Tuple[str]]) -> "Series":
    #    ...

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
        return Series(new_data)

    def __setitem__(self, key: str, value: Any) -> None:
        pass

    def __len__(self) -> int:
        return self._indices.__len__()

    @staticmethod
    def _get_repr_header(flatten_header: List[Tuple[str, ...]]) -> List[List[str]]:
        lines: List[List[str]] = []
        for names in zip_longest(*flatten_header, fillvalue=""):
            line = []
            pre_name = None
            upper_line = lines[-1][1:] if lines else []
            for name, upper_name in zip_longest(names, upper_line, fillvalue=""):
                if name == pre_name and upper_name == "":
                    line.append("")
                else:
                    line.append(name)
                pre_name = name
            lines.append(line)
        return lines

    @classmethod
    def _construct(cls, indices_data: Dict[str, Any]) -> "Series":
        obj: Series = object.__new__(cls)
        obj._indices_data = indices_data
        obj._indices = list(indices_data.keys())
        return obj

    def _flatten(self) -> Tuple[List[Tuple[str, ...]], List[Any]]:
        header: List[Tuple[str, ...]] = []
        data: List[Any] = []
        for key, value in self._indices_data.items():
            if isinstance(value, Series):
                nested_header, nested_data = value._flatten()  # pylint: disable=protected-access
                header.extend((key, *sub_column) for sub_column in nested_header)
                data.extend(nested_data)
            else:
                data.append(value)
                header.append((key,))
        return header, data

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
        return self._construct(indices_data)

    @property
    def iloc(self) -> RowSeriesILocIndexer:
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
        return RowSeriesILocIndexer(self)

    @property
    def loc(self) -> RowSeriesLocIndexer:
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
        return RowSeriesLocIndexer(self)
