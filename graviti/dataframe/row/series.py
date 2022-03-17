#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the Graviti Series."""

from itertools import chain, zip_longest
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union, overload

from graviti.dataframe.series import SeriesBase
from graviti.utility.repr import _MAX_REPR_ROWS


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
        if self.__len__() > _MAX_REPR_ROWS:
            lines.append(f"...({self.__len__()})")
        if self.name:
            lines.append(f"Name: {self.name}")
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
        return Series(new_data, name=self.name)

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
    def _construct(cls, indices_data: Dict[str, Any], new_name: Union[str, int, None]) -> "Series":
        obj: Series = object.__new__(cls)
        # pylint: disable=protected-access
        obj._indices_data = indices_data
        obj._indices = list(indices_data.keys())
        obj.name = new_name
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
        return self._construct(indices_data, self.name)
