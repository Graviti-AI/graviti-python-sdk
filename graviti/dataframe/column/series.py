#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the Graviti Series."""


from itertools import islice
from typing import Any, Iterable, List, Optional, Type, TypeVar, Union, overload

import pyarrow as pa

import graviti.portex as pt
from graviti.dataframe.column.indexing import ColumnSeriesILocIndexer, ColumnSeriesLocIndexer
from graviti.dataframe.container import Container
from graviti.paging import LazyFactoryBase, PyArrowPagingList
from graviti.utility import MAX_REPR_ROWS, FileBase

_S = TypeVar("_S", bound="Series")
_A = TypeVar("_A", bound="ArraySeries")


@pt.ContainerRegister(
    pt.binary,
    pt.boolean,
    pt.enum,
    pt.float32,
    pt.float64,
    pt.int32,
    pt.int64,
    pt.string,
)
class Series(Container):
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

    has_keys = False
    schema: pt.PortexType
    _data: PyArrowPagingList
    name: Optional[str]

    def __init__(
        self,
        data: Iterable[Any],
        schema: Optional[pt.PortexType] = None,
        name: Union[str, None] = None,
    ) -> None:
        raise NotImplementedError("Not support initializing Series by __init__")

    # @overload
    # def __getitem__(self, key: slice) -> "Series":
    #    ...

    # @overload
    # def __getitem__(self, key: int) -> Any:
    #     ...

    # @overload
    # def __getitem__(self, key: Iterable[int]) -> "Series":
    #     ...

    def __getitem__(self, key: int) -> Any:
        return self._data[key].as_py()

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
        indice_width = len(str(max(indices, default=1)))

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
        return islice(range(len(self)), MAX_REPR_ROWS)

    # @overload
    # @staticmethod
    # def _get_location_by_index(key: Iterable[int]) -> List[int]:
    #     ...

    # @overload
    # @staticmethod
    # def _get_location_by_index(key: int) -> int:
    #     ...

    # @staticmethod
    # def _get_location_by_index(key: Union[int, Iterable[int]]) -> Union[int, List[int]]:
    #     if isinstance(key, int):
    #         return key

    #     return list(key)

    # @overload
    # def _getitem_by_location(self, key: slice) -> "Series":
    #    ...

    # @overload
    # def _getitem_by_location(self, key: int) -> Any:
    #     ...

    # @overload
    # def _getitem_by_location(self, key: Iterable[int]) -> "Series":
    #     ...

    def _getitem_by_location(self, key: int) -> Any:
        return self.__getitem__(key)

    def _extend(self, values: "Series") -> None:
        """Extend Series to itself row by row.

        Arguments:
            values: A series that needs to be extended.

        Examples:
            >>> s1 = Series.from_pyarrow(pa.array([1,2,3]))
            >>> s1
            0  1
            1  2
            2  3
            >>> s2 = Series.from_pyarrow(pa.array([4,5,6]))
            >>> s2
            0  4
            1  5
            2  6
            >>> s1.extend(s2)
            >>> s1
            0  1
            1  2
            2  3
            3  1
            4  2
            5  3

        """
        self._data.extend(values._data)  # pylint: disable=protected-access

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
    def _from_factory(  # pylint: disable=arguments-differ
        cls: Type[_S], factory: LazyFactoryBase, schema: pt.PortexType, name: Optional[str] = None
    ) -> _S:
        obj: _S = object.__new__(cls)
        obj._data = factory.create_list()
        obj.schema = schema
        obj.name = name
        return obj

    @classmethod
    def _from_pyarrow(  # pylint: disable=arguments-differ
        cls: Type[_S], array: pa.Array, schema: pt.PortexType, name: Optional[str] = None
    ) -> _S:
        obj: _S = object.__new__(cls)
        obj._data = PyArrowPagingList.from_pyarrow(array)
        obj.schema = schema
        obj.name = name
        return obj

    @classmethod
    def from_pyarrow(
        cls: Type[_S],
        array: pa.Array,
        schema: Optional[pt.PortexType] = None,
        name: Optional[str] = None,
    ) -> _S:
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
            schema = pt.PortexType.from_pyarrow(array.type)
        elif not array.type.equals(schema.to_pyarrow()):
            raise TypeError("The schema is mismatched with the pyarrow array")

        return cls._from_pyarrow(array, schema, name)

    def to_pylist(self) -> List[Any]:
        """Convert the Series to a python list.

        Returns:
            The python list representing the Series.

        """
        return self._data.to_pyarrow().to_pylist()  # type: ignore[no-any-return]

    def copy(self: _S) -> _S:
        """Get a copy of the series.

        Returns:
            A copy of the series.

        """
        obj: _S = object.__new__(self.__class__)

        obj.name = self.name
        obj.schema = self.schema.copy()
        obj._data = self._data.copy()  # pylint: disable=protected-access

        return obj


@pt.ContainerRegister(pt.array)
class ArraySeries(Series):  # pylint: disable=abstract-method
    """One-dimensional array for portex builtin type array."""

    _item_container: Type[Container]

    def __getitem__(self, key: int) -> Any:
        scalar = self._data[key]
        if not scalar.is_valid:
            return None

        return self._item_container._from_pyarrow(
            scalar.values, self.schema.to_builtin().items  # type: ignore[attr-defined]
        )

    @classmethod
    def _from_factory(  # pylint: disable=arguments-differ
        cls: Type[_A], factory: LazyFactoryBase, schema: pt.PortexType, name: Optional[str] = None
    ) -> _A:
        obj = super()._from_factory(factory, schema, name)

        builtin_schema: pt.array = schema.to_builtin()  # type: ignore[attr-defined]
        obj._item_container = builtin_schema.items.container  # pylint: disable=protected-access

        return obj

    @classmethod
    def _from_pyarrow(  # pylint: disable=arguments-differ
        cls: Type[_A], array: pa.Array, schema: pt.PortexType, name: Optional[str] = None
    ) -> _A:
        obj = super()._from_pyarrow(array, schema, name)

        builtin_schema: pt.array = schema.to_builtin()  # type: ignore[attr-defined]
        obj._item_container = builtin_schema.items.container  # pylint: disable=protected-access

        return obj

    def copy(self: _A) -> _A:
        """Get a copy of the series.

        Returns:
            A copy of the series.

        """
        obj = super().copy()
        obj._item_container = self._item_container  # pylint: disable=protected-access

        return obj


@pt.ExternalContainerRegister(
    "https://github.com/Project-OpenBytes/standard",
    "main",
    "file.Audio",
    "file.Image",
    "file.PointCloud",
    "file.PointCloudBin",
    "file.RemoteFile",
    "label.file.RemoteInstanceMask",
    "label.file.RemoteSemanticMask",
)
class FileSeries(Series):  # pylint: disable=abstract-method
    """One-dimensional array for file."""

    def __getitem__(self, key: int) -> Optional[FileBase]:
        scalar = self._data[key]
        if not scalar.is_valid:
            return None

        return FileBase.from_pyarrow(scalar)
