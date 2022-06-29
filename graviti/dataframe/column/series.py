#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the Graviti Series."""


from itertools import islice
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Iterable,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    overload,
)

import pyarrow as pa

import graviti.portex as pt
from graviti.dataframe.column.indexing import ColumnSeriesILocIndexer, ColumnSeriesLocIndexer
from graviti.dataframe.container import Container
from graviti.paging import LazyFactoryBase, PagingList, PyArrowPagingList
from graviti.paging.lists import PagingListBase
from graviti.utility import MAX_REPR_ROWS, FileBase

if TYPE_CHECKING:
    from graviti.dataframe.frame import DataFrame

_SB = TypeVar("_SB", bound="SeriesBase")
_S = TypeVar("_S", bound="Series")
_A = TypeVar("_A", bound="ArraySeries")


class SeriesBase(Container):  # pylint: disable=abstract-method
    """One-dimensional array.

    Arguments:
        data: The data that needs to be stored in Series. Could be ndarray or Iterable.
        schema: Data type to force. If None, will be inferred from ``data``.

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
    _data: PagingListBase

    def __new__(
        cls: Type[_SB],
        data: Union[Iterable[Any], _SB],
        schema: Optional[pt.PortexType] = None,
    ) -> Any:
        """One-dimensional data with schema.

        Arguments:
            data: The data that needs to be stored in column series.
            schema: The schema of the column series. If None, will be inferred from `data`.

        Raises:
            ValueError: When both given schema and Series data.
            ValueError: When using DataFrame as data to construct a Series.

        Returns:
            The created Series object.

        """
        if isinstance(data, cls):
            if schema is not None:
                raise ValueError("Only support the schema when data is not Series")

            return data.copy()

        if data.__class__.__name__ == "DataFrame":
            raise ValueError("Do not support Constructing a Series through a DataFrame")

        if schema is None:
            array = pa.array(data)
            schema = pt.PortexType.from_pyarrow(array.type)
        else:
            array = cls._pylist_to_pyarrow(data, schema)  # type: ignore[arg-type]

        return schema.container._from_pyarrow(array, schema)

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
        return self._data[key]

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
        return "\n".join(lines)

    def _repr_folding(self) -> str:
        return f"{self.__class__.__name__}({self.__len__()})"

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

    @staticmethod
    def _pylist_to_pyarrow(values: Iterable[Any], schema: pt.PortexType) -> pa.StructArray:

        processor, need_process = SeriesBase._process_array(values, schema)

        if not need_process:
            return pa.array(values, schema.to_pyarrow())

        return pa.array(processor(values), schema.to_pyarrow())

    @staticmethod
    def _process_array(
        values: Iterable[Any], schema: pt.PortexType
    ) -> Tuple[Callable[[Any], Any], bool]:
        for value in values:
            if value is None:
                continue
            processor, need_process = SeriesBase._get_process(value, schema)
            return lambda x: list(map(processor, x)), need_process

        return lambda x: x, False

    @staticmethod
    def _get_process(value: Any, schema: pt.PortexType) -> Tuple[Callable[[Any], Any], bool]:
        container = schema.container
        if value.__class__.__name__ == "DataFrame":
            return lambda x: x.to_pylist(), True
        if container == ArraySeries:
            return SeriesBase._process_array(value, schema.items)  # type: ignore[attr-defined]
        if container == FileSeries:
            return SeriesBase._process_file(value)

        return lambda x: x, False

    @staticmethod
    def _process_file(values: Iterable[Any]) -> Tuple[Callable[[Any], Any], bool]:
        if isinstance(values, FileBase):
            return lambda x: x.to_pyobj(), True

        return lambda x: x, False

    def _extend(self: _SB, values: _SB) -> None:
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

    def _copy(self: _SB, schema: pt.PortexType) -> _SB:
        obj: _SB = object.__new__(self.__class__)

        obj.schema = schema
        obj._data = self._data.copy()  # pylint: disable=protected-access

        return obj

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
    def from_pyarrow(
        cls: Type[_SB],
        array: pa.Array,
        schema: Optional[pt.PortexType] = None,
    ) -> _SB:
        """Instantiate a Series backed by an pyarrow array.

        Arguments:
            array: The input pyarrow array.
            schema: The schema of the series. If None, will be inferred from `array`.

        Raises:
            TypeError: When the schema is mismatched with the pyarrow array type.

        Returns:
            The loaded :class:`~graviti.dataframe.column.Series` instance.

        """
        if schema is None:
            schema = pt.PortexType.from_pyarrow(array.type)
        elif not array.type.equals(schema.to_pyarrow()):
            raise TypeError("The schema is mismatched with the pyarrow array")

        return cls._from_pyarrow(array, schema)


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
class Series(SeriesBase):  # pylint: disable=abstract-method
    """One-dimensional array.

    Arguments:
        data: The data that needs to be stored in Series. Could be ndarray or Iterable.
        schema: Data type to force. If None, will be inferred from ``data``.

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

    _data: PyArrowPagingList

    def __getitem__(self, key: int) -> Any:
        return self._data[key].as_py()

    @classmethod
    def _from_factory(
        cls: Type[_S],
        factory: LazyFactoryBase,
        schema: pt.PortexType,
        parent: Optional["DataFrame"] = None,
    ) -> _S:
        obj: _S = object.__new__(cls)
        obj._data = factory.create_pyarrow_list()
        obj.schema = schema
        obj._parent = parent
        return obj

    @classmethod
    def _from_pyarrow(
        cls: Type[_S], array: pa.Array, schema: pt.PortexType, parent: Optional["DataFrame"] = None
    ) -> _S:
        obj: _S = object.__new__(cls)
        obj._data = PyArrowPagingList.from_pyarrow(array)
        obj.schema = schema
        obj._parent = parent
        return obj

    def to_pylist(self) -> List[Any]:
        """Convert the Series to a python list.

        Returns:
            The python list representing the Series.

        """
        return self._data.to_pyarrow().to_pylist()  # type: ignore[no-any-return]


@pt.ContainerRegister(pt.array)
class ArraySeries(SeriesBase):  # pylint: disable=abstract-method
    """One-dimensional array for portex builtin type array."""

    _data: PagingList

    def __getitem__(self, key: int) -> Any:
        return self._data[key]

    @classmethod
    def _from_factory(
        cls: Type[_A],
        factory: LazyFactoryBase,
        schema: pt.PortexType,
        parent: Optional["DataFrame"] = None,
    ) -> _A:
        builtin_schema: pt.array = schema.to_builtin()  # type: ignore[attr-defined]
        _item_schema = builtin_schema.items
        _item_creator = _item_schema.container._from_pyarrow  # pylint: disable=protected-access

        obj: _A = object.__new__(cls)
        obj._data = factory.create_list(
            lambda array: tuple(_item_creator(item.values, _item_schema) for item in array)
        )
        obj.schema = schema
        obj._parent = parent

        return obj

    @classmethod
    def _from_pyarrow(
        cls: Type[_A], array: pa.Array, schema: pt.PortexType, parent: Optional["DataFrame"] = None
    ) -> _A:
        builtin_schema: pt.array = schema.to_builtin()  # type: ignore[attr-defined]
        _item_schema = builtin_schema.items
        _item_creator = _item_schema.container._from_pyarrow  # pylint: disable=protected-access

        obj: _A = object.__new__(cls)
        obj._data = PagingList(_item_creator(item.values, _item_schema) for item in array)
        obj.schema = schema
        obj._parent = parent
        return obj

    # TODO: copy method will be implementated sooner.
    # def copy(self: _A) -> _A:
    #     """Get a copy of the series.

    #     Returns:
    #         A copy of the series.

    #     """
    #     obj = super().copy()

    #     return obj

    def to_pylist(self) -> List[Any]:
        """Convert the Series to a python list.

        Returns:
            The python list representing the Series.

        """
        return [item.to_pylist() for item in self._data]


@pt.ExternalContainerRegister(
    "https://github.com/Project-OpenBytes/portex-standard",
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
