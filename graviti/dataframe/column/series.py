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
    Dict,
    Iterable,
    Iterator,
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
from graviti.file import FileBase
from graviti.operation import UpdateData
from graviti.paging import (
    LazyFactoryBase,
    MappedPagingList,
    PagingList,
    PagingListBase,
    PyArrowPagingList,
)
from graviti.portex.enum import EnumValueType
from graviti.utility import MAX_REPR_ROWS

if TYPE_CHECKING:
    from graviti.dataframe.frame import DataFrame

_SB = TypeVar("_SB", bound="SeriesBase")
_S = TypeVar("_S", bound="Series")
_A = TypeVar("_A", bound="ArraySeries")
_F = TypeVar("_F", bound="FileSeries")
_E = TypeVar("_E", bound="EnumSeries")


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

    schema: pt.PortexType
    _data: PagingListBase[Any]

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
        elif issubclass(schema.container, FileSeries):
            return FileSeries._from_iterable(data, schema)
        else:
            array = cls._pylist_to_pyarrow(data, schema)

        return schema.container._from_pyarrow(array, schema)

    @overload
    def __getitem__(self: _SB, key: slice) -> _SB:
        ...

    @overload
    def __getitem__(self, key: int) -> Any:
        ...

    # @overload
    # def __getitem__(self, key: Iterable[int]) -> "Series":
    #     ...

    def __getitem__(self: _SB, key: Union[int, slice]) -> Union[Any, _SB]:
        if isinstance(key, int):
            return self._get_item_by_location(key)

        return self._get_slice_by_location(key, self.schema.copy())

    def __delitem__(self, key: Union[int, slice]) -> None:
        if self._root is not None:
            raise TypeError(
                "'__delitem__' is not supported for the Series which is a member of a DataFrame"
            )

        self._del_item_by_location(key)

    @overload
    def __setitem__(self, key: slice, value: Union[Iterable[Any], _SB]) -> None:
        ...

    @overload
    def __setitem__(self, key: int, value: Any) -> None:
        ...

    def __setitem__(
        self,
        key: Union[int, slice],
        value: Union[Iterable[Any], _SB, Any],
    ) -> None:
        if isinstance(key, int):
            series = self._from_iterable([value], self.schema)
            key = slice(key, key + 1)
        elif not isinstance(value, self.__class__):
            series = self._from_pyarrow(self._pylist_to_pyarrow(value, self.schema), self.schema)
        elif self.schema.to_pyarrow().equals(value.schema.to_pyarrow()):
            series = value
        else:
            raise TypeError("The schema of the given Series is mismatched")

        self._set_slice(key, series)
        if self._root is not None and self._root.operations is not None:
            value = series.copy()
            root = self._root
            name = self._name
            df = root._construct(
                {name[0]: series},
                pt.record({name[0]: series.schema}),
                root._record_key[key],  # type: ignore[index]
                name[1:],
            )
            root.operations.append(UpdateData(df))  # type: ignore[union-attr]

    def __len__(self) -> int:
        return self._data.__len__()

    def __iter__(self) -> Iterator[Any]:
        return self._data.__iter__()

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

    def _get_item_by_location(self, key: int) -> Any:
        return self._data.get_item(key)

    def _get_slice_by_location(
        self: _SB,
        key: slice,
        schema: pt.PortexType,
        root: Optional["DataFrame"] = None,
        name: Tuple[str, ...] = (),
    ) -> _SB:
        obj: _SB = object.__new__(self.__class__)

        obj.schema = schema
        # pylint: disable=protected-access
        obj._root = root
        obj._name = name
        obj._data = self._data.get_slice(key)

        return obj

    def _del_item_by_location(self, key: Union[int, slice]) -> None:
        self._data.__delitem__(key)

    @classmethod
    def _from_factory(
        cls: Type[_SB],
        factory: LazyFactoryBase,
        schema: pt.PortexType,
        root: Optional["DataFrame"] = None,
        name: Tuple[str, ...] = (),
    ) -> _SB:
        obj: _SB = object.__new__(cls)
        obj.schema = schema
        obj._refresh_data_from_factory(factory)
        obj._root = root
        obj._name = name
        return obj

    def _refresh_data_from_factory(self, factory: LazyFactoryBase) -> None:
        self._data = factory.create_pyarrow_list()

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

    @classmethod
    def _from_iterable(
        cls: Type[_SB],
        array: Iterable[Any],
        schema: pt.PortexType,
    ) -> _SB:
        return cls(array, schema)

    @staticmethod
    def _pylist_to_pyarrow(values: Iterable[Any], schema: pt.PortexType) -> pa.StructArray:
        if isinstance(values, Iterator):
            values = list(values)
        processor, need_process = SeriesBase._process_array(values, schema)

        if not need_process:
            return pa.array(values, schema.to_pyarrow())

        return pa.array(processor(values), schema.to_pyarrow())

    @staticmethod
    def _process_array(
        values: Iterable[Any], schema: pt.PortexType
    ) -> Tuple[Callable[[Any], Any], Optional[bool]]:
        if not values:
            return lambda x: x, None

        for value in values:
            if value is None:
                continue

            processor, need_process = SeriesBase._get_process(value, schema)
            if need_process is None:
                continue

            return lambda x: list(map(processor, x)), need_process

        return lambda x: x, False

    @staticmethod
    def _get_process(
        value: Any, schema: pt.PortexType
    ) -> Tuple[Callable[[Any], Any], Optional[bool]]:
        container = schema.container
        if value.__class__.__name__ == "DataFrame":
            return lambda x: x._to_post_data(), True  # pylint: disable=protected-access

        if container == ArraySeries:
            return SeriesBase._process_array(
                value, schema.to_builtin().items  # type: ignore[attr-defined]
            )

        if container == FileSeries:
            return SeriesBase._process_file(value)

        if container == EnumSeries:
            value_to_index = schema.to_builtin().values.value_to_index  # type: ignore[attr-defined]
            return lambda x: value_to_index[x], True

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

    def _copy(
        self: _SB,
        schema: pt.PortexType,
        root: Optional["DataFrame"] = None,
        name: Tuple[str, ...] = (),
    ) -> _SB:
        obj: _SB = object.__new__(self.__class__)

        obj.schema = schema
        # pylint: disable=protected-access
        obj._root = root
        obj._data = self._data.copy()
        obj._name = name

        return obj

    def _set_slice(self, key: slice, value: _SB) -> None:
        self._data.set_slice(key, value._data)  # pylint: disable=protected-access

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

    def _to_post_data(self) -> List[Any]:
        return self.to_pylist()


@pt.ContainerRegister(
    pt.binary,
    pt.boolean,
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

    _data: PyArrowPagingList[Any]

    @classmethod
    def _from_pyarrow(
        cls: Type[_S],
        array: pa.Array,
        schema: pt.PortexType,
        root: Optional["DataFrame"] = None,
        name: Tuple[str, ...] = (),
    ) -> _S:
        obj: _S = object.__new__(cls)
        obj._data = PyArrowPagingList.from_pyarrow(array)
        obj.schema = schema
        obj._root = root
        obj._name = name
        return obj

    def _get_item_by_location(self, key: int) -> Any:
        return self._data.get_item(key).as_py()

    def to_pylist(self) -> List[Any]:
        """Convert the Series to a python list.

        Returns:
            The python list representing the Series.

        """
        return self._data.to_pyarrow().to_pylist()  # type: ignore[no-any-return]


@pt.ContainerRegister(pt.array)
class ArraySeries(SeriesBase):  # pylint: disable=abstract-method
    """One-dimensional array for portex builtin type array."""

    _data: MappedPagingList[Any]
    _item_schema: pt.PortexType

    @classmethod
    def _from_pyarrow(
        cls: Type[_A],
        array: pa.Array,
        schema: pt.PortexType,
        root: Optional["DataFrame"] = None,
        name: Tuple[str, ...] = (),
    ) -> _A:
        builtin_schema: pt.array = schema.to_builtin()  # type: ignore[assignment]
        _item_schema = builtin_schema.items
        _item_creator = _item_schema.container._from_pyarrow  # pylint: disable=protected-access

        obj: _A = object.__new__(cls)
        obj._data = MappedPagingList.from_array(
            array, lambda scalar: _item_creator(scalar.values, _item_schema, root)
        )

        obj.schema = schema
        obj._root = root
        obj._item_schema = _item_schema  # pylint: disable=protected-access
        obj._name = name

        return obj

    @classmethod
    def _from_iterable(
        cls: Type[_A],
        array: Iterable[Any],
        schema: pt.PortexType,
    ) -> _A:
        obj: _A = object.__new__(cls)
        items_schema = schema.to_builtin().items  # type: ignore[attr-defined]
        items_container = items_schema.container
        array = (
            items_container._from_iterable(value, items_schema)  # pylint: disable=protected-access
            for value in array
        )
        obj._data = MappedPagingList(array)
        obj.schema = schema
        obj._item_schema = items_schema
        return obj

    def _get_slice_by_location(
        self: _A,
        key: slice,
        schema: pt.PortexType,
        root: Optional["DataFrame"] = None,
        name: Tuple[str, ...] = (),
    ) -> _A:
        obj = super()._get_slice_by_location(key, schema, root, name)
        obj._item_schema = self._item_schema  # pylint: disable=protected-access

        return obj

    def _refresh_data_from_factory(self, factory: LazyFactoryBase) -> None:
        builtin_schema: pt.array = self.schema.to_builtin()  # type: ignore[assignment]
        _item_schema = builtin_schema.items
        _item_creator = _item_schema.container._from_pyarrow  # pylint: disable=protected-access

        self._data = factory.create_mapped_list(
            lambda scalar: _item_creator(scalar.values, _item_schema, self._root)
        )
        self._item_schema = _item_schema  # pylint: disable=protected-access

    def _extract_paging_list(self: _A, values: _A) -> MappedPagingList[Any]:
        # pylint: disable=protected-access
        _item_schema = self._item_schema
        if values._item_schema is _item_schema and values is not self:
            return values._data

        _item_creator = _item_schema.container._from_pyarrow
        return values._data.copy(
            lambda df: df._copy(_item_schema),
            lambda scalar: _item_creator(scalar.values, _item_schema),
        )

    def _set_item_by_slice(self: _A, key: slice, value: _A) -> None:
        self._data.set_slice(key, self._extract_paging_list(value))

    def _extend(self: _A, values: _A) -> None:
        self._data.extend(self._extract_paging_list(values))

    def _copy(
        self: _A,
        schema: pt.PortexType,
        root: Optional["DataFrame"] = None,
        name: Tuple[str, ...] = (),
    ) -> _A:
        obj: _A = object.__new__(self.__class__)

        builtin_schema: pt.array = schema.to_builtin()  # type: ignore[assignment]
        _item_schema = builtin_schema.items
        _item_creator = _item_schema.container._from_pyarrow  # pylint: disable=protected-access

        obj.schema = schema
        # pylint: disable=protected-access
        obj._item_schema = _item_schema
        obj._root = root
        obj._data = self._data.copy(
            lambda df: df._copy(_item_schema),
            lambda scalar: _item_creator(scalar.values, _item_schema),
        )
        obj._name = name

        return obj

    def to_pylist(self) -> List[Any]:
        """Convert the Series to a python list.

        Returns:
            The python list representing the Series.

        """
        return [item.to_pylist() for item in self._data]

    def _to_post_data(self) -> List[Any]:
        return [item._to_post_data() for item in self._data]  # pylint: disable=protected-access


@pt.ExternalContainerRegister(
    pt.STANDARD_URL,
    "main",
    "file.File",
    "file.Audio",
    "file.Image",
    "file.PointCloud",
    "file.PointCloudBin",
    "label.Mask",
)
class FileSeries(SeriesBase):  # pylint: disable=abstract-method
    """One-dimensional array for file."""

    _data: PagingList[Any]

    @classmethod
    def _from_iterable(
        cls: Type[_F],
        array: Iterable[FileBase],
        schema: pt.PortexType,
    ) -> _F:
        obj: _F = object.__new__(cls)
        obj._data = PagingList(array)
        obj.schema = schema
        return obj

    @classmethod
    def _from_pyarrow(
        cls: Type[_F],
        array: pa.Array,
        schema: pt.PortexType,
        root: Optional["DataFrame"] = None,
        name: Tuple[str, ...] = (),
    ) -> _F:
        if root is None:
            raise ValueError(
                "The object policy manager from root is needed to create FileSeries from pyarrow"
            )

        obj: _F = object.__new__(cls)
        file_type: FileBase = schema.element  # type: ignore[assignment]
        # pylint: disable=protected-access
        obj._data = PagingList(
            file_type._from_pyarrow(item, root._object_policy_manager) for item in array
        )
        obj.schema = schema
        obj._root = root
        obj._name = name
        return obj

    def _to_post_data(self) -> List[Dict[str, Any]]:
        return [file._to_post_data() for file in self._data]  # pylint: disable=protected-access

    def _refresh_data_from_factory(self, factory: LazyFactoryBase) -> None:
        file_type = self.schema.element
        self._data = factory.create_list(
            lambda scalar: file_type(
                **scalar.as_py(), object_policy_manager=factory.object_policy_manager
            )
        )

    def to_pylist(self) -> List[FileBase]:
        """Convert the BinaryFileSeries to python list.

        Returns:
            The python list.

        """
        return list(self._data)


@pt.ContainerRegister(pt.enum)
class EnumSeries(Series):
    """One-dimensional array for portex builtin type enum."""

    _index_to_value: Dict[Optional[int], EnumValueType]

    def _get_item_by_location(self, key: int) -> Any:
        return self._index_to_value[self._data[key].as_py()]

    def _get_slice_by_location(
        self: _E,
        key: slice,
        schema: pt.PortexType,
        root: Optional["DataFrame"] = None,
        name: Tuple[str, ...] = (),
    ) -> _E:
        obj = super()._get_slice_by_location(key, schema, root, name)
        enum_values = self.schema.to_builtin().values  # type: ignore[attr-defined]
        obj._index_to_value = enum_values.index_to_value  # pylint: disable=protected-access

        return obj

    def _to_post_data(self) -> List[int]:
        return self._data.to_pyarrow().to_pylist()  # type: ignore[no-any-return]

    def _copy(
        self,
        schema: pt.PortexType,
        root: Optional["DataFrame"] = None,
        name: Tuple[str, ...] = (),
    ) -> "EnumSeries":
        obj = super()._copy(schema, root, name)
        enum_values = self.schema.to_builtin().values  # type: ignore[attr-defined]
        obj._index_to_value = enum_values.index_to_value  # pylint: disable=protected-access

        return obj

    @classmethod
    def _from_pyarrow(
        cls: Type[_E],
        array: pa.Array,
        schema: pt.PortexType,
        root: Optional["DataFrame"] = None,
        name: Tuple[str, ...] = (),
    ) -> _E:
        obj: _E = object.__new__(cls)
        obj._data = PyArrowPagingList.from_pyarrow(array)
        obj.schema = schema
        obj._root = root
        obj._name = name
        enum_values = schema.to_builtin().values  # type: ignore[attr-defined]
        obj._index_to_value = enum_values.index_to_value

        return obj

    def _refresh_data_from_factory(self, factory: LazyFactoryBase) -> None:
        self._data = factory.create_pyarrow_list()
        enum_values = self.schema.to_builtin().values  # type: ignore[attr-defined]
        self._index_to_value = enum_values.index_to_value

    def to_pylist(self) -> List[Any]:
        """Convert the Series to a python list.

        Returns:
            The python list representing the Series.

        """
        _index_to_value = self._index_to_value
        return [_index_to_value[i.as_py()] for i in self._data]
