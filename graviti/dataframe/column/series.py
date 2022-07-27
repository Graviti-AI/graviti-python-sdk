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
from graviti.operation import UpdateData
from graviti.paging import LazyFactoryBase, MappedPagingList, PagingListBase, PyArrowPagingList
from graviti.utility import MAX_REPR_ROWS, FileBase

if TYPE_CHECKING:
    from graviti.dataframe.frame import DataFrame
    from graviti.manager import ObjectPolicyManager

_SB = TypeVar("_SB", bound="SeriesBase")
_S = TypeVar("_S", bound="Series")
_A = TypeVar("_A", bound="ArraySeries")
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
            series = self._from_pyarrow(self._pylist_to_pyarrow([value], self.schema), self.schema)
            key = slice(key, key + 1)
        elif not isinstance(value, self.__class__):
            series = self._from_pyarrow(
                self._pylist_to_pyarrow(value, self.schema), self.schema  # type: ignore[arg-type]
            )
        elif self.schema.to_pyarrow().equals(value.schema.to_pyarrow()):
            series = value
        else:
            raise TypeError("The schema of the given Series is mismatched")

        self._set_slice(key, series)
        if self._root is not None and self._root.operations is not None:
            value = series.copy()
            root = self._root
            _record_key = root._record_key
            # TODO: support slicing methods for record_key
            record_key = [
                _record_key[i]  # type: ignore[index]
                for i in range(*key.indices(len(_record_key)))  # type: ignore[arg-type]
            ]
            name = self._name
            dataframe = root._construct(
                {name[0]: series},
                pt.record({name[0]: series.schema}),
                Series(record_key),
                name[1:],
            )
            root.operations.append(UpdateData(dataframe))  # type: ignore[union-attr]

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

    def _del_item_by_location(self, key: Union[int, slice]) -> None:
        self._data.__delitem__(key)

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
            return lambda x: x._to_post_data(), True  # pylint: disable=protected-access
        if container == ArraySeries:
            return SeriesBase._process_array(
                value, schema.to_builtin().items  # type: ignore[attr-defined]
            )
        if container == FileSeries:
            return SeriesBase._process_file(value)
        if container == EnumSeries:
            values_to_indices: Dict[Union[int, float, str, bool, None], Optional[int]] = {
                k: v for v, k in enumerate(schema.to_builtin().values)  # type: ignore[attr-defined]
            }
            if None not in values_to_indices:
                values_to_indices[None] = None
            return lambda x: values_to_indices[x], True

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

    _data: PyArrowPagingList

    def __getitem__(self, key: int) -> Any:
        return self._data[key].as_py()

    @classmethod
    def _from_factory(  # pylint: disable=too-many-arguments
        cls: Type[_S],
        factory: LazyFactoryBase,
        schema: pt.PortexType,
        root: Optional["DataFrame"] = None,
        name: Tuple[str, ...] = (),
        object_policy_manager: Optional["ObjectPolicyManager"] = None,
    ) -> _S:
        obj: _S = object.__new__(cls)
        obj._data = factory.create_pyarrow_list()
        obj.schema = schema
        obj._root = root
        obj._name = name
        return obj

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

    def to_pylist(self) -> List[Any]:
        """Convert the Series to a python list.

        Returns:
            The python list representing the Series.

        """
        return self._data.to_pyarrow().to_pylist()  # type: ignore[no-any-return]


@pt.ContainerRegister(pt.array)
class ArraySeries(SeriesBase):  # pylint: disable=abstract-method
    """One-dimensional array for portex builtin type array."""

    _data: MappedPagingList
    _item_schema: pt.PortexType

    def __getitem__(self, key: int) -> Any:
        return self._data[key]

    @classmethod
    def _from_factory(  # pylint: disable=too-many-arguments
        cls: Type[_A],
        factory: LazyFactoryBase,
        schema: pt.PortexType,
        root: Optional["DataFrame"] = None,
        name: Tuple[str, ...] = (),
        object_policy_manager: Optional["ObjectPolicyManager"] = None,
    ) -> _A:
        builtin_schema: pt.array = schema.to_builtin()  # type: ignore[attr-defined]
        _item_schema = builtin_schema.items
        _item_creator = _item_schema.container._from_pyarrow  # pylint: disable=protected-access

        obj: _A = object.__new__(cls)
        obj._data = factory.create_mapped_list(
            lambda scalar: _item_creator(scalar.values, _item_schema)
        )
        obj.schema = schema
        obj._root = root
        obj._item_schema = _item_schema  # pylint: disable=protected-access
        obj._name = name

        return obj

    @classmethod
    def _from_pyarrow(
        cls: Type[_A],
        array: pa.Array,
        schema: pt.PortexType,
        root: Optional["DataFrame"] = None,
        name: Tuple[str, ...] = (),
    ) -> _A:
        builtin_schema: pt.array = schema.to_builtin()  # type: ignore[attr-defined]
        _item_schema = builtin_schema.items
        _item_creator = _item_schema.container._from_pyarrow  # pylint: disable=protected-access

        obj: _A = object.__new__(cls)
        obj._data = MappedPagingList.from_array(
            array, lambda scalar: _item_creator(scalar.values, _item_schema)
        )

        obj.schema = schema
        obj._root = root
        obj._item_schema = _item_schema  # pylint: disable=protected-access
        obj._name = name

        return obj

    def _extract_paging_list(self: _A, values: _A) -> MappedPagingList:
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

        builtin_schema: pt.array = schema.to_builtin()  # type: ignore[attr-defined]
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


@pt.ContainerRegister(pt.enum)
class EnumSeries(Series):
    """One-dimensional array for portex builtin type enum."""

    _indices_to_values: Dict[Optional[pa.Scalar], Union[int, float, str, bool, None]]

    def __getitem__(self, key: int) -> Any:
        return self._indices_to_values[self._data[key]]

    def _to_post_data(self) -> List[int]:
        return self._data.to_pyarrow().to_pylist()  # type: ignore[no-any-return]

    def _copy(
        self,
        schema: pt.PortexType,
        root: Optional["DataFrame"] = None,
        name: Tuple[str, ...] = (),
    ) -> "EnumSeries":
        obj = super()._copy(schema, root, name)
        obj._indices_to_values = self._indices_to_values.copy()  # pylint: disable=protected-access
        return obj

    @classmethod
    def _from_factory(  # pylint: disable=too-many-arguments
        cls: Type[_E],
        factory: LazyFactoryBase,
        schema: pt.PortexType,
        root: Optional["DataFrame"] = None,
        name: Tuple[str, ...] = (),
        object_policy_manager: Optional["ObjectPolicyManager"] = None,
    ) -> _E:
        obj: _E = object.__new__(cls)
        obj._data = factory.create_pyarrow_list()
        obj.schema = schema
        obj._root = root
        obj._name = name
        obj._indices_to_values = cls._create_indices_to_values(
            schema.to_builtin().values,  # type: ignore[attr-defined]
            obj._data._patype,  # pylint: disable=protected-access
        )
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
        obj._indices_to_values = cls._create_indices_to_values(
            schema.to_builtin().values,  # type: ignore[attr-defined]
            obj._data._patype,  # pylint: disable=protected-access
        )
        return obj

    @staticmethod
    def _create_indices_to_values(
        enum_values: List[Union[int, float, str, bool, None]], patype: pa.DataType
    ) -> Dict[Optional[pa.Scalar], Union[int, float, str, bool, None]]:
        indices_to_values = dict(
            zip(
                pa.array(range(len(enum_values)), patype),  # pylint: disable=protected-access
                enum_values,
            )
        )
        if None not in indices_to_values:
            indices_to_values[pa.scalar(None, patype)] = None
        return indices_to_values

    def to_pylist(self) -> List[Any]:
        """Convert the Series to a python list.

        Returns:
            The python list representing the Series.

        """
        return [self._indices_to_values[i] for i in self._data]
