#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the Graviti DataFrame."""


from itertools import chain, islice, zip_longest
from typing import (
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
from graviti.dataframe.column.series import ArraySeries, FileSeries
from graviti.dataframe.column.series import Series as ColumnSeries
from graviti.dataframe.column.series import SeriesBase as ColumnSeriesBase
from graviti.dataframe.container import Container
from graviti.dataframe.indexing import DataFrameILocIndexer, DataFrameLocIndexer
from graviti.dataframe.row.series import Series as RowSeries
from graviti.dataframe.sql import RowSeries as SqlRowSeries
from graviti.operation import AddData, DataFrameOperation, UpdateData, UpdateSchema
from graviti.paging import LazyFactoryBase
from graviti.utility import MAX_REPR_ROWS, FileBase, Mode, engine

_T = TypeVar("_T", bound="DataFrame")
_C = TypeVar("_C", bound="Container")


RECORD_KEY = "__record_key"


@pt.ContainerRegister(pt.record)
class DataFrame(Container):
    """Two-dimensional, size-mutable, potentially heterogeneous tabular data.

    Arguments:
        data: The data that needs to be stored in DataFrame.
        schema: The schema of the DataFrame. If None, will be inferred from `data`.
        columns: Column labels to use for resulting frame when data does not have them,
            defaulting to RangeIndex(0, 1, 2, ..., n). If data contains column labels,
            will perform column selection instead.

    Examples:
        Constructing DataFrame from list.

        >>> df = DataFrame(
        ...     [
        ...         {"filename": "a.jpg", "box2ds": {"x": 1, "y": 1}},
        ...         {"filename": "b.jpg", "box2ds": {"x": 2, "y": 2}},
        ...         {"filename": "c.jpg", "box2ds": {"x": 3, "y": 3}},
        ...     ]
        ... )
        >>> df
            filename box2ds
                     x      y
        0   a.jpg    1      1
        1   b.jpg    2      2
        2   c.jpg    3      3

    """

    has_keys = True
    _columns: Dict[str, Container]
    _column_names: List[str]
    _record_key: Optional[ColumnSeries] = None
    schema: pt.PortexRecordBase
    operations: Optional[List[DataFrameOperation]] = None
    searcher: Optional[Callable[[Dict[str, Any]], "DataFrame"]] = None
    criteria: Optional[Dict[str, Any]] = None

    def __new__(
        cls: Type[_T],
        data: Union[List[Dict[str, Any]], _T, None] = None,
        schema: Optional[pt.PortexRecordBase] = None,
    ) -> _T:
        """Two-dimensional, size-mutable, potentially heterogeneous tabular data.

        Arguments:
            data: The data that needs to be stored in DataFrame.
            schema: The schema of the DataFrame. If None, will be inferred from `data`.

        Raises:
            ValueError: When both given schema and DataFrame data.
            ValueError: When the given data is not list.

        Returns:
            The created :class:`~graviti.dataframe.DataFrame` object.

        """
        if data is None:
            data = []

        if isinstance(data, cls):
            if schema is not None:
                raise ValueError("Only support the schema when data is not DataFrame")

            return data.copy()

        if schema is None:
            return cls.from_pyarrow(pa.array(data))

        if isinstance(data, list):
            return cls._from_pyarrow(cls._pylist_to_pyarrow(data, schema), schema)

        raise ValueError("DataFrame only supports creating from list object now")

    @overload
    def __getitem__(self, key: str) -> Union[ColumnSeriesBase, "DataFrame"]:  # type: ignore[misc]
        # https://github.com/python/mypy/issues/5090
        ...

    @overload
    def __getitem__(self, key: Iterable[str]) -> "DataFrame":
        ...

    def __getitem__(self, key: Union[str, Iterable[str]]) -> Container:
        if isinstance(key, str):
            return self._columns[key]

        if isinstance(key, Iterable):
            new_columns = {name: self._columns[name] for name in key}
            schema = pt.record({name: column.schema for name, column in new_columns.items()})
            return self._construct(new_columns, schema, self._record_key)

        raise KeyError(key)

    def __setitem__(self, key: str, value: Union[Iterable[Any], _C]) -> None:
        if self.operations is None:
            self._setitem(key, value)
            return

        if key in self._column_names:
            raise NotImplementedError("Replacing a column in a sheet is not supported yet")

        self._setitem(key, value)
        dataframe = self.copy()
        dataframe._record_key = self._record_key
        self.operations.extend((UpdateSchema(self.schema), UpdateData(dataframe)))

    def __repr__(self) -> str:
        flatten_header, flatten_data = self._flatten()
        header = self._get_repr_header(flatten_header)
        body = self._get_repr_body(flatten_data)
        column_widths = [len(max(column, key=len)) for column in zip_longest(*header, *body)]
        lines = [
            "".join(f"{item:<{column_widths[index]+2}}" for index, item in enumerate(line))
            for line in chain(header, body)
        ]
        if self.__len__() > MAX_REPR_ROWS:
            lines.append(f"...({self.__len__()})")
        return "\n".join(lines)

    def __len__(self) -> int:
        return self._columns[self._column_names[0]].__len__()

    @staticmethod
    def _get_repr_header(flatten_header: List[Tuple[str, ...]]) -> List[List[str]]:
        lines: List[List[str]] = []
        for names in zip_longest(*flatten_header, fillvalue=""):
            line = [""]
            pre_name = None
            upper_line = lines[-1][1:] if lines else [""]
            for name, upper_name in zip_longest(names, upper_line, fillvalue=""):
                line.append("" if name == pre_name and upper_name == "" else name)
                pre_name = name
            lines.append(line)
        return lines

    @classmethod
    def _construct(
        cls,
        columns: Dict[str, Container],
        schema: pt.record,
        record_key: Optional[ColumnSeries],
    ) -> "DataFrame":
        obj: DataFrame = object.__new__(cls)
        obj._columns = columns
        obj._column_names = list(obj._columns.keys())
        obj.schema = schema
        obj._record_key = record_key
        return obj

    @classmethod
    def _from_pyarrow(  # type: ignore[override]
        cls: Type[_T], array: pa.StructArray, schema: pt.PortexRecordBase
    ) -> _T:
        obj: _T = object.__new__(cls)
        obj.schema = schema
        obj._columns = {}
        obj._column_names = []

        # In this case schema.to_builtin always returns record.
        for key, value in schema.to_builtin().fields.items():  # type: ignore[attr-defined]
            obj._columns[key] = value.container._from_pyarrow(  # pylint: disable=protected-access
                array.field(key), schema=value
            )

            obj._column_names.append(key)

        return obj

    @classmethod
    def _from_factory(  # type: ignore[override]
        cls: Type[_T], factory: LazyFactoryBase, schema: pt.PortexRecordBase
    ) -> _T:
        """Create DataFrame with paging lists.

        Arguments:
            factory: The LazyFactory instance for creating the PagingList.
            schema: The schema of the DataFrame.

        Returns:
            The loaded :class:`~graviti.dataframe.DataFrame` object.

        """
        obj: _T = object.__new__(cls)
        obj.schema = schema
        obj._columns = {}
        obj._column_names = []

        # In this case schema.to_builtin always returns record.
        for key, value in schema.to_builtin().fields.items():  # type: ignore[attr-defined]
            obj._columns[key] = value.container._from_factory(  # pylint: disable=protected-access
                factory[key], schema=value
            )

            obj._column_names.append(key)

        if RECORD_KEY in factory:
            obj._record_key = ColumnSeries._from_factory(  # pylint: disable=protected-access
                factory[RECORD_KEY], pt.string(nullable=True)
            )

        return obj

    @classmethod
    def from_pyarrow(
        cls: Type[_T], array: pa.StructArray, schema: Optional[pt.PortexRecordBase] = None
    ) -> _T:
        """Create DataFrame with pyarrow struct array.

        Arguments:
            array: The input pyarrow struct array.
            schema: The schema of the DataFrame.

        Raises:
            TypeError: When the given schema is mismatched with the pyarrow array type.

        Returns:
            The loaded :class:`~graviti.dataframe.DataFrame` instance.

        """
        if schema is None:
            schema = pt.record.from_pyarrow(array.type)
        elif not array.type.equals(schema.to_pyarrow()):
            raise TypeError("The schema is mismatched with the pyarrow array")

        return cls._from_pyarrow(array, schema)

    def _setitem(self, key: str, value: Union[Iterable[Any], _C]) -> None:
        if isinstance(value, Container):
            schema = value.schema.copy()
            column = value.copy()
        else:
            # TODO: Need to support the case where iterable elements are subclass of Container.
            array = pa.array(value)
            schema = pt.PortexType.from_pyarrow(array.type)
            column = schema.container._from_pyarrow(  # pylint: disable=protected-access
                array, schema
            )

        if len(column) != len(self):
            raise ValueError(
                f"Length of values ({len(column)}) does not match length of "
                f"self DataFrame ({len(self)})"
            )

        self.schema[key] = schema
        self._columns[key] = column
        if key not in self._column_names:
            self._column_names.append(key)

    def _flatten(self) -> Tuple[List[Tuple[str, ...]], List[ColumnSeriesBase]]:
        header: List[Tuple[str, ...]] = []
        data: List[ColumnSeriesBase] = []
        for key, value in self._columns.items():
            if isinstance(value, DataFrame):
                nested_header, nested_data = value._flatten()  # pylint: disable=protected-access
                header.extend((key, *sub_column) for sub_column in nested_header)
                data.extend(nested_data)
            elif isinstance(value, ColumnSeriesBase):
                data.append(value)
                header.append((key,))
            else:
                raise TypeError(f"Unsupported column type: {value.__class__}")

        return header, data

    def _repr_folding(self) -> str:
        return f"{self.__class__.__name__}{self.shape}"

    def _get_repr_indices(self) -> Iterable[int]:
        return islice(range(len(self)), MAX_REPR_ROWS)

    def _get_repr_body(self, flatten_data: List[ColumnSeriesBase]) -> List[List[str]]:
        lines = []
        for i in self._get_repr_indices():
            line: List[str] = [str(i)]
            for column in flatten_data:
                item = column[i]
                name = (
                    item._repr_folding()  # pylint: disable=protected-access
                    if isinstance(item, DataFrame)
                    else str(item)
                )
                line.append(name)
            lines.append(line)
        return lines

    def _extend(self, values: "DataFrame") -> None:
        for key, value in self._columns.items():
            value._extend(values[key])  # pylint: disable=protected-access

    def _to_patch_data(self) -> List[Dict[str, Any]]:
        names = self._column_names.copy()
        values: List[Any] = list(self._columns.values())

        names.append(RECORD_KEY)
        values.append(self._record_key)

        return [
            dict(zip(names, value)) for value in zip(*(column.to_pylist() for column in values))
        ]

    @property
    def iloc(self) -> DataFrameILocIndexer:
        """Purely integer-location based indexing for selection by position.

        Allowed inputs are:

        - An integer, e.g. ``5``.
        - A list or array of integers, e.g. ``[4, 3, 0]``.
        - A slice object with ints, e.g. ``1:7``.
        - A boolean array of the same length as the axis being sliced.

        Returns:
            The instance of the ILocIndexer.

        Examples:
            >>> df = DataFrame({"col1": [1, 2], "col2": [3, 4]})
            >>> df.iloc[0]
            col1    1
            col2    3
            Name: 0, dtype: int64
            >>> df.iloc[[0]]
               col1  col2
            0     1     3

        """
        return DataFrameILocIndexer(self)

    @property
    def loc(self) -> DataFrameLocIndexer:
        """Access a group of rows and columns by indexes or a boolean array.

        Allowed inputs are:

        - A single index, e.g. ``5``.
        - A list or array of indexes, e.g. ``[4, 3, 0]``.
        - A slice object with indexes, e.g. ``1:7``.
        - A boolean array of the same length as the axis being sliced.

        Returns:
            The instance of the LocIndexer.

        Examples:
            >>> df = DataFrame({"col1": [1, 2], "col2": [3, 4]})
            >>> df.loc[0]
            col1    1
            col2    3
            Name: 0, dtype: int64
            >>> df.loc[[0]]
               col1  col2
            0     1     3

        """
        return DataFrameLocIndexer(self)

    @property
    def shape(self) -> Tuple[int, int]:
        """Return a tuple representing the dimensionality of the DataFrame.

        Returns:
            Shape of the DataFrame.

        Examples:
            >>> df = DataFrame(
            ...     [
            ...         {"filename": "a.jpg", "box2ds": {"x": 1, "y": 1}},
            ...         {"filename": "b.jpg", "box2ds": {"x": 2, "y": 2}},
            ...         {"filename": "c.jpg", "box2ds": {"x": 3, "y": 3}},
            ...     ]
            ... )
            >>> df
                filename box2ds
                         x      y
            0   a.jpg    1      1
            1   b.jpg    2      2
            2   c.jpg    3      3
            >>> df.shape
            (3, 2)

        """
        return (self.__len__(), len(self._column_names))

    # @property
    # def size(self) -> int:
    #     """Return an int representing the number of elements in this object.
    #
    #     Return:
    #         Size of the DataFrame.
    #
    #     Examples:
    #         >>> df = DataFrame({"col1": [1, 2], "col2": [3, 4]})
    #         >>> df.size
    #         4
    #
    #     """

    # @overload
    # def _getitem_by_location(self, key: slice) -> "DataFrame":
    #    ...

    # @overload
    # def _getitem_by_location(self, key: int) -> RowSeries:
    #     ...

    # @overload
    # def _getitem_by_location(self, key: Iterable[int]) -> "DataFrame":
    #     ...

    def _getitem_by_location(self, key: int) -> RowSeries:
        indices_data = {name: self._columns[name].iloc[key] for name in self._column_names}
        return RowSeries._construct(indices_data)  # pylint: disable=protected-access

    # @overload
    # @staticmethod
    # def _get_location_by_index(key: Iterable[int]) -> Iterable[int]:
    #     ...

    # @overload
    # @staticmethod
    # def _get_location_by_index(key: int) -> int:
    #     ...

    # @staticmethod
    # def _get_location_by_index(key: Union[int, Iterable[int]]) -> Union[int, Iterable[int]]:
    #     return key

    # def head(self, n: int = 5) -> "DataFrame":
    #     """Return the first `n` rows.
    #
    #     Arguments:
    #         n: Number of rows to select.
    #
    #     Return:
    #         The first `n` rows.
    #
    #     Examples:
    #         >>> df = DataFrame(
    #         ...     {
    #         ...         "animal": [
    #         ...             "alligator",
    #         ...             "bee",
    #         ...             "falcon",
    #         ...             "lion",
    #         ...             "monkey",
    #         ...             "parrot",
    #         ...             "shark",
    #         ...             "whale",
    #         ...             "zebra",
    #         ...         ]
    #         ...     }
    #         ... )
    #         >>> df
    #               animal
    #         0  alligator
    #         1        bee
    #         2     falcon
    #         3       lion
    #         4     monkey
    #         5     parrot
    #         6      shark
    #         7      whale
    #         8      zebra
    #
    #         Viewing the first `n` lines (three in this case)
    #
    #         >>> df.head(3)
    #               animal
    #         0  alligator
    #         1        bee
    #         2     falcon
    #
    #         For negative values of `n`
    #
    #         >>> df.head(-3)
    #               animal
    #         0  alligator
    #         1        bee
    #         2     falcon
    #         3       lion
    #         4     monkey
    #         5     parrot
    #
    #     """

    # def tail(self, n: int = 5) -> "DataFrame":
    #     """Return the last `n` rows.
    #
    #     Arguments:
    #         n: Number of rows to select.
    #
    #     Return:
    #         The last `n` rows.
    #
    #     Examples:
    #         >>> df = DataFrame(
    #         ...     {
    #         ...         "animal": [
    #         ...             "alligator",
    #         ...             "bee",
    #         ...             "falcon",
    #         ...             "lion",
    #         ...             "monkey",
    #         ...             "parrot",
    #         ...             "shark",
    #         ...             "whale",
    #         ...             "zebra",
    #         ...         ]
    #         ...     }
    #         ... )
    #         >>> df
    #               animal
    #         0  alligator
    #         1        bee
    #         2     falcon
    #         3       lion
    #         4     monkey
    #         5     parrot
    #         6      shark
    #         7      whale
    #         8      zebra
    #
    #         Viewing the last 5 lines
    #
    #         >>> df.tail()
    #            animal
    #         4  monkey
    #         5  parrot
    #         6   shark
    #         7   whale
    #         8   zebra
    #
    #         Viewing the last `n` lines (three in this case)
    #
    #         >>> df.tail(3)
    #           animal
    #         6  shark
    #         7  whale
    #         8  zebra
    #
    #     """

    def copy(self: _T) -> _T:
        """Get a copy of the dataframe.

        Returns:
            A copy of the dataframe.

        """
        obj: _T = object.__new__(self.__class__)

        obj.schema = self.schema.copy()

        columns = {}
        for key, value in self._columns.items():
            columns[key] = value.copy()

        # pylint: disable=protected-access
        obj._columns = columns
        obj._column_names = self._column_names.copy()
        return obj

    # def sample(self, n: Optional[int] = None, axis: Optional[int] = None) -> "DataFrame":
    #     """Return a random sample of items from an axis of object.
    #
    #     Arguments:
    #         n: Number of items from axis to return.
    #         axis: {0 or `index`, 1 or `columns`, None}
    #             Axis to sample. Accepts axis number or name. Default is stat axis
    #             for given data type (0 for Series and DataFrames).
    #
    #     Return:
    #         A new object of same type as caller containing `n` items randomly
    #         sampled from the caller object.
    #
    #     """

    # def info(self) -> None:
    #     """Print a concise summary of a DataFrame."""

    def extend(self, values: Union[Iterable[Dict[str, Any]], "DataFrame"]) -> None:
        """Extend Sequence object or DataFrame to itself row by row.

        Arguments:
            values: A sequence object or DataFrame.

        Raises:
            TypeError: When the given Dataframe mismatched with the self schema.

        Examples:
            >>> df = DataFrame([
            ...     {"filename": "a.jpg", "box2ds": {"x": 1, "y": 1}},
            ...     {"filename": "b.jpg", "box2ds": {"x": 2, "y": 2}},
            ... ])

            Extended by another list.

            >>> df.extend([{"filename": "c.jpg", "box2ds": {"x": 3, "y": 3}}])
            >>> df
                filename box2ds
                         x      y
            0   a.jpg    1      1
            1   b.jpg    2      2
            2   c.jpg    3      3

            Extended by another DataFrame.

            >>> df2 = DataFrame([{"filename": "d.jpg", "box2ds": {"x": 4 "y": 4}}])
            >>> df.extend(df2)
            >>> df
                filename box2ds
                         x      y
            0   a.jpg    1      1
            1   b.jpg    2      2
            2   d.jpg    4      4

        """
        if not isinstance(values, DataFrame):
            values = DataFrame._from_pyarrow(  # pylint: disable=protected-access
                self._pylist_to_pyarrow(values, self.schema), self.schema
            )
        elif not self.schema.to_pyarrow().equals(values.schema.to_pyarrow()):
            raise TypeError("The schema of the given DataFrame is mismatched")
        elif self.operations is not None:
            values = values.copy()

        values_length = len(values)
        self._extend(values)
        if self.operations is not None:
            self.operations.append(AddData(values))

        if self._record_key is not None:
            self._record_key._data.extend_nulls(values_length)  # pylint: disable=protected-access

    @staticmethod
    def _get_process(value: Any, schema: pt.PortexType) -> Tuple[Callable[[Any], Any], bool]:
        container = schema.container
        if container == DataFrame:
            return DataFrame._process_record(value, schema)  # type: ignore[arg-type]
        if container == ArraySeries:
            return DataFrame._process_array(value, schema.items)  # type: ignore[attr-defined]
        if container == FileSeries:
            return DataFrame._process_file(value)

        return lambda x: x, False

    @staticmethod
    def _process_array(
        values: Iterable[Any], schema: pt.PortexType
    ) -> Tuple[Callable[[Any], Any], bool]:
        if isinstance(values, DataFrame):
            return lambda x: x.to_pylist(), True

        for value in values:
            if value is None:
                continue

            processor, need_process = DataFrame._get_process(value, schema)
            return lambda x: list(map(processor, x)), need_process
        return lambda x: x, False

    @staticmethod
    def _process_file(values: Iterable[Any]) -> Tuple[Callable[[Any], Any], bool]:
        if isinstance(values, FileBase):
            return lambda x: x.to_pyobj(), True

        return lambda x: x, False

    @staticmethod
    def _process_record(
        values: Dict[str, Any], schema: pt.record
    ) -> Tuple[Callable[[Any], Any], bool]:
        processors: Dict[str, Callable[[Any], Any]] = {}
        need_process, sub_need_process = False, False
        fields = schema.to_builtin().fields
        for name, field in fields.items():
            item = values[name]
            if item is None:
                processors[name] = lambda x: x
            else:
                processors[name], sub_need_process = DataFrame._get_process(item, field)
                need_process = need_process or sub_need_process

        return lambda x: {k: processors[k](v) for k, v in x.items()}, need_process

    @staticmethod
    def _pylist_to_pyarrow(values: Iterable[Any], schema: pt.PortexRecordBase) -> pa.StructArray:

        # In this case schema.to_builtin always returns record.
        processor, need_process = DataFrame._process_array(values, schema)

        if not need_process:
            return pa.array(values, schema.to_pyarrow())

        return pa.array(processor(values), schema.to_pyarrow())

    def to_pylist(self) -> List[Dict[str, Any]]:
        """Convert the DataFrame to a python list.

        Returns:
            The python list representing the DataFrame.

        """
        return [
            dict(zip(self._column_names, values))
            for values in zip(*(column.to_pylist() for column in self._columns.values()))
        ]

    def query(self, func: Callable[[Any], Any]) -> "DataFrame":
        """Query the columns of a DataFrame with a lambda function.

        Arguments:
            func: The query function.

        Returns:
            The query result DataFrame.

        Raises:
            TypeError: When the DataFrame is not in a Commit.

        Examples:
            >>> df = DataFrame([
            ...     {"filename": "a.jpg", "box2ds": {"x": 1, "y": 1}},
            ...     {"filename": "b.jpg", "box2ds": {"x": 2, "y": 2}},
            ... ])
            >>> df.query(lambda x: x["filename"] == "a.jpg")
                filename box2ds
                         x      y
            0   a.jpg    1      1

        """
        if engine.mode != Mode.ONLINE:
            raise TypeError(
                "Only online mode is supported now, "
                "Please use 'engine.online' with block to enable the online mode."
            )
        if self.searcher is None:
            raise TypeError("'query' is not supported for the DataFrame not in a Commit")

        result = func(SqlRowSeries(self.schema))
        if self.criteria is None:
            criteria = {"where": result.expr}
        else:
            criteria = {
                "where": {
                    "$and": [
                        self.criteria["where"],  # pylint: disable=unsubscriptable-object
                        result.expr,
                    ]
                }
            }

        dataframe = self.searcher(criteria)  # pylint: disable=not-callable
        dataframe.criteria = criteria
        dataframe.searcher = self.searcher
        return dataframe

    def apply(self, func: Callable[[Any], Any]) -> Container:
        """Apply a function to the DataFrame row by row.

        Arguments:
            func: Function to apply to each row.

        Returns:
            The apply result DataFrame or Series.

        Raises:
            TypeError: When the DataFrame is not in a Commit.

        Examples:
            >>> df = DataFrame([
            ...     {"filename": "a.jpg", "box2ds": {"x": 1, "y": 1}},
            ...     {"filename": "b.jpg", "box2ds": {"x": 2, "y": 2}},
            ... ])
            >>> df.apply(lambda x: x["box2ds"]["x"] + 1)
                filename box2ds
                         x      y
            0   a.jpg    2      1
            1   b.jpg    3      2

        """
        if engine.mode != Mode.ONLINE:
            raise TypeError(
                "Only online mode is supported now, "
                "Please use 'engine.online' with block to enable the online mode."
            )
        if self.searcher is None:
            raise TypeError("'apply' is not supported for the DataFrame not in a Commit")

        result = func(SqlRowSeries(self.schema))
        criteria = {} if self.criteria is None else self.criteria.copy()
        criteria["select"] = [result.expr]
        dataframe = self.searcher(criteria)  # pylint: disable=not-callable
        key = dataframe._column_names[0]  # pylint: disable=protected-access
        return dataframe[key]
