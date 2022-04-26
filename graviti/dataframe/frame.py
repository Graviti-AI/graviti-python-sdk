#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the Graviti DataFrame."""


from itertools import chain, islice, zip_longest
from typing import (
    TYPE_CHECKING,
    Any,
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

from graviti.dataframe.column.series import Series as ColumnSeries
from graviti.dataframe.indexing import DataFrameILocIndexer, DataFrameLocIndexer
from graviti.dataframe.row.series import Series as RowSeries
from graviti.portex import int32, record
from graviti.utility import MAX_REPR_ROWS, NestedDict

if TYPE_CHECKING:
    from graviti.manager import LazyLists

_T = TypeVar("_T", bound="DataFrame")


class DataFrame:
    """Two-dimensional, size-mutable, potentially heterogeneous tabular data.

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

    _columns: Dict[str, Union["DataFrame", ColumnSeries]]
    _column_names: List[str]
    _index: ColumnSeries
    schema: record

    def __new__(
        cls: Type[_T],
        data: Union[Iterable[NestedDict[str, Any]], "DataFrame", None] = None,
        schema: Optional[record] = None,
    ) -> _T:
        """Two-dimensional, size-mutable, potentially heterogeneous tabular data.

        Arguments:
            data: The data that needs to be stored in DataFrame.
            schema: The schema of the DataFrame. If None, will be inferred from `data`.

        Raises:
            NotImplementedError: When needed infered schema from the data.
            ValueError: When the given data is not iterable.

        Returns:
            The created :class:`~graviti.dataframe.DataFrame` object.

        """
        if schema is None:
            if data is None:
                obj: _T = object.__new__(cls)
                obj._columns = {}
                obj._column_names = []
                obj._index = ColumnSeries([], schema=int32())  # schema has no null type.
                obj.schema = record([])
                return obj

            raise NotImplementedError("Inferred schema from `data` is not support now.")

        if not isinstance(data, DataFrame) and isinstance(data, Iterable):
            # TODO: Check the schema is valid for pyarrow struct array.
            return cls.from_pyarrow(pa.array(data, schema.to_pyarrow()), schema)

        raise ValueError("DataFrame only supports generating from Iterable object now")

    @overload
    def __getitem__(self, key: str) -> Union[ColumnSeries, "DataFrame"]:  # type: ignore[misc]
        # https://github.com/python/mypy/issues/5090
        ...

    @overload
    def __getitem__(self, key: Iterable[str]) -> "DataFrame":
        ...

    def __getitem__(self, key: Union[str, Iterable[str]]) -> Union[ColumnSeries, "DataFrame"]:
        if isinstance(key, str):
            return self._columns[key]

        if isinstance(key, Iterable):
            new_columns = {name: self._columns[name] for name in key}
            return self._construct(new_columns, self._index)

        raise KeyError(key)

    def __setitem__(self, key: str, value: Iterable[Any]) -> None:
        pass

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
        if not self._columns:
            return 0

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
        columns: Dict[str, Union["DataFrame", ColumnSeries]],
        index: ColumnSeries,
    ) -> "DataFrame":
        obj: DataFrame = object.__new__(cls)
        obj._columns = columns
        obj._column_names = list(obj._columns.keys())
        obj._index = index
        return obj

    @classmethod
    def from_lazy_lists(cls: Type[_T], lazy_lists: "LazyLists") -> _T:
        """Create DataFrame with lazy lists.

        Arguments:
            lazy_lists: A dict used to initialize dataframe whose data is stored in
                lazy-loaded form.

        Returns:
            The loaded :class:`~graviti.dataframe.DataFrame` object.

        """
        # Not support loading tensorbay Dataset.
        return cls(lazy_lists)  # type: ignore[arg-type]

    @classmethod
    def from_pyarrow(cls: Type[_T], array: pa.StructArray, schema: record) -> _T:
        """Create DataFrame with pyarrow struct array.

        Arguments:
            array: The input pyarrow struct array.
            schema: Schema.

        Returns:
            The loaded :class:`~graviti.dataframe.DataFrame` object.

        """
        obj: _T = object.__new__(cls)
        obj.schema = schema
        obj._columns = {}
        obj._column_names = []

        for pafield in array.type:
            column_name = pafield.name
            column_value = array.field(column_name)
            column_schema = schema.fields[column_name]
            obj._columns[column_name] = (
                DataFrame.from_pyarrow(column_value, schema=column_schema)
                if isinstance(pafield.type, pa.StructType) and isinstance(column_schema, record)
                else ColumnSeries(column_value, name=column_name, schema=column_schema)
            )
            obj._column_names.append(column_name)
        obj._index = ColumnSeries(range(len(column_value)), schema=int32())
        return obj

    def _flatten(self) -> Tuple[List[Tuple[str, ...]], List[ColumnSeries]]:
        header: List[Tuple[str, ...]] = []
        data: List[ColumnSeries] = []
        for key, value in self._columns.items():
            if isinstance(value, DataFrame):
                nested_header, nested_data = value._flatten()  # pylint: disable=protected-access
                header.extend((key, *sub_column) for sub_column in nested_header)
                data.extend(nested_data)
            else:
                data.append(value)
                header.append((key,))

        return header, data

    def _repr_folding(self) -> str:
        return f"{self.__class__.__name__}{self.shape}"

    def _get_repr_indices(self) -> Iterable[int]:
        length = self.__len__()
        # pylint: disable=protected-access
        if self._index._indices is None:
            return range(min(length, MAX_REPR_ROWS))

        if length >= MAX_REPR_ROWS:
            return islice(self._index._indices, MAX_REPR_ROWS)

        return self._index._indices

    def _get_repr_body(self, flatten_data: List[ColumnSeries]) -> List[List[str]]:
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

    @property
    def size(self) -> int:
        """Return an int representing the number of elements in this object.

        Return:
            Size of the DataFrame.

        Examples:
            >>> df = DataFrame({"col1": [1, 2], "col2": [3, 4]})
            >>> df.size
            4

        """

    # @overload
    # def _getitem_by_location(self, key: slice) -> "DataFrame":
    #    ...

    @overload
    def _getitem_by_location(self, key: int) -> RowSeries:
        ...

    @overload
    def _getitem_by_location(self, key: Iterable[int]) -> "DataFrame":
        ...

    def _getitem_by_location(self, key: Union[int, Iterable[int]]) -> Union["DataFrame", RowSeries]:
        if isinstance(key, int):
            indices_data = {name: self._columns[name].iloc[key] for name in self._column_names}
            return RowSeries._construct(indices_data, key)  # pylint: disable=protected-access

        return self._construct(self._columns, self._index[key])

    @overload
    def _get_location_by_index(self, key: Iterable[int]) -> List[int]:
        ...

    @overload
    def _get_location_by_index(self, key: int) -> int:
        ...

    def _get_location_by_index(self, key: Union[int, Iterable[int]]) -> Union[int, List[int]]:
        return self._index._get_location_by_index(key)  # pylint: disable=protected-access

    def head(self, n: int = 5) -> "DataFrame":
        """Return the first `n` rows.

        Arguments:
            n: Number of rows to select.

        Return:
            The first `n` rows.

        Examples:
            >>> df = DataFrame(
            ...     {
            ...         "animal": [
            ...             "alligator",
            ...             "bee",
            ...             "falcon",
            ...             "lion",
            ...             "monkey",
            ...             "parrot",
            ...             "shark",
            ...             "whale",
            ...             "zebra",
            ...         ]
            ...     }
            ... )
            >>> df
                  animal
            0  alligator
            1        bee
            2     falcon
            3       lion
            4     monkey
            5     parrot
            6      shark
            7      whale
            8      zebra

            Viewing the first `n` lines (three in this case)

            >>> df.head(3)
                  animal
            0  alligator
            1        bee
            2     falcon

            For negative values of `n`

            >>> df.head(-3)
                  animal
            0  alligator
            1        bee
            2     falcon
            3       lion
            4     monkey
            5     parrot

        """

    def tail(self, n: int = 5) -> "DataFrame":
        """Return the last `n` rows.

        Arguments:
            n: Number of rows to select.

        Return:
            The last `n` rows.

        Examples:
            >>> df = DataFrame(
            ...     {
            ...         "animal": [
            ...             "alligator",
            ...             "bee",
            ...             "falcon",
            ...             "lion",
            ...             "monkey",
            ...             "parrot",
            ...             "shark",
            ...             "whale",
            ...             "zebra",
            ...         ]
            ...     }
            ... )
            >>> df
                  animal
            0  alligator
            1        bee
            2     falcon
            3       lion
            4     monkey
            5     parrot
            6      shark
            7      whale
            8      zebra

            Viewing the last 5 lines

            >>> df.tail()
               animal
            4  monkey
            5  parrot
            6   shark
            7   whale
            8   zebra

            Viewing the last `n` lines (three in this case)

            >>> df.tail(3)
              animal
            6  shark
            7  whale
            8  zebra

        """

    def sample(self, n: Optional[int] = None, axis: Optional[int] = None) -> "DataFrame":
        """Return a random sample of items from an axis of object.

        Arguments:
            n: Number of items from axis to return.
            axis: {0 or `index`, 1 or `columns`, None}
                Axis to sample. Accepts axis number or name. Default is stat axis
                for given data type (0 for Series and DataFrames).

        Return:
            A new object of same type as caller containing `n` items randomly
            sampled from the caller object.

        """

    def info(self) -> None:
        """Print a concise summary of a DataFrame."""

    def extend(self, objs: Union[Iterable[Dict[str, Any]], "DataFrame"]) -> None:
        """Extend Sequence object or DataFrame to itself row by row.

        Arguments:
            objs: A sequence object or DataFrame.

        Raises:  # noqa: DAR402
            ValueError: When the key or column name of objs not exists in self dataframe.

        Examples:
        >>> df = DataFrame([
        ...     {"filename": "a.jpg", "box2ds": {"x": 1, "y": 1}},
        ...     {"filename": "b.jpg", "box2ds": {"x": 2, "y": 2}},
        ... ])
        >>> df.extend([{"filename": "c.jpg", "box2ds": {"x": 3, "y": 3}])
        >>> df
            filename box2ds
                     x      y
        0   a.jpg    1      1
        1   b.jpg    2      2
        2   c.jpg    3      3

        >>> df2 = DataFrame([{"filename": "d.jpg", "box2ds": {"x": 4 "y": 4}}])
        >>> df.extend(df2)
        >>> df
            filename box2ds
                     x      y
        0   a.jpg    1      1
        1   b.jpg    2      2
        2   d.jpg    4      4

        """
