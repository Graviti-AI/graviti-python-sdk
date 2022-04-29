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
    Sequence,
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
from graviti.portex import PortexExternalType, PortexType, record
from graviti.utility import MAX_REPR_ROWS

if TYPE_CHECKING:
    from graviti.manager import PagingLists

_T = TypeVar("_T", bound="DataFrame")
_S = Union[record, PortexExternalType]


class DataFrame:
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

    _columns: Dict[str, Union["DataFrame", ColumnSeries]]
    _column_names: List[str]
    schema: _S

    def __init__(
        self,
        data: Union[
            Sequence[Sequence[Any]], Dict[str, Any], "DataFrame", "PagingLists", None
        ] = None,
        schema: Any = None,
        columns: Optional[Iterable[str]] = None,
    ) -> None:
        if data is None:
            data = {}  # type: ignore[assignment]
            # https://github.com/python/mypy/issues/6463
        if schema is not None:
            # TODO: missing schema processing
            pass
        if columns is not None:
            # TODO: missing columns processing
            pass

        self._columns = {}
        self._column_names = []
        if isinstance(data, dict):
            for key, value in data.items():
                self._columns[key] = (
                    DataFrame(value) if isinstance(value, dict) else ColumnSeries(value, name=key)
                )
                self._column_names.append(key)
        else:
            raise ValueError("DataFrame only supports generating from dictionary now")

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
            return self._construct(new_columns)

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
    ) -> "DataFrame":
        obj: DataFrame = object.__new__(cls)
        obj._columns = columns
        obj._column_names = list(obj._columns.keys())
        return obj

    @classmethod
    def _from_pyarrow(cls: Type[_T], array: pa.StructArray, schema: _S) -> _T:
        obj: _T = object.__new__(cls)
        obj.schema = schema
        obj._columns = {}
        obj._column_names = []

        # In this case it's always record.
        expanded_schema: record = schema.to_builtin()  # type:ignore[assignment]

        for pafield in array.type:
            column_name = pafield.name
            column_value = array.field(column_name)
            column_schema = expanded_schema.fields[column_name]
            if isinstance(pafield.type, pa.StructType):
                # if pafield.type is pa.StructType,
                # the schema will always be record or PortexExternalType.
                obj._columns[column_name] = DataFrame._from_pyarrow(
                    column_value, schema=column_schema  # type:ignore[arg-type]
                )
            else:
                obj._columns[
                    column_name
                ] = ColumnSeries._from_pyarrow(  # pylint: disable=protected-access
                    column_value, schema=column_schema
                )

            obj._column_names.append(column_name)
        return obj

    @classmethod
    def from_paging_lists(cls: Type[_T], paging_lists: "PagingLists") -> _T:
        """Create DataFrame with paging lists.

        Arguments:
            paging_lists: A dict used to initialize dataframe whose data is stored in
                lazy-loaded form.

        Returns:
            The loaded :class:`~graviti.dataframe.DataFrame` object.

        """
        return cls(paging_lists)

    @classmethod
    def from_pyarrow(cls: Type[_T], array: pa.StructArray, schema: Optional[_S] = None) -> _T:
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
            portex_type = PortexType.from_pyarrow(array.type)

        else:
            if not array.type.equals(schema.to_pyarrow()):
                raise TypeError("The schema is mismatched with the pyarrow array.")

            portex_type = schema

        return cls._from_pyarrow(array, portex_type)  # type:ignore[arg-type]

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
        return islice(range(len(self)), MAX_REPR_ROWS)

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

    # @overload
    # def _getitem_by_location(self, key: int) -> RowSeries:
    #     ...

    # @overload
    # def _getitem_by_location(self, key: Iterable[int]) -> "DataFrame":
    #     ...

    def _getitem_by_location(self, key: int) -> RowSeries:
        indices_data = {name: self._columns[name].iloc[key] for name in self._column_names}
        return RowSeries._construct(indices_data, key)  # pylint: disable=protected-access

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

    def to_pylist(self) -> List[Dict[str, Any]]:
        """Convert the DataFrame to a python list.

        Returns:
            The python list representing the DataFrame.

        """
        return [
            dict(zip(self._column_names, values))
            for values in zip(*(column.to_pylist() for column in self._columns.values()))
        ]
