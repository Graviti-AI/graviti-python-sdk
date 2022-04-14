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

from graviti.dataframe.column.series import Series as ColumnSeries
from graviti.dataframe.indexing import DataFrameILocIndexer, DataFrameLocIndexer
from graviti.dataframe.row.series import Series as RowSeries
from graviti.utility import MAX_REPR_ROWS

if TYPE_CHECKING:
    from graviti.manager import LazyLists


class DataFrame:
    """Two-dimensional, size-mutable, potentially heterogeneous tabular data.

    Arguments:
        data: The data that needs to be stored in DataFrame. Could be ndarray, Iterable or dict.
            If data is a dict, column order follows insertion-order.
        schema: The schema of the DataFrame. If None, will be inferred from `data`.
        columns: Column labels to use for resulting frame when data does not have them,
            defaulting to RangeIndex(0, 1, 2, ..., n). If data contains column labels,
            will perform column selection instead.

    Examples:
        Constructing DataFrame from a dictionary.

        >>> data = {"col1": [1, 2], "col2": [3, 4]}
        >>> df = DataFrame(data=data)
        >>> df
            col1 col2
        0   1    3
        1   2    4

        >>> df = DataFrame(
                {
                    "filename": ["a.jpg", "b.jpg", "c.jpg"],
                    "box2ds": {"x": [1, 2, 3], "y": [1, 2, 3]},
                }
            )
        >>> df
            filename box2ds
                     x      y
        0   a.jpg    1      1
        1   b.jpg    2      2
        2   c.jpg    3      3

    """

    _T = TypeVar("_T", bound="DataFrame")
    _columns: Dict[str, Union["DataFrame", ColumnSeries]]
    _column_names: List[str]
    _index: ColumnSeries

    def __init__(
        self,
        data: Union[Sequence[Sequence[Any]], Dict[str, Any], "DataFrame", "LazyLists", None] = None,
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
            self._index = ColumnSeries(list(range(self.__len__())))
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
        return cls(lazy_lists)

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
                    {
                        "filename": ["a.jpg", "b.jpg", "c.jpg"],
                        "box2ds": {"x": [1, 2, 3], "y": [1, 2, 3]},
                    }
                )
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
