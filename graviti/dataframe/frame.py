#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the Graviti DataFrame."""


from typing import (
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
from graviti.utility.lazy import LazyFactory


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
           col1  col2
        0     1     3
        1     2     4

        Constructing DataFrame from numpy ndarray:

        >>> df2 = DataFrame(np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]]), columns=["a", "b", "c"])
        >>> df2
           a  b  c
        0  1  2  3
        1  4  5  6
        2  7  8  9

    """

    _T = TypeVar("_T", bound="DataFrame")
    _columns: Dict[str, Union["DataFrame", ColumnSeries]]
    _column_names: List[str]
    _index: ColumnSeries

    def __init__(
        self,
        data: Union[Sequence[Sequence[Any]], Dict[str, Any], "DataFrame", None] = None,
        schema: Any = None,
        columns: Optional[Iterable[str]] = None,
    ) -> None:
        if data is None:
            data = {}
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
        return f"\n DataFrame\n columns: {self._columns} \n index: {self._index}"

    def __len__(self) -> int:
        return self._columns[self._column_names[0]].__len__()

    @classmethod
    def from_lazy_factory(cls: Type[_T], factory: LazyFactory) -> _T:
        """Create DataFrame with lazy factory and schema.

        Arguments:
            factory: class :class:`~graviti.utility.lazy.LazyFactory` instance.

        Return:
            The loaded :class:`~graviti.dataframe.DataFrame` object.

        """

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

        Return:
            Shape of the DataFrame.

        Examples:
            >>> df = DataFrame({"col1": [1, 2], "col2": [3, 4]})
            >>> df.shape
            (2, 2)

        """

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
