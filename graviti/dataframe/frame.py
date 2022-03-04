#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the Graviti DataFrame."""


from typing import Any, Dict, Iterable, Optional, Tuple, Union, overload

from graviti.dataframe.column.series import Series
from graviti.dataframe.indexing import DataFrameILocIndexer, DataFrameLocIndexer


class DataFrame:
    """Two-dimensional, size-mutable, potentially heterogeneous tabular data.

    Arguments:
        data: The data that needs to be stored in DataFrame. Could be ndarray, Iterable or dict.
            If data is a dict, column order follows insertion-order.
        schema: The schema of the DataFrame. If None, will be inferred from `data`.
        columns: Column labels to use for resulting frame when data does not have them,
            defaulting to RangeIndex(0, 1, 2, ..., n). If data contains column labels,
            will perform column selection instead.
        client: The client for getting a remote data.

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

    def __init__(
        self,
        data: Union[Iterable[Any], Dict[Any, Any], "DataFrame", None] = None,
        schema: Any = None,
        columns: Optional[Iterable[str]] = None,
        client: Any = None,
    ) -> None:
        pass

    @overload
    def __getitem__(self, key: str) -> Union[Series, "DataFrame"]:  # type: ignore[misc]
        # https://github.com/python/mypy/issues/5090
        ...

    @overload
    def __getitem__(self, key: Iterable[str]) -> "DataFrame":
        ...

    def __getitem__(self, key: Union[str, Iterable[str]]) -> Union[Series, "DataFrame"]:
        pass

    def __setitem__(self, key: str, value: Iterable[Any]) -> None:
        pass

    def __repr__(self) -> str:
        pass

    def __len__(self) -> int:
        pass

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
