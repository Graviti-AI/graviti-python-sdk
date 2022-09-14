#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the Graviti indexing related class."""

from typing import TYPE_CHECKING, Any, Iterable, Tuple, Union, overload

from graviti.dataframe.column.series import Series as ColumnSeries
from graviti.dataframe.column.series import SeriesBase
from graviti.operation import UpdateData
from graviti.utility import NestedDict

if TYPE_CHECKING:
    from graviti.dataframe import DataFrame
    from graviti.dataframe.row.series import Series as RowSeries


class DataFrameILocIndexer:
    """Index class for DataFrame.iloc."""

    def __init__(self, obj: "DataFrame") -> None:
        self.obj = obj

    # @overload
    # def __getitem__(self, key: Iterable[int]) -> "DataFrame":
    #     ...

    @overload
    def __getitem__(self, key: slice) -> "DataFrame":
        ...

    @overload
    def __getitem__(self, key: Tuple[Union[int, slice], str]) -> Any:
        ...

    @overload
    def __getitem__(self, key: int) -> "RowSeries":
        ...

    def __getitem__(
        self, key: Union[int, slice, Tuple[Union[int, slice], str]]
    ) -> Union[Any, "DataFrame", "RowSeries"]:
        if isinstance(key, tuple):
            index, name = key
            return self.obj[name].iloc[index]

        if isinstance(key, int):
            return self.obj._get_item_by_location(key)

        return self.obj._get_slice_by_location(key)

    @overload
    def __setitem__(self, key: int, value: NestedDict[str, Any]) -> None:
        ...

    @overload
    def __setitem__(
        self, key: slice, value: Union[Iterable[NestedDict[str, Any]], "DataFrame"]
    ) -> None:
        ...

    @overload
    def __setitem__(self, key: Tuple[int, str], value: Any) -> None:
        ...

    @overload
    def __setitem__(self, key: Tuple[slice, str], value: Union[Iterable[Any], SeriesBase]) -> None:
        ...

    def __setitem__(
        self,
        key: Union[int, slice, Tuple[int, str], Tuple[slice, str]],
        value: Union[
            NestedDict[str, Any],
            "DataFrame",
            Iterable[NestedDict[str, Any]],
            Any,
            Iterable[Any],
            SeriesBase,
        ],
    ) -> None:
        if isinstance(key, tuple):
            index, name = key
            self.obj[name].loc[index] = value
            return

        if isinstance(key, int):
            # pylint: disable=protected-access
            value = self.obj._from_pyarrow(
                self.obj._pylist_to_pyarrow([value], self.obj.schema),
                self.obj.schema,
            )
            key = slice(key, key + 1)
        elif not isinstance(value, self.obj.__class__):
            value = self.obj._from_pyarrow(
                self.obj._pylist_to_pyarrow(value, self.obj.schema),  # type: ignore[arg-type]
                self.obj.schema,
            )
        elif not self.obj.schema.to_pyarrow().equals(value.schema.to_pyarrow()):
            raise TypeError("The schema of the given DataFrame is mismatched")

        self.obj._set_slice_by_location(key, value)
        if self.obj.operations is not None:
            df = value.copy()
            _record_key = self.obj._record_key
            # TODO: support slicing methods for record_key
            record_key = [
                _record_key[i]  # type: ignore[index]
                for i in range(*key.indices(len(_record_key)))  # type: ignore[arg-type]
            ]
            df._record_key = ColumnSeries(record_key)
            self.obj.operations.append(UpdateData(df))

    def __delitem__(self, key: Union[int, slice]) -> None:
        if self.obj._root is not None:
            raise TypeError(
                "'iloc.__delitem__' is not supported for the DataFrame"
                "which is a member of another DataFrame"
            )

        self.obj._del_item_by_location(key)


class DataFrameLocIndexer:
    """Index class for DataFrame.loc."""

    def __init__(self, obj: "DataFrame") -> None:
        self.obj: "DataFrame" = obj

    # @overload
    # def __getitem__(self, key: Iterable[int]) -> "DataFrame":
    #     ...

    @overload
    def __getitem__(self, key: slice) -> "DataFrame":
        ...

    @overload
    def __getitem__(self, key: Tuple[Union[int, slice], str]) -> Any:
        ...

    @overload
    def __getitem__(self, key: int) -> "RowSeries":
        ...

    def __getitem__(
        self, key: Union[int, slice, Tuple[Union[int, slice], str]]
    ) -> Union[Any, "DataFrame", "RowSeries"]:
        if isinstance(key, tuple):
            index, name = key
            return self.obj[name].loc[index]

        if isinstance(key, int):
            return self.obj._get_item_by_location(key)

        return self.obj._get_slice_by_location(key)

    @overload
    def __setitem__(self, key: int, value: NestedDict[str, Any]) -> None:
        ...

    @overload
    def __setitem__(
        self, key: slice, value: Union[Iterable[NestedDict[str, Any]], "DataFrame"]
    ) -> None:
        ...

    @overload
    def __setitem__(self, key: Tuple[int, str], value: Any) -> None:
        ...

    @overload
    def __setitem__(self, key: Tuple[slice, str], value: Union[Iterable[Any], SeriesBase]) -> None:
        ...

    def __setitem__(
        self,
        key: Union[int, slice, Tuple[int, str], Tuple[slice, str]],
        value: Union[
            NestedDict[str, Any],
            "DataFrame",
            Iterable[NestedDict[str, Any]],
            Any,
            Iterable[Any],
            SeriesBase,
        ],
    ) -> None:
        if isinstance(key, tuple):
            index, name = key
            self.obj[name].iloc[index] = value
            return

        if isinstance(key, int):
            # pylint: disable=protected-access
            value = self.obj._from_pyarrow(
                self.obj._pylist_to_pyarrow([value], self.obj.schema),
                self.obj.schema,
            )
            key = slice(key, key + 1)
        elif not isinstance(value, self.obj.__class__):
            value = self.obj._from_pyarrow(
                self.obj._pylist_to_pyarrow(value, self.obj.schema),  # type: ignore[arg-type]
                self.obj.schema,
            )
        elif not self.obj.schema.to_pyarrow().equals(value.schema.to_pyarrow()):
            raise TypeError("The schema of the given DataFrame is mismatched")

        self.obj._set_slice_by_location(key, value)
        if self.obj.operations is not None:
            df = value.copy()
            _record_key = self.obj._record_key
            # TODO: support slicing methods for record_key
            record_key = [
                _record_key[i]  # type: ignore[index]
                for i in range(*key.indices(len(_record_key)))  # type: ignore[arg-type]
            ]
            df._record_key = ColumnSeries(record_key)
            self.obj.operations.append(UpdateData(df))

    def __delitem__(self, key: Union[int, slice]) -> None:
        if self.obj._root is not None:
            raise TypeError(
                "'loc.__delitem__' is not supported for the DataFrame"
                "which is a member of another DataFrame"
            )

        self.obj._del_item_by_location(key)
