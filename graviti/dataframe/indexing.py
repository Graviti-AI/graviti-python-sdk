#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the Graviti indexing related class."""

from typing import TYPE_CHECKING, Any, Iterable, Union, overload

from graviti.utility import NestedDict

if TYPE_CHECKING:
    from graviti.dataframe import DataFrame
    from graviti.dataframe.row.series import Series as RowSeries


class DataFrameILocIndexer:
    """Index class for DataFrame.iloc."""

    def __init__(self, obj: "DataFrame") -> None:
        self.obj = obj

    # @overload
    # def __getitem__(self, key: Union[Tuple[int, str], Iterable[bool], slice]) -> Any:
    #     ...

    # @overload
    # def __getitem__(self, key: int) -> "RowSeries":
    #     ...

    # @overload
    # def __getitem__(self, key: Iterable[int]) -> "DataFrame":
    #     ...

    def __getitem__(self, key: int) -> "RowSeries":
        return self.obj._getitem_by_location(key)

    @overload
    def __setitem__(self, key: int, value: NestedDict[str, Any]) -> None:
        ...

    @overload
    def __setitem__(
        self, key: slice, value: Union[Iterable[NestedDict[str, Any]], "DataFrame"]
    ) -> None:
        ...

    def __setitem__(
        self,
        key: Union[int, slice],
        value: Union[NestedDict[str, Any], "DataFrame", Iterable[NestedDict[str, Any]]],
    ) -> None:
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

        self.obj._set_item_by_slice(key, value)


class DataFrameLocIndexer:
    """Index class for DataFrame.loc."""

    def __init__(self, obj: "DataFrame") -> None:
        self.obj: "DataFrame" = obj

    # @overload
    # def __getitem__(self, key: Union[Tuple[int, str], Iterable[bool], slice]) -> Any:
    #     ...

    # @overload
    # def __getitem__(self, key: int) -> "RowSeries":
    #     ...

    # @overload
    # def __getitem__(self, key: Iterable[int]) -> "DataFrame":
    #     ...

    def __getitem__(self, key: int) -> "RowSeries":
        return self.obj._getitem_by_location(key)

    @overload
    def __setitem__(self, key: int, value: NestedDict[str, Any]) -> None:
        ...

    @overload
    def __setitem__(
        self, key: slice, value: Union[Iterable[NestedDict[str, Any]], "DataFrame"]
    ) -> None:
        ...

    def __setitem__(
        self,
        key: Union[int, slice],
        value: Union[NestedDict[str, Any], "DataFrame", Iterable[NestedDict[str, Any]]],
    ) -> None:
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

        self.obj._set_item_by_slice(key, value)
