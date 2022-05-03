#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the Sheets."""

from functools import partial
from typing import TYPE_CHECKING, Any, Dict, Iterator, MutableMapping

from graviti.dataframe import DataFrame
from graviti.portex import PortexType
from graviti.utility import LazyFactory, ReprMixin

if TYPE_CHECKING:
    from graviti.manager.dataset import Dataset


class Sheets(MutableMapping[str, DataFrame], ReprMixin):
    """The basic structure of the Graviti sheets."""

    _data: Dict[str, DataFrame]
    _dataset: "Dataset"

    def __len__(self) -> int:
        return self._get_data().__len__()

    def __getitem__(self, key: str) -> DataFrame:
        return self._get_data().__getitem__(key)

    def __setitem__(self, key: str, value: DataFrame) -> None:
        self._get_data().__setitem__(key, value)

    def __delitem__(self, key: str) -> None:
        self._get_data().__delitem__(key)

    def __iter__(self) -> Iterator[str]:
        return self._get_data().__iter__()

    def _list_data(self, offset: int, limit: int, sheet_name: str) -> Dict[str, Any]:
        raise NotImplementedError

    def _list_sheets(self) -> Dict[str, Any]:
        raise NotImplementedError

    def _get_sheet(self, sheet_name: str) -> Dict[str, Any]:
        raise NotImplementedError

    def _init_data(self) -> None:
        self._data = {}
        sheets_info = self._list_sheets()["sheets"]

        for sheet_info in sheets_info:
            sheet_name = sheet_info["name"]
            sheet = self._get_sheet(sheet_name)
            schema = PortexType.from_yaml(sheet["schema"])

            factory = LazyFactory(
                sheet["record_count"],
                128,
                partial(self._list_data, sheet_name=sheet_name),
                schema.to_pyarrow(),
            )
            paging_lists = factory.create_lists(schema.get_keys())
            self._data[sheet_name] = DataFrame._from_paging(  # pylint: disable=protected-access
                paging_lists, schema
            )

    def _get_data(self) -> Dict[str, DataFrame]:
        if not hasattr(self, "_data"):
            self._init_data()

        return self._data
