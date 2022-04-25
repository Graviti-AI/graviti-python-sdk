#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the Sheets."""

from typing import Any, Dict, Iterator, MutableMapping

from tensorbay.dataset import Notes, RemoteData
from tensorbay.label import Catalog
from tensorbay.utility import URL

from graviti.client import get_catalog, get_notes, list_data_details, list_segments
from graviti.dataframe import DataFrame
from graviti.portex import Extractors, catalog_to_schema, get_extractors
from graviti.utility import LazyFactory, LazyList, NestedDict

LazyLists = NestedDict[str, LazyList[Any]]


class Sheets(MutableMapping[str, DataFrame]):
    """The basic structure of the Graviti sheets."""

    _data: Dict[str, DataFrame]
    _dataset_id: str
    access_key: str
    url: str
    commit_id: str

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

    def _get_lazy_lists(self, factory: LazyFactory, extractors: Extractors) -> LazyLists:
        lazy_lists: LazyLists = {}
        for key, arguments in extractors.items():
            if isinstance(arguments, tuple):
                lazy_lists[key] = factory.create_list(*arguments)
            else:
                lazy_lists[key] = self._get_lazy_lists(factory, arguments)
        return lazy_lists

    def _init_data(self) -> None:
        self._data = {}
        response = list_segments(
            self.url,
            self.access_key,
            self._dataset_id,
            commit=self.commit_id,
        )
        for sheet in response["segments"]:
            sheet_name = sheet["name"]
            data_details = list_data_details(
                self.url,
                self.access_key,
                self._dataset_id,
                sheet_name,
                commit=self.commit_id,
            )

            def factory_getter(
                offset: int, limit: int, sheet_name: str = sheet_name
            ) -> Dict[str, Any]:
                return list_data_details(
                    self.url,
                    self.access_key,
                    self._dataset_id,
                    sheet_name,
                    commit=self.commit_id,
                    offset=offset,
                    limit=limit,
                )

            factory = LazyFactory(
                data_details["totalCount"],
                128,
                factory_getter,
            )
            catalog = get_catalog(
                self.url,
                self.access_key,
                self._dataset_id,
                commit=self.commit_id,
            )

            first_data_details = data_details["dataDetails"][0]
            remote_data = RemoteData.from_response_body(
                first_data_details,
                url=URL(
                    first_data_details["url"], updater=lambda: "update is not supported currently"
                ),
            )
            notes = get_notes(
                self.url,
                self.access_key,
                self._dataset_id,
                commit=self.commit_id,
            )

            schema = catalog_to_schema(
                Catalog.loads(catalog["catalog"]), remote_data, Notes.loads(notes)
            )
            lazy_lists = self._get_lazy_lists(factory, get_extractors(schema))
            self._data[sheet_name] = DataFrame.from_lazy_lists(lazy_lists)

    def _get_data(self) -> Dict[str, DataFrame]:
        if not hasattr(self, "_data"):
            self._init_data()

        return self._data
