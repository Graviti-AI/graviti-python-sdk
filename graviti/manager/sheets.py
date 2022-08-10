#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the Sheets."""

from functools import partial
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, MutableMapping

import pyarrow as pa
from tqdm.auto import tqdm

import graviti.portex as pt
from graviti.dataframe import RECORD_KEY, DataFrame
from graviti.exception import FieldNameConflictError
from graviti.manager.common import LIMIT
from graviti.operation import AddData, CreateSheet, DeleteSheet, SheetOperation
from graviti.paging import LazyLowerCaseFactory
from graviti.portex import PortexRecordBase
from graviti.utility import ReprMixin

if TYPE_CHECKING:
    from graviti.manager.dataset import Dataset


class Sheets(MutableMapping[str, DataFrame], ReprMixin):
    """The basic structure of the Graviti sheets."""

    _data: Dict[str, DataFrame]
    _dataset: "Dataset"
    operations: List[SheetOperation] = []

    def __len__(self) -> int:
        return self._get_data().__len__()

    def __getitem__(self, key: str) -> DataFrame:
        return self._get_data().__getitem__(key)

    def __setitem__(self, key: str, value: DataFrame) -> None:
        is_replace = False
        if key in self:
            is_replace = True

        self._get_data().__setitem__(key, value)
        if is_replace:
            self.operations.append(DeleteSheet(key))

        if value.operations is None:
            value.operations = [AddData(value.copy())]
        else:
            raise NotImplementedError(
                "Not support assigning one DataFrame to multiple sheets."
                " Please use method 'copy' first."
            )
        self.operations.append(CreateSheet(key, value.schema.copy()))

    def __delitem__(self, key: str) -> None:
        dataframe = self[key]
        self._get_data().__delitem__(key)
        del dataframe.operations
        self.operations.append(DeleteSheet(key))

    def __iter__(self) -> Iterator[str]:
        return self._get_data().__iter__()

    def _list_data(self, offset: int, limit: int, sheet_name: str) -> Dict[str, Any]:
        raise NotImplementedError

    def _list_sheets(self) -> Dict[str, Any]:
        raise NotImplementedError

    def _get_sheet(self, sheet_name: str) -> Dict[str, Any]:
        raise NotImplementedError

    def _init_dataframe(self, sheet_name: str) -> DataFrame:
        sheet = self._get_sheet(sheet_name)
        schema = PortexRecordBase.from_yaml(sheet["schema"])

        patype = schema.to_pyarrow()

        factory = LazyLowerCaseFactory(
            sheet["record_count"],
            LIMIT,
            partial(self._list_data, sheet_name=sheet_name),
            pa.struct([pa.field(RECORD_KEY, pa.string()), *patype]),
        )

        dataframe = DataFrame._from_factory(factory, schema)  # pylint: disable=protected-access
        dataframe.operations = []
        return dataframe

    def _init_data(self) -> None:
        self._data = {}
        sheets_info = self._list_sheets()["sheets"]

        for sheet_info in sheets_info:
            sheet_name = sheet_info["name"]
            self._data[sheet_name] = self._init_dataframe(sheet_name)

    def _get_data(self) -> Dict[str, DataFrame]:
        if not hasattr(self, "_data"):
            self._init_data()

        return self._data

    def _check_record_names(self, schema: PortexRecordBase, sheet_name: str) -> None:
        name_mapping: Dict[str, str] = {}
        for name, portex_type in schema.items():
            builtin_type = portex_type.to_builtin()
            if isinstance(builtin_type, pt.record):
                self._check_record_names(builtin_type, sheet_name)
            elif isinstance(builtin_type, pt.array):
                self._check_array_names(builtin_type, sheet_name)

            lower_name = name.lower()
            if lower_name in name_mapping:
                raise FieldNameConflictError(
                    f'In sheet "{sheet_name}", the field name "{name}" is conflict with '
                    f'"{name_mapping[lower_name]}", the Graviti data platform is case insensitive.'
                )

            name_mapping[lower_name] = name

    def _check_array_names(self, schema: pt.array, sheet_name: str) -> None:
        items = schema.items
        builtin_type = items.to_builtin()

        if isinstance(builtin_type, pt.record):
            self._check_record_names(builtin_type, sheet_name)
        elif isinstance(builtin_type, pt.array):
            self._check_array_names(builtin_type, sheet_name)

    def _upload_to_draft(self, draft_number: int, jobs: int, quiet: bool) -> None:
        """Upload the local dataset to Graviti.

        Arguments:
            draft_number: The number of the draft.
            jobs: The number of the max workers in multi-thread upload, the default is 8.
            quiet: Set to True to stop showing the upload process bar.

        """
        for sheet_name in {operation.sheet for operation in self.operations}:
            self._check_record_names(self[sheet_name].schema, sheet_name)

        dataset = self._dataset
        for sheet_operation in self.operations:
            sheet_operation.do(
                dataset.access_key,
                dataset.url,
                dataset.owner,
                dataset.name,
                draft_number=draft_number,
            )
        self.operations = []

        df_total = 0
        file_total = 0
        for dataframe in self.values():
            for df_operation in dataframe.operations:  # type: ignore[union-attr]
                df_total += df_operation.get_upload_count()
                file_total += sum(map(len, df_operation.get_file_arrays()))

        # Note that after done uploading, the two process bars will switch position due to the tqdm
        # bug https://github.com/tqdm/tqdm/issues/1000.
        with tqdm(
            total=file_total, disable=(file_total == 0 or quiet), desc="uploading binary files"
        ) as file_pbar:
            with tqdm(total=df_total, disable=quiet, desc="uploading structured data") as data_pbar:
                for sheet_name, dataframe in self.items():
                    for df_operation in dataframe.operations:  # type: ignore[union-attr]
                        df_operation.do(
                            dataset.access_key,
                            dataset.url,
                            dataset.owner,
                            dataset.name,
                            draft_number=draft_number,
                            sheet=sheet_name,
                            jobs=jobs,
                            data_pbar=data_pbar,
                            file_pbar=file_pbar,
                        )
                        dataframe.operations = []
