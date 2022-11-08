#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Definitions of different operations on a DataFrame."""

from typing import TYPE_CHECKING, Dict, Iterable, List, Tuple

from tqdm import tqdm

from graviti.exception import ObjectCopyError
from graviti.file import File, FileBase, RemoteFile
from graviti.openapi import (
    RECORD_KEY,
    add_data,
    copy_objects,
    delete_data,
    update_data,
    update_schema,
)
from graviti.operation.common import get_schema
from graviti.portex import record
from graviti.utility import chunked, submit_multithread_tasks

if TYPE_CHECKING:
    from graviti.dataframe import DataFrame
    from graviti.dataframe.column.series import NumberSeries
    from graviti.manager import Dataset, ObjectPermissionManager

_MAX_BATCH_SIZE = 2048
_MAX_ITEMS = 60000


class DataFrameOperation:
    """This class defines the basic method of the operation on a DataFrame."""

    def get_file_count(self) -> int:  # pylint: disable=no-self-use
        """Get the file amount to be uploaded.

        Returns:
            The file amount to be uploaded.

        """
        return 0

    def get_data_count(self) -> int:  # pylint: disable=no-self-use
        """Get the data amount to be uploaded.

        Returns:
            The data amount to be uploaded.

        """
        return 0

    def execute(
        self,
        dataset: "Dataset",
        *,
        draft_number: int,
        sheet: str,
        jobs: int,
        data_pbar: tqdm,
        file_pbar: tqdm,
    ) -> None:
        """Execute the OpenAPI create sheet.

        Arguments:
            dataset: The Dataset instance.
            draft_number: The draft number.
            sheet: The sheet name.
            jobs: The number of the max workers in multi-thread operation.
            data_pbar: The process bar for uploading structured data.
            file_pbar: The process bar for uploading binary files.

        Raises:
            NotImplementedError: The method of the base class should not be called.

        """
        raise NotImplementedError


class DataOperation(DataFrameOperation):  # pylint: disable=abstract-method
    """This class defines the basic method of the data operation on a DataFrame."""

    def __init__(self, data: "DataFrame") -> None:
        self._data = data

    def _get_max_batch_size(self) -> int:
        return min(
            _MAX_BATCH_SIZE,
            _MAX_ITEMS // self._data.schema._get_column_count(),  # pylint: disable=protected-access
        )

    def get_file_count(self) -> int:
        """Get the file amount to be uploaded.

        Returns:
            The file amount to be uploaded.

        """
        return sum(map(len, self._data._generate_file_series()))  # pylint: disable=protected-access

    def get_data_count(self) -> int:
        """Get the data amount to be uploaded.

        Returns:
            The data amount to be uploaded.

        """
        return len(self._data)


class AddData(DataOperation):
    """This class defines the operation that add data to a DataFrame.

    Arguments:
        data: The data to be added.

    """

    def execute(
        self,
        dataset: "Dataset",
        *,
        draft_number: int,
        sheet: str,
        jobs: int,
        data_pbar: tqdm,
        file_pbar: tqdm,
    ) -> None:
        """Execute the OpenAPI add data.

        Arguments:
            dataset: The Dataset instance.
            draft_number: The draft number.
            sheet: The sheet name.
            jobs: The number of the max workers in multi-thread operation.
            data_pbar: The process bar for uploading structured data.
            file_pbar: The process bar for uploading binary files.

        """
        batch_size = self._get_max_batch_size()
        df = self._data

        for i in range(0, len(df), batch_size):
            batch = df.iloc[i : i + batch_size]

            # pylint: disable=protected-access
            local_files, remote_files = _separate_files(batch._generate_file(), dataset)
            if remote_files:
                _copy_files(dataset, remote_files, file_pbar)

            if local_files:
                _upload_files(local_files, dataset.object_permission_manager, file_pbar, jobs)

            _workspace = dataset.workspace
            add_data(
                _workspace.access_key,
                _workspace.url,
                _workspace.name,
                dataset.name,
                draft_number=draft_number,
                sheet=sheet,
                data=batch.to_pylist(_to_backend=True),
            )
            data_pbar.update(len(batch))


class UpdateSchema(DataFrameOperation):
    """This class defines the operation that update the schema of a DataFrame.

    Arguments:
        schema: New portex schema after updated.

    """

    def __init__(self, schema: record, data: "DataFrame") -> None:
        self._data = data
        self.schema = schema

    def execute(  # pylint: disable=unused-argument
        self,
        dataset: "Dataset",
        *,
        draft_number: int,
        sheet: str,
        jobs: int,
        data_pbar: tqdm,
        file_pbar: tqdm,
    ) -> None:
        """Execute the OpenAPI update schema.

        Arguments:
            dataset: The Dataset instance.
            draft_number: The draft number.
            sheet: The sheet name.
            jobs: The number of the max workers in multi-thread operation.
            data_pbar: The process bar for uploading structured data.
            file_pbar: The process bar for uploading binary files.

        """
        portex_schema, avro_schema, arrow_schema = get_schema(self.schema)

        # Request data between "Update Schema" and "Update Data" will make Graviti backend report
        # error. So here to move the "request data" before "UpdateSchema" to bypass this issue.
        record_keys: "NumberSeries" = self._data[RECORD_KEY]  # type: ignore[assignment]
        for page in record_keys._data._pages:  # pylint: disable=protected-access
            page.get_array()

        _workspace = dataset.workspace
        update_schema(
            _workspace.access_key,
            _workspace.url,
            _workspace.name,
            dataset.name,
            draft_number=draft_number,
            sheet=sheet,
            _schema=portex_schema,
            _avro_schema=avro_schema,
            _arrow_schema=arrow_schema,
        )


class UpdateData(DataOperation):
    """This class defines the operation that updates the data of a DataFrame.

    Arguments:
        data: The data for updating.

    """

    def execute(
        self,
        dataset: "Dataset",
        *,
        draft_number: int,
        sheet: str,
        jobs: int,
        data_pbar: tqdm,
        file_pbar: tqdm,
    ) -> None:
        """Execute the OpenAPI add data.

        Arguments:
            dataset: The Dataset instance.
            draft_number: The draft number.
            sheet: The sheet name.
            jobs: The number of the max workers in multi-thread operation.
            data_pbar: The process bar for uploading structured data.
            file_pbar: The process bar for uploading binary files.

        """
        batch_size = self._get_max_batch_size()
        df = self._data

        for i in range(0, len(df), batch_size):
            batch = df.iloc[i : i + batch_size]

            # pylint: disable=protected-access
            local_files, remote_files = _separate_files(batch._generate_file(), dataset)
            if remote_files:
                _copy_files(dataset, remote_files, file_pbar)

            if local_files:
                _upload_files(local_files, dataset.object_permission_manager, file_pbar, jobs)

            _workspace = dataset.workspace
            update_data(
                _workspace.access_key,
                _workspace.url,
                _workspace.name,
                dataset.name,
                draft_number=draft_number,
                sheet=sheet,
                data=batch.to_pylist(_to_backend=True),
            )
            data_pbar.update(len(batch))


class DeleteData(DataFrameOperation):
    """This class defines the operation that delete the data of a DataFrame.

    Arguments:
        record_keys: The record keys of the data to be deleted.

    """

    def __init__(self, record_keys: List[str]) -> None:
        self.record_keys = record_keys

    def execute(  # pylint: disable=unused-argument
        self,
        dataset: "Dataset",
        *,
        draft_number: int,
        sheet: str,
        jobs: int,
        data_pbar: tqdm,
        file_pbar: tqdm,
    ) -> None:
        """Execute the OpenAPI delete data.

        Arguments:
            dataset: The Dataset instance.
            draft_number: The draft number.
            sheet: The sheet name.
            jobs: The number of the max workers in multi-thread operation.
            data_pbar: The process bar for uploading structured data.
            file_pbar: The process bar for uploading binary files.

        """
        _workspace = dataset.workspace
        delete_data(
            _workspace.access_key,
            _workspace.url,
            _workspace.name,
            dataset.name,
            draft_number=draft_number,
            sheet=sheet,
            record_keys=self.record_keys,
        )


def _copy_files(
    dataset: "Dataset",
    remote_files: Dict[str, List[RemoteFile]],
    pbar: tqdm,
) -> None:
    _workspace = dataset.workspace
    for source_dataset, files in remote_files.items():
        for batch in chunked(files, _MAX_BATCH_SIZE):
            keys = copy_objects(
                _workspace.access_key,
                _workspace.url,
                _workspace.name,
                dataset.name,
                source_dataset=source_dataset,
                keys=[file.key for file in batch],
            )["keys"]
            for file, key in zip(batch, keys):
                file._post_key = key  # pylint: disable=protected-access

            pbar.update(len(batch))


def _upload_files(
    files: Iterable[File],
    object_permission_manager: "ObjectPermissionManager",
    pbar: tqdm,
    jobs: int = 8,
) -> None:
    submit_multithread_tasks(
        lambda file: _upload_file(file, object_permission_manager, pbar),
        files,
        jobs=jobs,
    )


def _upload_file(
    file: File,
    object_permission_manager: "ObjectPermissionManager",
    pbar: tqdm,
) -> None:
    post_key = f"{object_permission_manager.prefix}{file.get_checksum()}"
    object_permission_manager.put_object(post_key, file.path)
    file._post_key = post_key  # pylint: disable=protected-access
    pbar.update()


def _separate_files(
    files: Iterable[FileBase], target_dataset: "Dataset"
) -> Tuple[List[File], Dict[str, List[RemoteFile]]]:
    local_files: List[File] = []
    remote_files: Dict[str, List[RemoteFile]] = {}

    for file in files:
        # pylint: disable=protected-access
        if isinstance(file, File):
            local_files.append(file)

        elif isinstance(file, RemoteFile):
            source_dataset = file._object_permission._dataset
            if source_dataset._id == target_dataset._id:
                continue

            try:
                remote_files[source_dataset.name].append(file)
            except KeyError:
                if source_dataset.workspace._id != target_dataset.workspace._id:
                    raise ObjectCopyError(
                        "It is not allowed to copy object between diffenent workspaces.\n"
                        "  Source:\n"
                        f"    workspace: {source_dataset.workspace.name},\n"
                        f"    dataset: {source_dataset.name},\n"
                        f"    object key: {file.key}"
                        "  Target:\n"
                        f"    workspace: {target_dataset.workspace.name},\n"
                        f"    dataset: {target_dataset.name},\n"
                    ) from None

                if source_dataset.storage_config.name != target_dataset.storage_config.name:
                    raise ObjectCopyError(
                        "It is not allowed to copy object between datasets "
                        "with different storage configs.\n"
                        "  Source:\n"
                        f"    workspace: {source_dataset.workspace.name},\n"
                        f"    dataset: {source_dataset.name},\n"
                        f"    storage config: {source_dataset.storage_config.name},\n"
                        f"    object key: {file.key}"
                        "  Target:\n"
                        f"    workspace: {target_dataset.workspace.name},\n"
                        f"    dataset: {target_dataset.name},\n"
                        f"    storage config: {target_dataset.storage_config.name}\n"
                    ) from None

                remote_files[source_dataset.name] = [file]

        else:
            raise TypeError("The file instance is neither 'File' nor 'RemoteFile'")

    return local_files, remote_files
