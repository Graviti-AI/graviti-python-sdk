#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Definitions of different operations on a DataFrame."""

from typing import TYPE_CHECKING, Iterable, List

from tqdm import tqdm

from graviti.file import File, FileBase
from graviti.openapi import RECORD_KEY, add_data, delete_data, update_data, update_schema
from graviti.operation.common import get_schema
from graviti.portex import record
from graviti.utility import submit_multithread_tasks

if TYPE_CHECKING:
    from graviti.dataframe import DataFrame
    from graviti.dataframe.column.series import NumberSeries
    from graviti.manager import ObjectPermissionManager

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

    def do(  # pylint: disable=invalid-name
        self,
        access_key: str,
        url: str,
        owner: str,
        dataset: str,
        *,
        draft_number: int,
        sheet: str,
        jobs: int,
        data_pbar: tqdm,
        file_pbar: tqdm,
        object_permission_manager: "ObjectPermissionManager",
    ) -> None:
        """Execute the OpenAPI create sheet.

        Arguments:
            access_key: User's access key.
            url: The URL of the graviti website.
            owner: The owner of the dataset.
            dataset: Name of the dataset, unique for a user.
            draft_number: The draft number.
            sheet: The sheet name.
            jobs: The number of the max workers in multi-thread operation.
            data_pbar: The process bar for uploading structured data.
            file_pbar: The process bar for uploading binary files.
            object_permission_manager: The object permission manager of the dataset.

        Raises:
            NotImplementedError: The method of the base class should not be called.

        """
        raise NotImplementedError


class DataOperation(DataFrameOperation):  # pylint: disable=abstract-method
    """This class defines the basic method of the data operation on a DataFrame."""

    _files: List[FileBase]

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
        return len(list(self._data._generate_file()))  # pylint: disable=protected-access

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

    def do(  # pylint: disable=invalid-name
        self,
        access_key: str,
        url: str,
        owner: str,
        dataset: str,
        *,
        draft_number: int,
        sheet: str,
        jobs: int,
        data_pbar: tqdm,
        file_pbar: tqdm,
        object_permission_manager: "ObjectPermissionManager",
    ) -> None:
        """Execute the OpenAPI add data.

        Arguments:
            access_key: User's access key.
            url: The URL of the graviti website.
            owner: The owner of the dataset.
            dataset: Name of the dataset, unique for a user.
            draft_number: The draft number.
            sheet: The sheet name.
            jobs: The number of the max workers in multi-thread operation.
            data_pbar: The process bar for uploading structured data.
            file_pbar: The process bar for uploading binary files.
            object_permission_manager: The object permission manager of the dataset.

        """
        batch_size = self._get_max_batch_size()
        df = self._data

        for i in range(0, len(df), batch_size):
            batch = df.iloc[i : i + batch_size]

            # pylint: disable=protected-access
            _upload_files(
                filter(
                    lambda f: isinstance(f, File), batch._generate_file()  # type: ignore[arg-type]
                ),
                object_permission_manager,
                jobs=jobs,
                pbar=file_pbar,
            )

            add_data(
                access_key,
                url,
                owner,
                dataset,
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

    def do(  # pylint: disable=invalid-name, unused-argument, too-many-locals
        self,
        access_key: str,
        url: str,
        owner: str,
        dataset: str,
        *,
        draft_number: int,
        sheet: str,
        jobs: int,
        data_pbar: tqdm,
        file_pbar: tqdm,
        object_permission_manager: "ObjectPermissionManager",
    ) -> None:
        """Execute the OpenAPI update schema.

        Arguments:
            access_key: User's access key.
            url: The URL of the graviti website.
            owner: The owner of the dataset.
            dataset: Name of the dataset, unique for a user.
            draft_number: The draft number.
            sheet: The sheet name.
            jobs: The number of the max workers in multi-thread operation.
            data_pbar: The process bar for uploading structured data.
            file_pbar: The process bar for uploading binary files.
            object_permission_manager: The object permission manager of the dataset.

        """
        portex_schema, avro_schema, arrow_schema = get_schema(self.schema)

        # Request data between "Update Schema" and "Update Data" will make Graviti backend report
        # error. So here to move the "request data" before "UpdateSchema" to bypass this issue.
        record_keys: "NumberSeries" = self._data[RECORD_KEY]  # type: ignore[assignment]
        for page in record_keys._data._pages:  # pylint: disable=protected-access
            page.get_array()

        update_schema(
            access_key,
            url,
            owner,
            dataset,
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

    def do(  # pylint: disable=invalid-name
        self,
        access_key: str,
        url: str,
        owner: str,
        dataset: str,
        *,
        draft_number: int,
        sheet: str,
        jobs: int,
        data_pbar: tqdm,
        file_pbar: tqdm,
        object_permission_manager: "ObjectPermissionManager",
    ) -> None:
        """Execute the OpenAPI add data.

        Arguments:
            access_key: User's access key.
            url: The URL of the graviti website.
            owner: The owner of the dataset.
            dataset: Name of the dataset, unique for a user.
            draft_number: The draft number.
            sheet: The sheet name.
            jobs: The number of the max workers in multi-thread operation.
            data_pbar: The process bar for uploading structured data.
            file_pbar: The process bar for uploading binary files.
            object_permission_manager: The object permission manager of the dataset.

        """
        batch_size = self._get_max_batch_size()
        df = self._data

        for i in range(0, len(df), batch_size):
            batch = df.iloc[i : i + batch_size]

            # pylint: disable=protected-access
            _upload_files(
                filter(
                    lambda f: isinstance(f, File), batch._generate_file()  # type: ignore[arg-type]
                ),
                object_permission_manager,
                jobs=jobs,
                pbar=file_pbar,
            )

            update_data(
                access_key,
                url,
                owner,
                dataset,
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

    def do(  # pylint: disable=invalid-name, unused-argument
        self,
        access_key: str,
        url: str,
        owner: str,
        dataset: str,
        *,
        draft_number: int,
        sheet: str,
        jobs: int,
        data_pbar: tqdm,
        file_pbar: tqdm,
        object_permission_manager: "ObjectPermissionManager",
    ) -> None:
        """Execute the OpenAPI delete data.

        Arguments:
            access_key: User's access key.
            url: The URL of the graviti website.
            owner: The owner of the dataset.
            dataset: Name of the dataset, unique for a user.
            draft_number: The draft number.
            sheet: The sheet name.
            jobs: The number of the max workers in multi-thread operation.
            data_pbar: The process bar for uploading structured data.
            file_pbar: The process bar for uploading binary files.
            object_permission_manager: The object permission manager of the dataset.

        """
        delete_data(
            access_key,
            url,
            owner,
            dataset,
            draft_number=draft_number,
            sheet=sheet,
            record_keys=self.record_keys,
        )


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
