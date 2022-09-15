#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Definitions of different operations on a DataFrame."""

from typing import TYPE_CHECKING, Iterable, List

from tqdm import tqdm

from graviti.file import File, FileBase
from graviti.openapi import add_data, delete_data, update_data, update_schema
from graviti.operation.common import get_schema
from graviti.portex import PortexType
from graviti.utility import chunked, submit_multithread_tasks

if TYPE_CHECKING:
    from graviti.dataframe import DataFrame
    from graviti.manager import ObjectPolicyManager

_MAX_BATCH_SIZE = 2048
_MAX_ITEMS = 60000


class DataFrameOperation:
    """This class defines the basic method of the operation on a DataFrame."""

    def get_files(self) -> List[FileBase]:  # pylint: disable=no-self-use
        """Get the list of FileSeries.

        Returns:
            The list of FileSeries.

        """
        return []

    def get_upload_count(self) -> int:  # pylint: disable=no-self-use
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
        object_policy_manager: "ObjectPolicyManager",
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
            object_policy_manager: The object policy manager of the dataset.

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

    def get_files(self) -> List[FileBase]:
        """Get the list of FileSeries.

        Returns:
            The list of FileSeries.

        """
        if not hasattr(self, "_files"):
            self._files = list(self._data._generate_file())  # pylint: disable=protected-access

        return self._files

    def get_upload_count(self) -> int:
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
        object_policy_manager: "ObjectPolicyManager",
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
            object_policy_manager: The object policy manager of the dataset.

        """
        _upload_files(
            filter(lambda f: isinstance(f, File), self.get_files()),  # type: ignore[arg-type]
            object_policy_manager,
            jobs=jobs,
            pbar=file_pbar,
        )

        for batch in chunked(
            self._data._to_post_data(),  # pylint: disable=protected-access
            self._get_max_batch_size(),
        ):
            add_data(
                access_key,
                url,
                owner,
                dataset,
                draft_number=draft_number,
                sheet=sheet,
                data=batch,
            )
            data_pbar.update(len(batch))


class UpdateSchema(DataFrameOperation):
    """This class defines the operation that update the schema of a DataFrame.

    Arguments:
        schema: New portex schema after updated.

    """

    def __init__(self, schema: PortexType) -> None:
        self.schema = schema

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
        object_policy_manager: "ObjectPolicyManager",
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
            object_policy_manager: The object policy manager of the dataset.

        """
        portex_schema, avro_schema, arrow_schema = get_schema(self.schema)

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
        object_policy_manager: "ObjectPolicyManager",
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
            object_policy_manager: The object policy manager of the dataset.

        """
        _upload_files(
            filter(lambda f: isinstance(f, File), self.get_files()),  # type: ignore[arg-type]
            object_policy_manager,
            jobs=jobs,
            pbar=file_pbar,
        )

        for batch in chunked(
            self._data._to_patch_data(),  # pylint: disable=protected-access
            self._get_max_batch_size(),
        ):
            update_data(
                access_key,
                url,
                owner,
                dataset,
                draft_number=draft_number,
                sheet=sheet,
                data=batch,
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
        object_policy_manager: "ObjectPolicyManager",
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
            object_policy_manager: The object policy manager of the dataset.

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
    object_policy_manager: "ObjectPolicyManager",
    pbar: tqdm,
    _allow_retry: bool = True,
    jobs: int = 8,
) -> None:
    submit_multithread_tasks(
        lambda file: _upload_file(file, object_policy_manager, pbar, _allow_retry),
        files,
        jobs=jobs,
    )


def _upload_file(
    file: File,
    object_policy_manager: "ObjectPolicyManager",
    pbar: tqdm,
    _allow_retry: bool = True,
) -> None:
    post_key = f"{object_policy_manager.prefix}{file.get_checksum()}"
    object_policy_manager.put_object(post_key, file.path, _allow_retry)
    file._post_key = post_key  # pylint: disable=protected-access
    pbar.update()
