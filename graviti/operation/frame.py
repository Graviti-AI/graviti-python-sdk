#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Definitions of different operations on a DataFrame."""

from pathlib import Path
from typing import TYPE_CHECKING, Iterable, List, Optional, Tuple

from tqdm import tqdm

from graviti.file import File
from graviti.openapi import add_data, delete_data, update_data, update_schema
from graviti.operation.common import get_schema
from graviti.portex import PortexType, RemoteFileTypeResgister
from graviti.utility import chunked, submit_multithread_tasks

if TYPE_CHECKING:
    from graviti.dataframe import DataFrame, FileSeries
    from graviti.manager import ObjectPolicyManager

_MAX_BATCH_SIZE = 2048
_MAX_ITEMS = 60000


class DataFrameOperation:
    """This class defines the basic method of the operation on a DataFrame."""

    def get_file_arrays(self) -> List["FileSeries"]:  # pylint: disable=no-self-use
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

    _file_arrays: List["FileSeries"]

    def __init__(self, data: "DataFrame") -> None:
        self._data = data

    def _get_max_batch_size(self) -> int:
        return min(
            _MAX_BATCH_SIZE,
            _MAX_ITEMS // self._data.schema._get_column_count(),  # pylint: disable=protected-access
        )

    def _update_local_files(
        self,
        object_policy_manager: "ObjectPolicyManager",
    ) -> List[List[Optional[Tuple[str, Path]]]]:
        # object_policy_manager = self._data.object_policy_manager
        if not object_policy_manager:
            raise ValueError("Require object policy manager to upload data")

        prefix = object_policy_manager.prefix

        keys_and_paths = []
        for file_array in self.get_file_arrays():
            schema = file_array.schema
            file_type = RemoteFileTypeResgister.SCHEMA_TO_REMOTE_FILE[
                schema.package.repo, schema.__class__.__name__  # type: ignore[index]
            ]
            key_and_path: List[Optional[Tuple[str, Path]]] = []
            # pylint: disable=protected-access
            for index, file in enumerate(file_array._data):
                if isinstance(file, File):
                    key = f"{prefix}{file.get_checksum()}"
                    key_and_path.append((key, file.path))

                    post_data = file._to_post_data()
                    post_data["key"] = key
                    file_array[index] = file_type(
                        **post_data,  # type: ignore[arg-type]
                        object_policy_manager=object_policy_manager,
                    )
                else:
                    key_and_path.append(None)
            keys_and_paths.append(key_and_path)
        return keys_and_paths

    def get_file_arrays(self) -> List["FileSeries"]:
        """Get the list of FileSeries.

        Returns:
            The list of FileSeries.

        """
        if not hasattr(self, "_file_arrays"):
            self._file_arrays = self._data._get_file_columns()  # pylint: disable=protected-access

        return self._file_arrays

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
        keys_and_paths = self._update_local_files(object_policy_manager)
        data = self._data._to_post_data()  # pylint: disable=protected-access
        for batch, *keys_and_paths in zip(
            *map(
                # TODO: support dataframe slicing methods to replace chunked
                lambda x: chunked(x, self._get_max_batch_size()),  # type: ignore[arg-type]
                (data, *keys_and_paths),
            )
        ):
            for key_and_path in keys_and_paths:
                _upload_files(
                    filter(lambda x: x is not None, key_and_path),  # type: ignore[arg-type]
                    object_policy_manager,
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

    _file_arrays: List["FileSeries"]

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
        keys_and_paths = self._update_local_files(object_policy_manager)
        data = self._data._to_patch_data()  # pylint: disable=protected-access

        for batch, *keys_and_paths in zip(
            *map(
                lambda x: chunked(x, self._get_max_batch_size()),  # type: ignore[arg-type]
                (data, *keys_and_paths),
            )
        ):
            for key_and_path in keys_and_paths:
                _upload_files(
                    filter(lambda x: x is not None, key_and_path),  # type: ignore[arg-type]
                    object_policy_manager,
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
    keys_and_paths: Iterable[Tuple[str, Path]],
    object_policy_manager: "ObjectPolicyManager",
    pbar: tqdm,
    _allow_retry: bool = True,
    jobs: int = 8,
) -> None:
    submit_multithread_tasks(
        lambda key_and_path: _upload_file(
            *key_and_path,
            object_policy_manager,
            pbar,
            _allow_retry,
        ),
        keys_and_paths,
        jobs=jobs,
    )


def _upload_file(
    key: str,
    path: Path,
    object_policy_manager: "ObjectPolicyManager",
    pbar: tqdm,
    _allow_retry: bool = True,
) -> None:
    object_policy_manager.put_object(key, path, _allow_retry)
    pbar.update()
