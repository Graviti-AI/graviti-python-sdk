#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Definitions of different operations on a DataFrame."""

from typing import TYPE_CHECKING, List

from tqdm import tqdm

from graviti.openapi import add_data, update_data, update_schema, upload_files
from graviti.operation.common import get_schema
from graviti.portex import PortexType
from graviti.utility import File, chunked

if TYPE_CHECKING:
    from graviti.dataframe import DataFrame, FileSeries

_MAX_BATCH_SIZE = 2048


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

        Raises:
            NotImplementedError: The method of the base class should not be called.

        """
        raise NotImplementedError


class DataOperation(DataFrameOperation):  # pylint: disable=abstract-method
    """This class defines the basic method of the data operation on a DataFrame."""

    _file_arrays: List["FileSeries"]

    def __init__(self, data: "DataFrame") -> None:
        self._data = data

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

        """
        data = self._data.to_pylist()

        for batch, *file_arrays in zip(
            *map(
                lambda x: chunked(x, _MAX_BATCH_SIZE),  # type: ignore[arg-type]
                (data, *self.get_file_arrays()),
            )
        ):
            for file_array in file_arrays:
                local_files = filter(lambda x: isinstance(x, File), file_array)
                upload_files(
                    access_key,
                    url,
                    owner,
                    dataset,
                    draft_number=draft_number,
                    sheet=sheet,
                    files=local_files,
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

        """
        data = self._data._to_patch_data()  # pylint: disable=protected-access

        for batch, *file_arrays in zip(
            *map(
                lambda x: chunked(x, _MAX_BATCH_SIZE),  # type: ignore[arg-type]
                (data, *self.get_file_arrays()),
            )
        ):
            for file_array in file_arrays:
                local_files = filter(lambda x: isinstance(x, File), file_array)
                upload_files(
                    access_key,
                    url,
                    owner,
                    dataset,
                    draft_number=draft_number,
                    sheet=sheet,
                    files=local_files,
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
