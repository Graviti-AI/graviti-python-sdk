#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Definitions of different operations on a DataFrame."""

from typing import TYPE_CHECKING, List

from graviti.openapi import add_data, update_data, upload_files
from graviti.utility import chunked

if TYPE_CHECKING:
    from graviti.dataframe import Container, DataFrame

_MAX_BATCH_SIZE = 2048


class DataFrameOperation:
    """This class defines the basic method of the operation on a DataFrame."""

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

        Raises:
            NotImplementedError: The method of the base class should not be called.

        """
        raise NotImplementedError


class AddData(DataFrameOperation):
    """This class defines the operation that add data to a DataFrame.

    Arguments:
        data: The data to be added.

    """

    def __init__(self, data: "DataFrame") -> None:
        self.data = data

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

        """
        dataframe = self.data
        arrays = _get_arrays(dataframe, "file.RemoteFile")
        data = dataframe.to_pylist()

        for batch, *file_arrays in zip(
            *map(lambda x: chunked(x, _MAX_BATCH_SIZE), (data, *arrays))  # type: ignore[arg-type]
        ):
            for file_array in file_arrays:
                upload_files(
                    access_key,
                    url,
                    owner,
                    dataset,
                    draft_number=draft_number,
                    sheet=sheet,
                    files=file_array,
                    jobs=jobs,
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


class UpdateData(DataFrameOperation):
    """This class defines the operation that updates the data of a DataFrame.

    Arguments:
        data: The data for updating.

    """

    def __init__(self, data: "DataFrame") -> None:
        self.data = data

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

        """
        dataframe = self.data
        arrays = _get_arrays(dataframe, "file.RemoteFile")
        data = dataframe.to_pylist()

        for batch, *file_arrays in zip(
            *map(lambda x: chunked(x, _MAX_BATCH_SIZE), (data, *arrays))  # type: ignore[arg-type]
        ):
            for file_array in file_arrays:
                upload_files(
                    access_key,
                    url,
                    owner,
                    dataset,
                    draft_number=draft_number,
                    sheet=sheet,
                    files=file_array,
                    jobs=jobs,
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


def _get_arrays(dataframe: "DataFrame", type_name: str) -> List["Container"]:
    arrays = []
    for key in dataframe.schema.get_keys(type_name):
        array: "Container" = dataframe
        for subkey in key:
            array = array[subkey]  # type: ignore[index]
        arrays.append(array)
    return arrays
