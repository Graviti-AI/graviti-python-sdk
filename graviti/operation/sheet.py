#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Definitions of different operations about the sheet on a draft."""

from graviti.openapi import create_sheet, delete_sheet
from graviti.operation.common import get_schema
from graviti.portex import PortexRecordBase


class SheetOperation:
    """This class defines the basic method of the operation about the sheet on a draft.

    Arguments:
        sheet: The sheet name.

    """

    def __init__(self, sheet: str) -> None:
        self.sheet = sheet

    def do(  # pylint: disable=invalid-name
        self,
        access_key: str,
        url: str,
        owner: str,
        dataset: str,
        *,
        draft_number: int,
    ) -> None:
        """Execute the OpenAPI create sheet.

        Arguments:
            access_key: User's access key.
            url: The URL of the graviti website.
            owner: The owner of the dataset.
            dataset: Name of the dataset, unique for a user.
            draft_number: The draft number.

        Raises:
            NotImplementedError: The method of the base class should not be called.

        """
        raise NotImplementedError


class CreateSheet(SheetOperation):
    """This class defines the operation that create a sheet.

    Arguments:
        sheet: The sheet name.
        schema: The schema of the DataFrame.

    """

    def __init__(self, sheet: str, schema: PortexRecordBase) -> None:
        super().__init__(sheet)
        self.schema = schema

    def do(  # pylint: disable=invalid-name
        self,
        access_key: str,
        url: str,
        owner: str,
        dataset: str,
        *,
        draft_number: int,
    ) -> None:
        """Execute the OpenAPI create sheet.

        Arguments:
            access_key: User's access key.
            url: The URL of the graviti website.
            owner: The owner of the dataset.
            dataset: Name of the dataset, unique for a user.
            draft_number: The draft number.

        """
        portex_schema, avro_schema, arrow_schema = get_schema(self.schema)

        create_sheet(
            access_key,
            url,
            owner,
            dataset,
            draft_number=draft_number,
            name=self.sheet,
            schema=portex_schema,
            _avro_schema=avro_schema,
            _arrow_schema=arrow_schema,
        )


class DeleteSheet(SheetOperation):
    """This class defines the operation that delete a sheet."""

    def do(  # pylint: disable=invalid-name
        self,
        access_key: str,
        url: str,
        owner: str,
        dataset: str,
        *,
        draft_number: int,
    ) -> None:
        """Execute the OpenAPI delete sheet.

        Arguments:
            access_key: User's access key.
            url: The URL of the graviti website.
            owner: The owner of the dataset.
            dataset: Name of the dataset, unique for a user.
            draft_number: The draft number.

        """
        delete_sheet(
            access_key,
            url,
            owner,
            dataset,
            draft_number=draft_number,
            sheet=self.sheet,
        )
