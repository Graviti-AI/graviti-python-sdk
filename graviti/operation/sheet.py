#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Definitions of different operations about the sheet on a draft."""

from typing import TYPE_CHECKING

from graviti.openapi import create_sheet, delete_sheet
from graviti.operation.common import get_schema
from graviti.portex import record

if TYPE_CHECKING:
    from graviti.manager import Dataset


class SheetOperation:
    """This class defines the basic method of the operation about the sheet on a draft.

    Arguments:
        sheet: The sheet name.

    """

    def __init__(self, sheet: str) -> None:
        self.sheet = sheet

    def execute(self, dataset: "Dataset", draft_number: int) -> None:
        """Execute the OpenAPI create sheet.

        Arguments:
            dataset: The Dataset instance.
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

    def __init__(self, sheet: str, schema: record) -> None:
        super().__init__(sheet)
        self.schema = schema

    def execute(self, dataset: "Dataset", draft_number: int) -> None:
        """Execute the OpenAPI create sheet.

        Arguments:
            dataset: The Dataset instance.
            draft_number: The draft number.

        """
        portex_schema, avro_schema, arrow_schema = get_schema(self.schema)

        _workspace = dataset.workspace
        create_sheet(
            _workspace.access_key,
            _workspace.url,
            _workspace.name,
            dataset.name,
            draft_number=draft_number,
            name=self.sheet,
            schema=portex_schema,
            _avro_schema=avro_schema,
            _arrow_schema=arrow_schema,
        )


class DeleteSheet(SheetOperation):
    """This class defines the operation that delete a sheet."""

    def execute(self, dataset: "Dataset", draft_number: int) -> None:
        """Execute the OpenAPI delete sheet.

        Arguments:
            dataset: The Dataset instance.
            draft_number: The draft number.

        """
        _workspace = dataset.workspace
        delete_sheet(
            _workspace.access_key,
            _workspace.url,
            _workspace.name,
            dataset.name,
            draft_number=draft_number,
            sheet=self.sheet,
        )
