#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the Draft and DraftManager."""

from typing import TYPE_CHECKING, Any, Dict, Generator, List, Optional, Tuple

from graviti.dataframe import DataFrame
from graviti.manager.commit import Commit
from graviti.manager.lazy import LazyPagingList
from graviti.manager.sheets import Sheets
from graviti.openapi import (
    commit_draft,
    create_draft,
    get_draft,
    get_draft_sheet,
    list_draft_data,
    list_draft_sheets,
    list_drafts,
    update_draft,
)
from graviti.operation import CreateSheet, DeleteSheet, SheetOperation

if TYPE_CHECKING:
    from graviti.manager.dataset import Dataset


class Draft(Sheets):  # pylint: disable=too-many-instance-attributes
    """The basic structure of the Graviti draft.

    Arguments:
        dataset: Class :class:`~graviti.dataset.dataset.Dataset` instance.
        number: The number of the draft.
        title: The title of the draft.
        branch_name: The branch name.
        state: The draft state which includes "OPEN", "CLOSED", "COMMITTED".
        parent_commit_id: The parent commit id.
        creator: The creator of the draft.
        created_at: The time when the draft is created.
        updated_at: The time of last update.
        description: The draft description.

    """

    _repr_attrs: Tuple[str, ...] = ("title", "state", "branch")

    def __init__(
        self,
        dataset: "Dataset",
        number: int,
        *,
        title: str,
        branch: str,
        state: str,
        parent_commit_id: str,
        creator: str,
        created_at: str,
        updated_at: str,
        description: str = "",
    ) -> None:
        self._dataset = dataset
        self.number = number
        self.title = title
        self.branch = branch
        self.state = state
        self.parent_commit_id = parent_commit_id
        self.creator = creator
        self.created_at = created_at
        self.updated_at = updated_at
        self.description = description
        self.operations: List[SheetOperation] = []

    def __setitem__(self, key: str, value: DataFrame) -> None:
        is_replace = False
        if key in self:
            is_replace = True

        super().__setitem__(key, value)
        if is_replace:
            self.operations.append(DeleteSheet(key))

        self.operations.append(CreateSheet(key, value.schema.copy()))

    def __delitem__(self, key: str) -> None:
        super().__delitem__(key)
        self.operations.append(DeleteSheet(key))

    def _repr_head(self) -> str:
        return f'{self.__class__.__name__}("{self.number}")'

    def _list_data(self, offset: int, limit: int, sheet_name: str) -> Dict[str, Any]:
        return list_draft_data(  # type: ignore[no-any-return]
            access_key=self._dataset.access_key,
            url=self._dataset.url,
            owner=self._dataset.owner,
            dataset=self._dataset.name,
            draft_number=self.number,
            sheet=sheet_name,
            offset=offset,
            limit=limit,
        )["data"]

    def _list_sheets(self) -> Dict[str, Any]:
        return list_draft_sheets(
            access_key=self._dataset.access_key,
            url=self._dataset.url,
            owner=self._dataset.owner,
            dataset=self._dataset.name,
            draft_number=self.number,
        )

    def _get_sheet(self, sheet_name: str) -> Dict[str, Any]:
        return get_draft_sheet(
            access_key=self._dataset.access_key,
            url=self._dataset.url,
            owner=self._dataset.owner,
            dataset=self._dataset.name,
            draft_number=self.number,
            sheet=sheet_name,
            with_record_count=True,
        )

    def edit(self, title: Optional[str] = None, description: Optional[str] = None) -> None:
        """Update title and description of the draft.

        Arguments:
            title: The title of the draft.
            description: The description of the draft.

        """
        update_draft(
            self._dataset.access_key,
            self._dataset.url,
            self._dataset.owner,
            self._dataset.name,
            draft_number=self.number,
            title=title,
            description=description,
        )
        if title is not None:
            self.title = title
        if description is not None:
            self.description = description
        # TODO: update the draft.updated_at

    def close(self) -> None:
        """Close the draft."""
        update_draft(
            self._dataset.access_key,
            self._dataset.url,
            self._dataset.owner,
            self._dataset.name,
            draft_number=self.number,
            state="CLOSED",
        )
        self.state = "CLOSED"
        # TODO: update the draft.updated_at

    def commit(self, title: str, description: Optional[str] = None) -> Commit:
        """Commit the current draft.

        Arguments:
            title: The commit title.
            description: The commit description.

        Returns:
            The :class:`~graviti.manager.commit.Commit` instance.

        """
        response = commit_draft(
            self._dataset.access_key,
            self._dataset.url,
            self._dataset.owner,
            self._dataset.name,
            draft_number=self.number,
            title=title,
            description=description,
        )
        self.state = "COMMITTED"
        # TODO: update the draft.updated_at
        return Commit(self._dataset, **response)

    def upload(self, jobs: int = 1) -> None:  # pylint: disable=unused-argument
        """Upload the local dataset to Graviti.

        Arguments:
            jobs: The number of the max workers in multi-thread upload.

        """
        for sheet_operation in self.operations:
            sheet_operation.do(
                self._dataset.access_key,
                self._dataset.url,
                self._dataset.owner,
                self._dataset.name,
                draft_number=self.number,
            )

        for sheet_name, dataframe in self.items():
            for df_operation in dataframe.operations:
                df_operation.do(
                    self._dataset.access_key,
                    self._dataset.url,
                    self._dataset.owner,
                    self._dataset.name,
                    draft_number=self.number,
                    sheet=sheet_name,
                )

        delattr(self, "_data")


class DraftManager:
    """This class defines the operations on the draft in Graviti.

    Arguments:
        dataset: :class:`~graviti.manager.dataset.Dataset` instance.

    """

    def __init__(self, dataset: "Dataset") -> None:
        self._dataset = dataset

    def _generate(
        self,
        state: Optional[str] = None,
        branch: Optional[str] = None,
        offset: int = 0,
        limit: int = 128,
    ) -> Generator[Draft, None, int]:
        response = list_drafts(
            self._dataset.access_key,
            self._dataset.url,
            self._dataset.owner,
            self._dataset.name,
            state=state,
            branch=branch,
            offset=offset,
            limit=limit,
        )

        for item in response["drafts"]:
            yield Draft(self._dataset, **item)

        return response["totalCount"]  # type: ignore[no-any-return]

    def create(
        self, title: str, description: Optional[str] = None, branch: Optional[str] = None
    ) -> Draft:
        """Create a draft.

        Arguments:
            title: The draft title.
            description: The draft description.
            branch: The branch name.

        Returns:
            The :class:`.Draft` instance with the given title and description.

        """
        response = create_draft(
            self._dataset.access_key,
            self._dataset.url,
            self._dataset.owner,
            self._dataset.name,
            title=title,
            branch=branch,
            description=description,
        )
        return Draft(self._dataset, **response)

    def get(self, draft_number: int) -> Draft:
        """Get the certain draft with the given draft number.

        Arguments:
            draft_number: The required draft number.

        Returns:
            The :class:`.Draft` instance with the given number.

        """
        response = get_draft(
            self._dataset.access_key,
            self._dataset.url,
            self._dataset.owner,
            self._dataset.name,
            draft_number=draft_number,
        )
        return Draft(self._dataset, **response)

    def list(
        self, state: Optional[str] = None, branch: Optional[str] = None
    ) -> LazyPagingList[Draft]:
        """List all the drafts.

        Arguments:
            state: The draft state which includes "OPEN", "CLOSED", "COMMITTED", "ALL" and None.
                    None means listing open drafts.
            branch: The branch name. None means listing drafts from all branches.

        Returns:
            The LazyPagingList of :class:`drafts<.Draft>` instances.

        """
        return LazyPagingList(
            lambda offset, limit: self._generate(state, branch, offset, limit),
            128,
        )
