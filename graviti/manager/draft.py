#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the Draft and DraftManager."""

from functools import partial
from typing import TYPE_CHECKING, Any, Dict, Generator, List, Optional, Tuple

import pyarrow as pa

from graviti.exception import StatusError
from graviti.manager.branch import Branch
from graviti.manager.commit import Commit
from graviti.manager.common import ALL_BRANCHES, CURRENT_BRANCH, LIMIT, check_head_status
from graviti.manager.lazy import LazyPagingList
from graviti.manager.sheets import Sheets
from graviti.openapi import (
    RECORD_KEY,
    commit_draft,
    create_draft,
    get_draft,
    get_draft_sheet,
    list_draft_data,
    list_draft_sheets,
    list_drafts,
    update_draft,
)
from graviti.operation import SheetOperation
from graviti.paging.factory import LazyLowerCaseFactory
from graviti.utility import check_type, convert_iso_to_datetime

if TYPE_CHECKING:
    from graviti.manager.dataset import Dataset


class Draft(Sheets):  # pylint: disable=too-many-instance-attributes
    """The basic structure of the Graviti draft.

    Arguments:
        dataset: Class :class:`~graviti.dataset.dataset.Dataset` instance.
        number: The number of the draft.
        title: The title of the draft.
        branch: The based branch of the draft.
        state: The draft state which includes "OPEN", "CLOSED", "COMMITTED".
        parent_commit_id: The parent commit id.
        creator: The creator of the draft.
        created_at: The time when the draft is created.
        updated_at: The time of last update.
        description: The draft description.

    Attributes:
        parent: The parent of the draft.

    """

    _repr_attrs: Tuple[str, ...] = ("state", "branch", "creator", "created_at", "updated_at")

    def __init__(
        self,
        dataset: "Dataset",
        number: int,
        *,
        title: str,
        branch: str,
        state: str,
        parent_commit_id: Optional[str],
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
        self.parent = None if parent_commit_id is None else Commit(dataset, parent_commit_id)
        self.creator = creator
        self.created_at = convert_iso_to_datetime(created_at)
        self.updated_at = convert_iso_to_datetime(updated_at)
        self.description = description
        self.operations: List[SheetOperation] = []

    def _repr_head(self) -> str:
        return f'{self.__class__.__name__}("#{self.number}: {self.title}")'

    def _list_data(self, offset: int, limit: int, sheet_name: str) -> Dict[str, Any]:
        _dataset = self._dataset
        return list_draft_data(  # type: ignore[no-any-return]
            _dataset.access_key,
            _dataset.url,
            _dataset.workspace,
            _dataset.name,
            draft_number=self.number,
            sheet=sheet_name,
            offset=offset,
            limit=limit,
        )["data"]

    def _list_sheets(self) -> Dict[str, Any]:
        _dataset = self._dataset
        return list_draft_sheets(
            _dataset.access_key,
            _dataset.url,
            _dataset.workspace,
            _dataset.name,
            draft_number=self.number,
        )

    def _get_sheet(self, sheet_name: str) -> Dict[str, Any]:
        _dataset = self._dataset
        return get_draft_sheet(
            _dataset.access_key,
            _dataset.url,
            _dataset.workspace,
            _dataset.name,
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
        _dataset = self._dataset
        response = update_draft(
            _dataset.access_key,
            _dataset.url,
            _dataset.workspace,
            _dataset.name,
            draft_number=self.number,
            title=title,
            description=description,
        )
        self.title = response["title"]
        self.description = response["description"]
        self.updated_at = convert_iso_to_datetime(response["updated_at"])

    def close(self) -> None:
        """Close the draft."""
        _dataset = self._dataset
        response = update_draft(
            _dataset.access_key,
            _dataset.url,
            _dataset.workspace,
            _dataset.name,
            draft_number=self.number,
            state="CLOSED",
        )
        self.state = response["state"]
        self.updated_at = convert_iso_to_datetime(response["updated_at"])

    def commit(
        self, title: str, description: Optional[str] = None, update_dataset_head: bool = True
    ) -> Branch:
        """Commit the current draft.

        Arguments:
            title: The commit title.
            description: The commit description.
            update_dataset_head: Whether to update the dataset HEAD.

                * | True (the default value): The dataset will be updated to the committed
                    version. At this time, previous modifications to the dataset will be lost.
                * | False: The HEAD of the dataset will not be updated. This can be set if
                    the user needs to continue with some operations on the dataset.

        Returns:
            The :class:`~graviti.manager.branch.Branch` instance.

        Examples:
            The default scenario: ``update_dataset_head`` is True.

            >>> dataset = ws.datasets.get("Graviti-dataset-demo")
            >>> dataset.HEAD.name  # The version of the dataset is Branch("main").
            "main"
            >>> dataset.HEAD.commit_id
            "524d110ecae7474eaec9471f4a6c28b0"
            >>> draft = dataset.drafts.create("draft-4", branch="dev")
            >>> draft.commit("commit-4")
            Branch("dev")(
              (commit_id): '3db73ac2876a42c0bf43a0489ce1756a',
              (parent): Commit("1b21a40f03ab4cec814ec47ee0d10b24"),
              (title): 'commit-4',
              (committer): 'graviti-example',
              (committed_at): 2022-07-19 04:23:45+00:00
            )
            >>> dataset.HEAD.name  # The version of the dataset has been updated to Branch("dev").
            "dev"
            >>> dataset.HEAD.commit_id
            "3db73ac2876a42c0bf43a0489ce1756a"

            Set ``update_dataset_head`` to False.

            >>> dataset = ws.datasets.get("Graviti-dataset-demo")
            >>> dataset.HEAD.name  # The version of the dataset is Branch("main").
            "main"
            >>> dataset.HEAD.commit_id
            "524d110ecae7474eaec9471f4a6c28b0"
            >>> draft = dataset.drafts.create("draft-5", branch="dev")
            >>> draft.commit("commit-5", update_dataset_head=False)
            Branch("dev")(
              (commit_id): '781007a41d1641859c87cb00f8e32bf3',
              (parent): Commit("3db73ac2876a42c0bf43a0489ce1756a"),
              (title): 'commit-5',
              (committer): 'graviti-example',
              (committed_at): 2022-07-19 04:25:45+00:00
            )
            >>> dataset.HEAD.name  # The version of the dataset has not been updated.
            "main"
            >>> dataset.HEAD.commit_id
            "524d110ecae7474eaec9471f4a6c28b0"

        """
        _dataset = self._dataset
        branch_info = commit_draft(
            _dataset.access_key,
            _dataset.url,
            _dataset.workspace,
            _dataset.name,
            draft_number=self.number,
            title=title,
            description=description,
        )
        branch_info["name"] = self.branch
        branch = Branch.from_response(_dataset, branch_info)

        draft_info = get_draft(
            _dataset.access_key,
            _dataset.url,
            _dataset.workspace,
            _dataset.name,
            draft_number=self.number,
        )
        self.state = draft_info["state"]
        self.updated_at = convert_iso_to_datetime(draft_info["updated_at"])

        if update_dataset_head:
            _dataset._data = branch  # pylint: disable=protected-access

        return branch

    def upload(self, jobs: int = 8, quiet: bool = False) -> None:
        """Upload the local dataset to Graviti.

        Arguments:
            jobs: The number of the max workers in multi-thread upload, the default is 8.
            quiet: Set to True to stop showing the upload process bar.

        """
        modified_sheets = {name: df for name, df in self.items() if df.operations}

        self._upload_to_draft(self.number, jobs, quiet)

        for name, df in modified_sheets.items():
            patype = df.schema.to_pyarrow(_to_backend=True)
            factory = LazyLowerCaseFactory(
                len(df),
                LIMIT,
                partial(self._list_data, sheet_name=name),
                pa.struct([pa.field(RECORD_KEY, pa.string()), *patype]),
            )
            df._refresh_data_from_factory(  # pylint: disable=protected-access)
                factory, self._dataset.object_permission_manager
            )


class DraftManager:
    """This class defines the operations on the draft in Graviti.

    Arguments:
        dataset: :class:`~graviti.manager.dataset.Dataset` instance.

    """

    def __init__(self, dataset: "Dataset") -> None:
        self._dataset = dataset

    def _generate(
        self,
        state: str,
        branch: Optional[str],
        offset: int,
        limit: int,
    ) -> Generator[Draft, None, int]:
        _dataset = self._dataset
        response = list_drafts(
            _dataset.access_key,
            _dataset.url,
            _dataset.workspace,
            _dataset.name,
            state=state,
            branch=branch,
            offset=offset,
            limit=limit,
        )

        for item in response["drafts"]:
            yield Draft(_dataset, **item)

        return response["total_count"]  # type: ignore[no-any-return]

    def create(
        self, title: str, description: Optional[str] = None, branch: str = CURRENT_BRANCH
    ) -> Draft:
        """Create a draft.

        Arguments:
            title: The draft title.
            description: The draft description.
            branch: The branch name. The default value is the current branch of the dataset.

        Returns:
            The :class:`.Draft` instance with the given title and description.

        Raises:
            StatusError: When creating the draft without basing on a branch.

        """
        _dataset = self._dataset
        head = _dataset.HEAD
        if branch is CURRENT_BRANCH:
            if not isinstance(head, Branch):
                raise StatusError(
                    "The current dataset is not on a branch, please checkout a branch first "
                    "or input the argument 'branch'"
                )
            branch = head.name

        response = create_draft(
            _dataset.access_key,
            _dataset.url,
            _dataset.workspace,
            _dataset.name,
            title=title,
            branch=branch,
            description=description,
        )

        check_head_status(head, branch, response["parent_commit_id"])
        return Draft(_dataset, **response)

    def get(self, draft_number: int) -> Draft:
        """Get the certain draft with the given draft number.

        Arguments:
            draft_number: The required draft number.

        Returns:
            The :class:`.Draft` instance with the given number.

        """
        check_type("draft_number", draft_number, int)
        _dataset = self._dataset
        response = get_draft(
            _dataset.access_key,
            _dataset.url,
            _dataset.workspace,
            _dataset.name,
            draft_number=draft_number,
        )
        return Draft(_dataset, **response)

    def list(self, state: str = "OPEN", branch: str = ALL_BRANCHES) -> LazyPagingList[Draft]:
        """List all the drafts.

        Arguments:
            state: The draft state which includes "OPEN", "CLOSED", "COMMITTED", "ALL". The default
                value is "OPEN".
            branch: The branch name. The default value is all branches.

        Returns:
            The LazyPagingList of :class:`drafts<.Draft>` instances.

        """
        if branch is ALL_BRANCHES:
            _branch = None
        else:
            check_type("branch", branch, str)
            _branch = branch

        return LazyPagingList(
            lambda offset, limit: self._generate(state, _branch, offset, limit),
            LIMIT,
        )
