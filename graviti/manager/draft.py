#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the Draft and DraftManager."""

from typing import TYPE_CHECKING, Optional

from tensorbay.client.struct import User

from graviti.manager.lazy import PagingList

if TYPE_CHECKING:
    from graviti.manager.dataset import Dataset


class Draft:
    """This class defines the basic structure of a draft.

    Arguments:
        dataset: Class :class:`~graviti.dataset.dataset.Dataset` instance.
        number: The number of the draft.
        title: The title of the draft.
        branch_name: The branch name.
        status: The draft status which includes "OPEN", "CLOSED", "COMMITTED".
        parent_commit_id: The parent commit id.
        author: The author of the draft.
        updated_at: The time of last update.
        description: The draft description.

    """

    def __init__(
        self,
        dataset: "Dataset",
        number: int,
        *,
        title: str,
        branch_name: str,
        status: str,
        parent_commit_id: str,
        author: User,
        updated_at: int,
        description: str = "",
    ) -> None:
        pass

    def edit(self, title: Optional[str] = None, description: Optional[str] = None) -> None:
        """Update title and description of the draft.

        Arguments:
            title: The title of the draft.
            description: The description of the draft.

        """

    def close(self) -> None:
        """Close the draft."""


class DraftManager:
    """This class defines the operations on the draft in Graviti.

    Arguments:
        dataset: :class:`~graviti.dataset.dataset.Dataset` instance.

    """

    def __init__(self, dataset: "Dataset") -> None:
        pass

    def create(self, title: str, description: str = "", branch_name: Optional[str] = None) -> Draft:
        """Create a draft.

        Arguments:
            title: The draft title.
            description: The draft description.
            branch_name: The branch name.

        Return:
            The :class:`.Draft` instance with the given title and description.

        """

    def get(self, draft_number: Optional[int] = None) -> Draft:
        """Get the certain draft with the given draft number.

        Arguments:
            draft_number: The required draft number. If it is not given, get the current draft.

        Return:
            The :class:`.Draft` instance with the given number.

        """

    def list(
        self, status: Optional[str] = "OPEN", branch_name: Optional[str] = None
    ) -> PagingList[Draft]:
        """List all the drafts.

        Arguments:
            status: The draft status which includes "OPEN", "CLOSED", "COMMITTED", "ALL" and None.
                    where None means listing open drafts.
            branch_name: The branch name.

        Return:
            The PagingList of :class:`drafts<.Draft>` instances.

        """
