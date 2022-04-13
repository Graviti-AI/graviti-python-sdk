#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Structures of dataset-scopic actions on the Graviti."""

from typing import TYPE_CHECKING, Optional

from tensorbay.client.struct import User

if TYPE_CHECKING:
    from graviti.dataset import Dataset


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
