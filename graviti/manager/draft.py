#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the DraftManager."""

from typing import TYPE_CHECKING, Optional

from tensorbay.client.lazy import PagingList

from graviti.dataset.struct import Draft

if TYPE_CHECKING:
    from graviti.dataset.dataset import Dataset


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
