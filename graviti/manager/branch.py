#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the BranchManager."""

from typing import TYPE_CHECKING, Optional

from tensorbay.client.lazy import PagingList
from tensorbay.client.struct import Branch

if TYPE_CHECKING:
    from graviti.dataset.dataset import Dataset


class BranchManager:
    """This class defines the operations on the branch in Graviti.

    Arguments:
        dataset: :class:`~graviti.dataset.dataset.Dataset` instance.

    """

    def __init__(self, dataset: "Dataset") -> None:
        pass

    def create(self, name: str, revision: Optional[str] = None) -> Branch:
        """Create a branch.

        Arguments:
            name: The branch name.
            revision: The information to locate the specific commit, which can be the commit id,
                the branch name, or the tag name.
                If the revision is not given, create the branch based on the current commit.

        Return:
            The :class:`.Branch` instance with the given name.


        """

    def get(self, name: str) -> Branch:
        """Get the branch with the given name.

        Arguments:
            name: The required branch name.

        Return:
            The :class:`.Branch` instance with the given name.

        """

    def list(self) -> PagingList[Branch]:
        """List the information of branches.

        Return:
            The PagingList of :class:`branches<.Branch>` instances.

        """

    def delete(self, name: str) -> None:
        """Delete a branch.

        Arguments:
            name: The name of the branch to be deleted.

        """
