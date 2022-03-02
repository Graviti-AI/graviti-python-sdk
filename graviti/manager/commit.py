#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the CommitManager."""

from typing import TYPE_CHECKING, Optional

from tensorbay.client.lazy import PagingList
from tensorbay.client.struct import Commit

if TYPE_CHECKING:
    from graviti.dataset.dataset import Dataset


class CommitManager:
    """This class defines the operations on the commit in Graviti.

    Arguments:
        dataset: :class:`~graviti.dataset.dataset.Dataset` instance.

    """

    def __init__(self, dataset: "Dataset") -> None:
        pass

    def get(self, revision: Optional[str] = None) -> Commit:
        """Get the certain commit with the given revision.

        Arguments:
            revision: The information to locate the specific commit, which can be the commit id,
                the branch name, or the tag name. If it is not given, get the current commit.

        Return:
            The :class:`.Commit` instance with the given revision.

        """

    def list(self, revision: Optional[str] = None) -> PagingList[Commit]:
        """List the commits.

        Arguments:
            revision: The information to locate the specific commit, which can be the commit id,
                the branch name, or the tag name. If it is given, list the commits before the given
                commit. If it is not given, list the commits before the current commit.

        Return:
            The PagingList of :class:`commits<.Commit>` instances.

        """
