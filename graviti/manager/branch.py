#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the BranchManager."""

from typing import TYPE_CHECKING, Generator, Optional

from tensorbay.client.struct import Branch

from graviti.client import list_branches
from graviti.exception import ResourceNotExistError
from graviti.manager.lazy import PagingList

if TYPE_CHECKING:
    from graviti.manager.dataset import Dataset


class BranchManager:
    """This class defines the operations on the branch in Graviti.

    Arguments:
        dataset: :class:`~graviti.dataset.dataset.Dataset` instance.

    """

    def __init__(self, dataset: "Dataset") -> None:
        self._dataset = dataset

    def _generate(
        self, name: Optional[str] = None, offset: int = 0, limit: int = 128
    ) -> Generator[Branch, None, int]:
        response = list_branches(
            self._dataset.url,
            self._dataset.access_key,
            self._dataset.dataset_id,
            name=name,
            offset=offset,
            limit=limit,
        )

        for item in response["branches"]:
            yield Branch.loads(item)

        return response["totalCount"]  # type: ignore[no-any-return]

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

        Raises:
            TypeError: When the given branch name is illegal.
            ResourceNotExistError: When the required branch does not exist.

        Returns:
            The :class:`.Branch` instance with the given name.

        """
        if not name:
            raise TypeError("The given branch name is illegal")

        try:
            branch = next(self._generate(name))
        except StopIteration as error:
            raise ResourceNotExistError(resource="branch", identification=name) from error

        return branch

    def list(self) -> PagingList[Branch]:
        """List the information of branches.

        Returns:
            The PagingList of :class:`branches<.Branch>` instances.

        """
        return PagingList(lambda offset, limit: self._generate(None, offset, limit), 128)

    def delete(self, name: str) -> None:
        """Delete a branch.

        Arguments:
            name: The name of the branch to be deleted.

        """
