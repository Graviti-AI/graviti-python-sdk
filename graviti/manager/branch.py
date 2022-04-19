#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the Branch and BranchManager."""

from functools import partial
from typing import TYPE_CHECKING, Dict, Generator, Optional

from tensorbay.client.struct import Branch as TensorbayBranch
from tensorbay.utility import attr

from graviti.client import list_branches
from graviti.exception import ResourceNotExistError
from graviti.manager.commit import ROOT_COMMIT_ID, NamedCommit
from graviti.manager.lazy import PagingList

if TYPE_CHECKING:
    from graviti.manager.dataset import Dataset

_ERROR_MESSAGE = "The '{attr_name}' is not available due to this branch has no commit history."
_attr = partial(attr, is_dynamic=True, error_message=_ERROR_MESSAGE)


class Branch(NamedCommit):
    """This class defines the structure of a branch.

    Arguments:
        name: The name of the branch.
        commit_id: The commit id.
        parent_commit_id: The parent commit id.
        title: The commit title.
        description: The commit description.
        committer: The commit user.
        committed_at: The time when the draft is committed.

    """

    parent_commit_id: str = _attr()
    title: str = _attr()
    description: str = _attr()
    committer: str = _attr()
    committed_at: str = _attr()

    def _loads(self, contents: Dict[str, str]) -> None:
        self.name = contents["name"]
        self.commit_id = contents["commit_id"]

        if self.commit_id == ROOT_COMMIT_ID:
            return

        self.parent_commit_id = contents["parent_commit_id"]
        self.title = contents["title"]
        self.description = contents["description"]
        self.committer = contents["committer"]
        self.committed_at = contents["committed_at"]


class BranchManager:
    """This class defines the operations on the branch in Graviti.

    Arguments:
        dataset: :class:`~graviti.dataset.dataset.Dataset` instance.

    """

    def __init__(self, dataset: "Dataset") -> None:
        self._dataset = dataset

    def _generate(
        self, name: Optional[str] = None, offset: int = 0, limit: int = 128
    ) -> Generator[TensorbayBranch, None, int]:
        response = list_branches(
            self._dataset.url,
            self._dataset.access_key,
            self._dataset._dataset_id,  # pylint: disable=protected-access
            name=name,
            offset=offset,
            limit=limit,
        )

        for item in response["branches"]:
            yield TensorbayBranch.loads(item)

        return response["totalCount"]  # type: ignore[no-any-return]

    def create(self, name: str, revision: Optional[str] = None) -> TensorbayBranch:
        """Create a branch.

        Arguments:
            name: The branch name.
            revision: The information to locate the specific commit, which can be the commit id,
                the branch name, or the tag name.
                If the revision is not given, create the branch based on the current commit.

        Return:
            The :class:`.TensorbayBranch` instance with the given name.

        """

    def get(self, name: str) -> TensorbayBranch:
        """Get the branch with the given name.

        Arguments:
            name: The required branch name.

        Raises:
            TypeError: When the given branch name is illegal.
            ResourceNotExistError: When the required branch does not exist.

        Returns:
            The :class:`.TensorbayBranch` instance with the given name.

        """
        if not name:
            raise TypeError("The given branch name is illegal")

        try:
            branch = next(self._generate(name))
        except StopIteration as error:
            raise ResourceNotExistError(resource="branch", identification=name) from error

        return branch

    def list(self) -> PagingList[TensorbayBranch]:
        """List the information of branches.

        Returns:
            The PagingList of :class:`branches<.TensorbayBranch>` instances.

        """
        return PagingList(lambda offset, limit: self._generate(None, offset, limit), 128)

    def delete(self, name: str) -> None:
        """Delete a branch.

        Arguments:
            name: The name of the branch to be deleted.

        """
