#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the Branch and BranchManager."""

from functools import partial
from typing import TYPE_CHECKING, Dict, Generator, Optional

from tensorbay.utility import attr

from graviti.manager.commit import ROOT_COMMIT_ID, NamedCommit
from graviti.manager.lazy import PagingList
from graviti.openapi import create_branch, delete_branch, get_branch, list_branches

if TYPE_CHECKING:
    from graviti.manager.dataset import DatasetAccessInfo

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
        access_info: :class:`~graviti.manager.dataset.DatasetAccessInfo` instance.
        commit_id: The commit id.

    """

    def __init__(self, access_info: "DatasetAccessInfo", commit_id: str) -> None:
        self._access_info = access_info
        self._commit_id = commit_id

    def _generate(self, offset: int = 0, limit: int = 128) -> Generator[Branch, None, int]:
        response = list_branches(
            **self._access_info,
            offset=offset,
            limit=limit,
        )

        for item in response["branches"]:
            yield Branch.from_pyobj(item)

        return response["totalCount"]  # type: ignore[no-any-return]

    def create(self, name: str, revision: Optional[str] = None) -> Branch:
        """Create a branch.

        Arguments:
            name: The branch name.
            revision: The information to locate the specific commit, which can be the commit id,
                the branch name, or the tag name.
                If the revision is not given, create the branch based on the current commit.

        Returns:
            The :class:`.Branch` instance with the given name.

        """
        if not revision:
            revision = self._commit_id

        response = create_branch(**self._access_info, name=name, revision=revision)
        return Branch.from_pyobj(response)

    def get(self, name: str) -> Branch:
        """Get the branch with the given name.

        Arguments:
            name: The required branch name.

        Raises:
            TypeError: When the given branch name is illegal.

        Returns:
            The :class:`.Branch` instance with the given name.

        """
        if not name:
            raise TypeError("The given branch name is illegal")

        response = get_branch(**self._access_info, branch=name)
        return Branch.from_pyobj(response)

    def list(self) -> PagingList[Branch]:
        """List the information of branches.

        Returns:
            The PagingList of :class:`branches<.Branch>` instances.

        """
        return PagingList(self._generate, 128)

    def delete(self, name: str) -> None:
        """Delete a branch.

        Arguments:
            name: The name of the branch to be deleted.

        """
        delete_branch(**self._access_info, branch=name)
