#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the Branch and BranchManager."""

from functools import partial
from typing import TYPE_CHECKING, Generator, Optional

from tensorbay.utility import attr

from graviti.exception import NoCommitsError, ResourceNameError
from graviti.manager.commit import NamedCommit
from graviti.manager.common import check_head_status
from graviti.manager.lazy import LazyPagingList
from graviti.openapi import create_branch, delete_branch, get_branch, list_branches

if TYPE_CHECKING:
    from graviti.manager.dataset import Dataset

_ERROR_MESSAGE = "The '{attr_name}' is not available due to this branch has no commit history."
_attr = partial(attr, is_dynamic=True, error_message=_ERROR_MESSAGE)


class Branch(NamedCommit):
    """This class defines the structure of a branch.

    Arguments:
        dataset: Class :class:`~graviti.dataset.dataset.Dataset` instance.
        name: The name of the branch.
        commit_id: The commit id.
        parent_commit_id: The parent commit id.
        title: The commit title.
        description: The commit description.
        committer: The commit user.
        committed_at: The time when the draft is committed.

    """

    parent_commit_id: Optional[str] = _attr()
    title: str = _attr()
    description: str = _attr()
    committer: str = _attr()
    committed_at: str = _attr()


class BranchManager:
    """This class defines the operations on the branch in Graviti.

    Arguments:
        dataset: :class:`~graviti.manager.dataset.Dataset` instance.

    """

    def __init__(self, dataset: "Dataset") -> None:
        self._dataset = dataset

    def _generate(self, offset: int, limit: int) -> Generator[Branch, None, int]:
        response = list_branches(
            self._dataset.access_key,
            self._dataset.url,
            self._dataset.owner,
            self._dataset.name,
            offset=offset,
            limit=limit,
        )

        head = self._dataset.HEAD
        for item in response["branches"]:
            check_head_status(head, item["name"], item["commit_id"])
            yield Branch(self._dataset, **item)

        return response["total_count"]  # type: ignore[no-any-return]

    def create(self, name: str, revision: Optional[str] = None) -> Branch:
        """Create a branch.

        Arguments:
            name: The branch name.
            revision: The information to locate the specific commit, which can be the commit id,
                the branch name, or the tag name.
                If the revision is not given, create the branch based on the current commit.

        Raises:
            NoCommitsError: When create branches on default branch without commit history.

        Returns:
            The :class:`.Branch` instance with the given name.

        """
        head = self._dataset.HEAD
        if not revision:
            revision = head.commit_id
            if revision is None:
                raise NoCommitsError(
                    "Creating branches on the default branch without commit history is not allowed."
                    "Please commit a draft first"
                )

        response = create_branch(
            self._dataset.access_key,
            self._dataset.url,
            self._dataset.owner,
            self._dataset.name,
            name=name,
            revision=revision,
        )

        check_head_status(head, revision, response["commit_id"])
        return Branch(self._dataset, **response)

    def get(self, name: str) -> Branch:
        """Get the branch with the given name.

        Arguments:
            name: The required branch name.

        Raises:
            ResourceNameError: When the given name is an empty string.

        Returns:
            The :class:`.Branch` instance with the given name.

        """
        if not name:
            raise ResourceNameError("branch", name)

        response = get_branch(
            self._dataset.access_key,
            self._dataset.url,
            self._dataset.owner,
            self._dataset.name,
            branch=name,
        )

        if getattr(self._dataset, "HEAD", None):
            check_head_status(self._dataset.HEAD, name, response["commit_id"])

        return Branch(self._dataset, **response)

    def list(self) -> LazyPagingList[Branch]:
        """List the information of branches.

        Returns:
            The LazyPagingList of :class:`branches<.Branch>` instances.

        """
        return LazyPagingList(self._generate, 24)

    def delete(self, name: str) -> None:
        """Delete a branch.

        Arguments:
            name: The name of the branch to be deleted.

        Raises:
            ResourceNameError: When the given name is an empty string.

        """
        if not name:
            raise ResourceNameError("branch", name)

        delete_branch(
            self._dataset.access_key,
            self._dataset.url,
            self._dataset.owner,
            self._dataset.name,
            branch=name,
        )
