#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the Branch and BranchManager."""

from typing import TYPE_CHECKING, Generator

from graviti.exception import NoCommitsError, ResourceNameError
from graviti.manager.commit import NamedCommit
from graviti.manager.common import CURRENT_COMMIT, LIMIT, check_head_status
from graviti.manager.lazy import LazyPagingList
from graviti.openapi import create_branch, delete_branch, get_branch, list_branches
from graviti.utility import check_type

if TYPE_CHECKING:
    from graviti.manager.dataset import Dataset


class Branch(NamedCommit):
    """This class defines the structure of a branch.

    Arguments:
        dataset: Class :class:`~graviti.dataset.dataset.Dataset` instance.
        name: The name of the branch.
        commit_id: The commit id.

    """


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
            yield Branch.from_response(self._dataset, item)

        return response["total_count"]  # type: ignore[no-any-return]

    def create(self, name: str, revision: str = CURRENT_COMMIT) -> Branch:
        """Create a branch.

        Arguments:
            name: The branch name.
            revision: The information to locate the specific commit, which can be the commit id,
                the branch name, or the tag name. The default value is the current commit of the
                dataset.

        Raises:
            NoCommitsError: When create branches on default branch without commit history.

        Returns:
            The :class:`.Branch` instance with the given name.

        """
        head = self._dataset.HEAD
        if revision is CURRENT_COMMIT:
            _revision = head.commit_id
            if _revision is None:
                raise NoCommitsError(
                    "Creating branches on the default branch without commit history is not allowed."
                    "Please commit a draft first"
                )
        else:
            _revision = revision

        response = create_branch(
            self._dataset.access_key,
            self._dataset.url,
            self._dataset.owner,
            self._dataset.name,
            name=name,
            revision=_revision,
        )

        check_head_status(head, _revision, response["commit_id"])
        return Branch.from_response(self._dataset, response)

    def get(self, name: str) -> Branch:
        """Get the branch with the given name.

        Arguments:
            name: The required branch name.

        Raises:
            ResourceNameError: When the given name is an empty string.

        Returns:
            The :class:`.Branch` instance with the given name.

        """
        check_type("name", name, str)
        if not name:
            raise ResourceNameError("branch", name)

        response = get_branch(
            self._dataset.access_key,
            self._dataset.url,
            self._dataset.owner,
            self._dataset.name,
            branch=name,
        )
        check_head_status(self._dataset.HEAD, name, response["commit_id"])

        return Branch.from_response(self._dataset, response)

    def list(self) -> LazyPagingList[Branch]:
        """List the information of branches.

        Returns:
            The LazyPagingList of :class:`branches<.Branch>` instances.

        """
        return LazyPagingList(self._generate, LIMIT)

    def delete(self, name: str) -> None:
        """Delete a branch.

        Arguments:
            name: The name of the branch to be deleted.

        Raises:
            ResourceNameError: When the given name is an empty string.

        """
        check_type("name", name, str)
        if not name:
            raise ResourceNameError("branch", name)

        delete_branch(
            self._dataset.access_key,
            self._dataset.url,
            self._dataset.owner,
            self._dataset.name,
            branch=name,
        )
