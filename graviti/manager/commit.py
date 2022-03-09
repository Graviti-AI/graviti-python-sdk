#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the CommitManager."""

from typing import TYPE_CHECKING, Generator, Optional

from tensorbay.client.lazy import PagingList
from tensorbay.client.struct import Commit
from tensorbay.exception import ResourceNotExistError

from graviti.client.commit import list_commits

if TYPE_CHECKING:
    from graviti.dataset.dataset import Dataset


class CommitManager:
    """This class defines the operations on the commit in Graviti.

    Arguments:
        dataset: :class:`~graviti.dataset.dataset.Dataset` instance.

    """

    def __init__(self, dataset: "Dataset") -> None:
        self._dataset = dataset

    def _generate(
        self, revision: Optional[str], offset: int = 0, limit: int = 128
    ) -> Generator[Commit, None, int]:
        if revision is None:
            revision = self._dataset.commit_id

        if not revision:
            raise TypeError("The given revision is illegal")

        response = list_commits(
            self._dataset.url,
            self._dataset.access_key,
            self._dataset.dataset_id,
            commit=revision,
            offset=offset,
            limit=limit,
        )

        for item in response["commits"]:
            yield Commit.loads(item)

        return response["totalCount"]  # type: ignore[no-any-return]

    def get(self, revision: Optional[str] = None) -> Commit:
        """Get the certain commit with the given revision.

        Arguments:
            revision: The information to locate the specific commit, which can be the commit id,
                the branch name, or the tag name. If it is not given, get the current commit.

        Raises:
            ResourceNotExistError: When the required commit does not exist.

        Returns:
            The :class:`.Commit` instance with the given revision.

        """
        try:
            commit = next(self._generate(revision))
        except StopIteration as error:
            raise ResourceNotExistError(resource="commit", identification=revision) from error

        return commit

    def list(self, revision: Optional[str] = None) -> PagingList[Commit]:
        """List the commits.

        Arguments:
            revision: The information to locate the specific commit, which can be the commit id,
                the branch name, or the tag name.
                If it is given, list the commits before the given commit.
                If it is not given, list the commits before the current commit.

        Returns:
            The PagingList of :class:`commits<.Commit>` instances.

        """
        return PagingList(
            lambda offset, limit: self._generate(revision, offset, limit),
            128,
        )
