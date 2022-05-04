#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the Tag and TagManager."""

from typing import TYPE_CHECKING, Generator, Optional

from graviti.exception import ResourceNotExistError
from graviti.manager.commit import NamedCommit
from graviti.manager.lazy import LazyPagingList
from graviti.openapi import create_tag, delete_tag, get_tag, list_tags

if TYPE_CHECKING:
    from graviti.manager.dataset import Dataset


class Tag(NamedCommit):
    """This class defines the structure of the tag of a commit.

    Arguments:
        dataset: Class :class:`~graviti.dataset.dataset.Dataset` instance.
        name: The name of the tag.
        commit_id: The commit id.
        parent_commit_id: The parent commit id.
        title: The commit title.
        description: The commit description.
        committer: The commit user.
        committed_at: The time when the draft is committed.

    """


class TagManager:
    """This class defines the operations on the tag in Graviti.

    Arguments:
        dataset: :class:`~graviti.manager.dataset.Dataset` instance.

    """

    def __init__(self, dataset: "Dataset") -> None:
        self._dataset = dataset

    def _generate(self, offset: int = 0, limit: int = 128) -> Generator[Tag, None, int]:
        response = list_tags(
            self._dataset.access_key,
            self._dataset.url,
            self._dataset.owner,
            self._dataset.name,
            offset=offset,
            limit=limit,
        )

        for item in response["tags"]:
            yield Tag(self._dataset, **item)

        return response["total_count"]  # type: ignore[no-any-return]

    def create(self, name: str, revision: Optional[str] = None) -> Tag:
        """Create a tag for a commit.

        Arguments:
            name: The tag name to be created for the specific commit.
            revision: The information to locate the specific commit, which can be the commit id,
                the branch name, or the tag name.
                If the revision is not given, create the tag for the current commit.

        Returns:
            The :class:`.Tag` instance with the given name.

        """
        if not revision:
            revision = self._dataset.HEAD.commit_id

        response = create_tag(
            self._dataset.access_key,
            self._dataset.url,
            self._dataset.owner,
            self._dataset.name,
            name=name,
            revision=revision,
        )
        return Tag(self._dataset, **response)

    def get(self, name: str) -> Tag:
        """Get the certain tag with the given name.

        Arguments:
            name: The required tag name.

        Raises:
            ResourceNotExistError: When the name is an empty string.

        Returns:
            The :class:`.Tag` instance with the given name.

        """
        if not name:
            raise ResourceNotExistError(resource="tag", identification=name)

        response = get_tag(
            self._dataset.access_key,
            self._dataset.url,
            self._dataset.owner,
            self._dataset.name,
            tag=name,
        )
        return Tag(self._dataset, **response)

    def list(self) -> LazyPagingList[Tag]:
        """List the information of tags.

        Returns:
            The LazyPagingList of :class:`tags<.Tag>` instances.

        """
        return LazyPagingList(self._generate, 128)

    def delete(self, name: str) -> None:
        """Delete a tag.

        Arguments:
            name: The tag name to be deleted for the specific commit.

        Raises:
            ResourceNotExistError: When the name is an empty string.

        """
        if not name:
            raise ResourceNotExistError(resource="tag", identification=name)

        delete_tag(
            self._dataset.access_key,
            self._dataset.url,
            self._dataset.owner,
            self._dataset.name,
            tag=name,
        )
