#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the Tag and TagManager."""

from typing import TYPE_CHECKING, Generator

from graviti.exception import NoCommitsError, ResourceNameError
from graviti.manager.commit import NamedCommit
from graviti.manager.common import CURRENT_COMMIT, LIMIT, check_head_status
from graviti.manager.lazy import LazyPagingList
from graviti.openapi import create_tag, delete_tag, get_tag, list_tags
from graviti.utility import check_type

if TYPE_CHECKING:
    from graviti.manager.dataset import Dataset


class Tag(NamedCommit):
    """This class defines the structure of the tag of a commit.

    Arguments:
        dataset: Class :class:`~graviti.dataset.dataset.Dataset` instance.
        name: The name of the tag.
        commit_id: The commit id.

    """


class TagManager:
    """This class defines the operations on the tag in Graviti.

    Arguments:
        dataset: :class:`~graviti.manager.dataset.Dataset` instance.

    """

    def __init__(self, dataset: "Dataset") -> None:
        self._dataset = dataset

    def _generate(self, offset: int, limit: int) -> Generator[Tag, None, int]:
        _dataset = self._dataset
        response = list_tags(
            _dataset.access_key,
            _dataset.url,
            _dataset.workspace,
            _dataset.name,
            offset=offset,
            limit=limit,
        )

        head = _dataset.HEAD
        for item in response["tags"]:
            check_head_status(head, item["name"], item["commit_id"])
            yield Tag.from_response(_dataset, item)

        return response["total_count"]  # type: ignore[no-any-return]

    def create(self, name: str, revision: str = CURRENT_COMMIT) -> Tag:
        """Create a tag for a commit.

        Arguments:
            name: The tag name to be created for the specific commit.
            revision: The information to locate the specific commit, which can be the commit id,
                the branch name, or the tag name. The default value is the current commit of the
                dataset.

        Raises:
            NoCommitsError: When create tags on the default branch without commit history.

        Returns:
            The :class:`.Tag` instance with the given name.

        """
        _dataset = self._dataset
        head = _dataset.HEAD
        if revision is CURRENT_COMMIT:
            _revision = head.commit_id
            if _revision is None:
                raise NoCommitsError(
                    "Creating tags on the default branch without commit history is not allowed."
                    "Please commit a draft first"
                )
        else:
            _revision = revision

        response = create_tag(
            _dataset.access_key,
            _dataset.url,
            _dataset.workspace,
            _dataset.name,
            name=name,
            revision=_revision,
        )

        check_head_status(head, _revision, response["commit_id"])
        return Tag.from_response(_dataset, response)

    def get(self, name: str) -> Tag:
        """Get the certain tag with the given name.

        Arguments:
            name: The required tag name.

        Raises:
            ResourceNameError: When the name is an empty string.

        Returns:
            The :class:`.Tag` instance with the given name.

        """
        check_type("name", name, str)
        if not name:
            raise ResourceNameError("tag", name)

        _dataset = self._dataset
        response = get_tag(
            _dataset.access_key,
            _dataset.url,
            _dataset.workspace,
            _dataset.name,
            tag=name,
        )
        check_head_status(_dataset.HEAD, name, response["commit_id"])
        return Tag.from_response(_dataset, response)

    def list(self) -> LazyPagingList[Tag]:
        """List the information of tags.

        Returns:
            The LazyPagingList of :class:`tags<.Tag>` instances.

        """
        return LazyPagingList(self._generate, LIMIT)

    def delete(self, name: str) -> None:
        """Delete a tag.

        Arguments:
            name: The tag name to be deleted for the specific commit.

        Raises:
            ResourceNameError: When the name is an empty string.

        """
        check_type("name", name, str)
        if not name:
            raise ResourceNameError("tag", name)

        _dataset = self._dataset
        delete_tag(
            _dataset.access_key,
            _dataset.url,
            _dataset.workspace,
            _dataset.name,
            tag=name,
        )
