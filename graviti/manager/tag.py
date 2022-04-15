#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the TagManager."""

from typing import TYPE_CHECKING, Optional

from tensorbay.client.struct import Tag

from graviti.manager.lazy import PagingList

if TYPE_CHECKING:
    from graviti.manager.dataset import Dataset


class TagManager:
    """This class defines the operations on the tag in Graviti.

    Arguments:
        dataset: :class:`~graviti.dataset.dataset.Dataset` instance.

    """

    def __init__(self, dataset: "Dataset") -> None:
        pass

    def create(self, name: str, revision: Optional[str] = None) -> Tag:
        """Create a tag for a commit.

        Arguments:
            name: The tag name to be created for the specific commit.
            revision: The information to locate the specific commit, which can be the commit id,
                the branch name, or the tag name.
                If the revision is not given, create the tag for the current commit.

        Return:
            The :class:`.Tag` instance with the given name.

        """

    def get(self, name: str) -> Tag:
        """Get the certain tag with the given name.

        Arguments:
            name: The required tag name.

        Return:
            The :class:`.Tag` instance with the given name.

        """

    def list(self) -> PagingList[Tag]:
        """List the information of tags.

        Return:
            The PagingList of :class:`tags<.Tag>` instances.

        """

    def delete(self, name: str) -> None:
        """Delete a tag.

        Arguments:
            name: The tag name to be deleted for the specific commit.

        """
