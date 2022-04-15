#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the Tag and TagManager."""

from typing import TYPE_CHECKING, Optional

from tensorbay.client.struct import Tag as TensorbayTag

from graviti.manager.commit import NamedCommit
from graviti.manager.lazy import PagingList

if TYPE_CHECKING:
    from graviti.manager.dataset import Dataset


class Tag(NamedCommit):
    """This class defines the structure of the tag of a commit.

    Arguments:
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
        dataset: :class:`~graviti.dataset.dataset.Dataset` instance.

    """

    def __init__(self, dataset: "Dataset") -> None:
        pass

    def create(self, name: str, revision: Optional[str] = None) -> TensorbayTag:
        """Create a tag for a commit.

        Arguments:
            name: The tag name to be created for the specific commit.
            revision: The information to locate the specific commit, which can be the commit id,
                the branch name, or the tag name.
                If the revision is not given, create the tag for the current commit.

        Return:
            The :class:`.TensorbayTag` instance with the given name.

        """

    def get(self, name: str) -> TensorbayTag:
        """Get the certain tag with the given name.

        Arguments:
            name: The required tag name.

        Return:
            The :class:`.TensorbayTag` instance with the given name.

        """

    def list(self) -> PagingList[TensorbayTag]:
        """List the information of tags.

        Return:
            The PagingList of :class:`tags<.TensorbayTag>` instances.

        """

    def delete(self, name: str) -> None:
        """Delete a tag.

        Arguments:
            name: The tag name to be deleted for the specific commit.

        """
