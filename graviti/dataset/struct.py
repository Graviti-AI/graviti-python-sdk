#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Structures of dataset-scopic actions on the Graviti."""

from typing import TYPE_CHECKING, Dict, Optional, Tuple, Type, TypeVar

from tensorbay.client.struct import User
from tensorbay.utility import AttrsMixin, ReprMixin, attr, common_loads

if TYPE_CHECKING:
    from graviti.dataset import Dataset


class Draft:
    """This class defines the basic structure of a draft.

    Arguments:
        dataset: Class :class:`~graviti.dataset.dataset.Dataset` instance.
        number: The number of the draft.
        title: The title of the draft.
        branch_name: The branch name.
        status: The draft status which includes "OPEN", "CLOSED", "COMMITTED".
        parent_commit_id: The parent commit id.
        author: The author of the draft.
        updated_at: The time of last update.
        description: The draft description.

    """

    def __init__(
        self,
        dataset: "Dataset",
        number: int,
        *,
        title: str,
        branch_name: str,
        status: str,
        parent_commit_id: str,
        author: User,
        updated_at: int,
        description: str = "",
    ) -> None:
        pass

    def edit(self, title: Optional[str] = None, description: Optional[str] = None) -> None:
        """Update title and description of the draft.

        Arguments:
            title: The title of the draft.
            description: The description of the draft.

        """

    def close(self) -> None:
        """Close the draft."""


class Commit(AttrsMixin, ReprMixin):
    """This class defines the structure of a commit.

    Arguments:
        commit_id: The commit id.
        parent_commit_id: The parent commit id.
        title: The commit title.
        description: The commit description.
        committer: The commit user.
        committed_at: The time when the draft is committed.

    """

    _T = TypeVar("_T", bound="Commit")

    _repr_attrs: Tuple[str, ...] = ("parent_commit_id", "title", "committer", "committed_at")

    commit_id: str = attr()
    parent_commit_id: str = attr(default="")
    title: str = attr()
    description: str = attr(default="")
    committer: str = attr()
    committed_at: str = attr()

    def __init__(  # pylint: disable=too-many-arguments
        self,
        commit_id: str,
        parent_commit_id: str,
        title: str,
        description: str,
        committer: str,
        committed_at: str,
    ) -> None:
        self.commit_id = commit_id
        self.parent_commit_id = parent_commit_id
        self.title = title
        self.description = description
        self.committer = committer
        self.committed_at = committed_at

    def _repr_head(self) -> str:
        return f'{self.__class__.__name__}("{self.commit_id}")'

    @classmethod
    def from_pyobj(cls: Type[_T], contents: Dict[str, str]) -> _T:
        """Create a :class:`Commit` instance from python dict.

        Arguments:
            contents: A python dict containing all the information of the commit::

                    {
                        "commit_id": <str>
                        "parent_commit_id": <str>
                        "title": <str>
                        "description": <str>
                        "committer":  <str>
                        "committed_at": <str>
                    }

        Returns:
            A :class:`Commit` instance created from the input python dict.

        """
        return common_loads(cls, contents)

    def to_pyobj(self) -> Dict[str, str]:
        """Dump the instance to a python dict.

        Returns:
            A python dict containing all the information of the commit::

                {
                    "commit_id": <str>
                    "parent_commit_id": <str>
                    "title": <str>
                    "description": <str>
                    "committer":  <str>
                    "committed_at": <str>
                }

        """
        return self._dumps()
