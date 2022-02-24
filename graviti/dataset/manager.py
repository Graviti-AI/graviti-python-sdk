#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the Manager."""

from typing import TYPE_CHECKING, Optional

from tensorbay.client.lazy import PagingList
from tensorbay.client.struct import Branch, Commit, Tag

from graviti.dataset.struct import Draft

if TYPE_CHECKING:
    from graviti.dataset.dataset import Dataset


class DatasetManager:
    """This class defines the operations on the dataset on Graviti.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.

    """

    def __init__(self, access_key: str, url: str) -> None:
        pass

    def create(
        self,
        name: str,
        alias: str = "",
        config_name: Optional[str] = None,
        is_public: bool = False,
    ) -> "Dataset":
        """Create a Graviti dataset with given name.

        Arguments:
            name: The name of the dataset, unique for a user.
            alias: Alias of the dataset, default is "".
            config_name: The auth storage config name.
            is_public: Whether the dataset is a public dataset.

        Return:
            The created :class:`~graviti.dataset.dataset.Dataset` instance.

        """

    def get(self, name: str) -> "Dataset":
        """Get a Graviti dataset with given name.

        Arguments:
            name: The name of the dataset, unique for a user.

        Return:
            The requested :class:`~graviti.dataset.dataset.Dataset` instance.

        """

    def list(self) -> PagingList["Dataset"]:
        """List Graviti datasets.

        Return:
            The PagingList of :class:`~graviti.dataset.dataset.Dataset` instances.

        """

    def delete(self, name: str) -> None:
        """Delete a Graviti dataset with given name.

        Arguments:
            name: The name of the dataset, unique for a user.

        """


class BranchManager:
    """This class defines the operations on the branch in Graviti.

    Arguments:
        dataset: :class:`~graviti.dataset.dataset.Dataset` instance.

    """

    def __init__(self, dataset: "Dataset") -> None:
        pass

    def create(self, name: str, revision: Optional[str] = None) -> Branch:
        """Create a branch.

        Arguments:
            name: The branch name.
            revision: The information to locate the specific commit, which can be the commit id,
                the branch name, or the tag name.
                If the revision is not given, create the branch based on the current commit.

        Return:
            The :class:`.Branch` instance with the given name.


        """

    def get(self, name: str) -> Branch:
        """Get the branch with the given name.

        Arguments:
            name: The required branch name.

        Return:
            The :class:`.Branch` instance with the given name.

        """

    def list(self) -> PagingList[Branch]:
        """List the information of branches.

        Return:
            The PagingList of :class:`branches<.Branch>` instances.

        """

    def delete(self, name: str) -> None:
        """Delete a branch.

        Arguments:
            name: The name of the branch to be deleted.

        """


class DraftManager:
    """This class defines the operations on the draft in Graviti.

    Arguments:
        dataset: :class:`~graviti.dataset.dataset.Dataset` instance.

    """

    def __init__(self, dataset: "Dataset") -> None:
        pass

    def create(self, title: str, description: str = "", branch_name: Optional[str] = None) -> Draft:
        """Create a draft.

        Arguments:
            title: The draft title.
            description: The draft description.
            branch_name: The branch name.

        Return:
            The :class:`.Draft` instance with the given title and description.

        """

    def get(self, draft_number: Optional[int] = None) -> Draft:
        """Get the certain draft with the given draft number.

        Arguments:
            draft_number: The required draft number. If it is not given, get the current draft.

        Return:
            The :class:`.Draft` instance with the given number.

        """

    def list(
        self, status: Optional[str] = "OPEN", branch_name: Optional[str] = None
    ) -> PagingList[Draft]:
        """List all the drafts.

        Arguments:
            status: The draft status which includes "OPEN", "CLOSED", "COMMITTED", "ALL" and None.
                    where None means listing open drafts.
            branch_name: The branch name.

        Return:
            The PagingList of :class:`drafts<.Draft>` instances.

        """


class CommitManager:
    """This class defines the operations on the commit in Graviti.

    Arguments:
        dataset: :class:`~graviti.dataset.dataset.Dataset` instance.

    """

    def __init__(self, dataset: "Dataset") -> None:
        pass

    def get(self, revision: Optional[str] = None) -> Commit:
        """Get the certain commit with the given revision.

        Arguments:
            revision: The information to locate the specific commit, which can be the commit id,
                the branch name, or the tag name. If it is not given, get the current commit.

        Return:
            The :class:`.Commit` instance with the given revision.

        """

    def list(self, revision: Optional[str] = None) -> PagingList[Commit]:
        """List the commits.

        Arguments:
            revision: The information to locate the specific commit, which can be the commit id,
                the branch name, or the tag name. If it is given, list the commits before the given
                commit. If it is not given, list the commits before the current commit.

        Return:
            The PagingList of :class:`commits<.Commit>` instances.

        """


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
