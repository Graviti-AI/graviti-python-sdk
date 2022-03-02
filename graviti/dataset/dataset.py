#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the Graviti Dataset."""

from typing import Optional

from tensorbay.client.status import Status
from tensorbay.utility.user import UserMutableMapping

from graviti.dataframe.frame import DataFrame
from graviti.manager.branch import BranchManager
from graviti.manager.commit import CommitManager
from graviti.manager.draft import DraftManager
from graviti.manager.tag import TagManager


class Dataset(UserMutableMapping[str, DataFrame]):
    """This class defines the basic concept of the dataset on Graviti.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        dataset_id: Dataset ID.
        name: The name of the dataset, unique for a user.
        status: The version control status of the dataset.
        alias: Dataset alias.

    """

    def __init__(
        self,
        access_key: str,
        url: str,
        dataset_id: str,
        name: str,
        *,
        status: Status,
        alias: str,
    ) -> None:
        pass

    @property
    def url(self) -> str:
        """Return the url of the graviti website.

        Return:
            The url of the graviti website.

        """

    @property
    def access_key(self) -> str:
        """Return the access key of the user.

        Return:
            The access key of the user.

        """

    @property
    def dataset_id(self) -> str:
        """Return the ID of the dataset.

        Return:
            The ID of the dataset.

        """

    @property
    def status(self) -> Status:
        """Return the status of the dataset.

        Return:
            The status of the dataset.

        """

    @property
    def config_name(self) -> str:
        """Return the config name of the dataset.

        Return:
            The config name of the dataset.

        """

    @property
    def branches(self) -> BranchManager:
        """Get class :class:`~graviti.manager.dataset.BranchManager` instance.

        Return:
            Required :class:`~graviti.manager.dataset.BranchManager` instance.

        """

    @property
    def drafts(self) -> DraftManager:
        """Get class :class:`~graviti.manager.dataset.DraftManager` instance.

        Return:
            Required :class:`~graviti.manager.dataset.DraftManager` instance.

        """

    @property
    def commits(self) -> CommitManager:
        """Get class :class:`~graviti.manager.dataset.CommitManager` instance.

        Return:
            Required :class:`~graviti.manager.dataset.CommitManager` instance.

        """

    @property
    def tags(self) -> TagManager:
        """Get class :class:`~graviti.manager.dataset.TagManager` instance.

        Return:
            Required :class:`~graviti.manager.dataset.TagManager` instance.

        """

    def checkout(self, revision: str) -> None:
        """Checkout to a commit.

        Arguments:
            revision: The information to locate the specific commit, which can be the commit id,
                the branch, or the tag.

        """

    def checkout_draft(self, draft_number: int) -> None:
        """Checkout to a draft.

        Arguments:
            draft_number: The draft number.

        """

    def commit(self, title: str, description: str = "", *, tag: Optional[str] = None) -> None:
        """Commit the current draft.

        Arguments:
            title: The commit title.
            description: The commit description.
            tag: A tag for current commit.

        """

    def upload(
        self,
        jobs: int = 1,
    ) -> None:
        """Upload the local dataset to Graviti.

        Arguments:
            jobs: The number of the max workers in multi-thread upload.

        """

    def edit(self) -> None:
        """Edit the dataset."""
