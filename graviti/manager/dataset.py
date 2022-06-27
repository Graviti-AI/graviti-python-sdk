#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the Dataset and DatasetManager."""

from enum import Enum
from typing import Any, Dict, Generator, Optional, Tuple, TypeVar

from graviti.dataframe import DataFrame
from graviti.exception import ResourceNameError
from graviti.manager.branch import Branch, BranchManager
from graviti.manager.commit import Commit, CommitManager
from graviti.manager.common import LIMIT
from graviti.manager.draft import DraftManager
from graviti.manager.lazy import LazyPagingList
from graviti.manager.tag import Tag, TagManager
from graviti.openapi import (
    create_dataset,
    delete_dataset,
    get_dataset,
    get_revision,
    list_datasets,
    update_dataset,
)
from graviti.utility import ReprMixin, ReprType, UserMutableMapping, convert_iso_to_datetime


class RevisionType(Enum):
    """RevisionType is an enumeration type including "BRANCH", "COMMIT" and "TAG"."""

    BRANCH = Branch
    COMMIT = Commit
    TAG = Tag


class Dataset(  # pylint: disable=too-many-instance-attributes
    UserMutableMapping[str, DataFrame], ReprMixin
):
    """This class defines the basic concept of the dataset on Graviti.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        response: The response of the OpenAPI associated with the dataset::

                {
                    "id": <str>
                    "name": <str>
                    "alias": <str>
                    "default_branch": <str>
                    "commit_id": <Optional[str]>
                    "cover_url": <str>
                    "created_at": <str>
                    "updated_at": <str>
                    "owner": <str>
                    "is_public": <bool>
                    "config": <str>
                }

    Attributes:
        dataset_id: Dataset ID.
        name: The name of the dataset, unique for a user.
        alias: Dataset alias.
        default_branch: The default branch of dataset.
        commit_id: The commit ID of the dataset.
        created_at: The time when the dataset was created.
        updated_at: The time when the dataset was last modified.
        owner: The owner of the dataset.
        is_public: Whether the dataset is public.
        config: The config of dataset.

    """

    _T = TypeVar("_T", bound="Dataset")

    _repr_type = ReprType.INSTANCE

    _repr_attrs: Tuple[str, ...] = (
        "alias",
        "default_branch",
        "created_at",
        "updated_at",
        "is_public",
        "config",
        "branch",
    )

    def __init__(self, access_key: str, url: str, response: Dict[str, Any]) -> None:
        self.access_key = access_key
        self.url = url
        self._dataset_id = response["id"]
        self.name = response["name"]
        self.alias = response["alias"]
        self.default_branch = response["default_branch"]
        self.created_at = convert_iso_to_datetime(response["created_at"])
        self.updated_at = convert_iso_to_datetime(response["updated_at"])
        self.owner = response["owner"]
        self.is_public = response["is_public"]
        self.config = response["config"]
        self._data: Commit = Branch(self, response["name"], response["commit_id"])

    def _repr_head(self) -> str:
        return f'{self.__class__.__name__}("{self.owner}/{self.name}")'

    @property
    def HEAD(self) -> Commit:  # pylint: disable=invalid-name
        """Return the current branch or commit.

        Returns:
            The current branch or commit.

        """
        return self._data

    @property
    def branches(self) -> BranchManager:
        """Get class :class:`~graviti.manager.branch.BranchManager` instance.

        Returns:
            Required :class:`~graviti.manager.branch.BranchManager` instance.

        """
        return BranchManager(self)

    @property
    def drafts(self) -> DraftManager:
        """Get class :class:`~graviti.manager.draft.DraftManager` instance.

        Returns:
            Required :class:`~graviti.manager.draft.DraftManager` instance.

        """
        return DraftManager(self)

    @property
    def commits(self) -> CommitManager:
        """Get class :class:`~graviti.manager.commit.CommitManager` instance.

        Returns:
            Required :class:`~graviti.manager.commit.CommitManager` instance.

        """
        return CommitManager(self)

    @property
    def tags(self) -> TagManager:
        """Get class :class:`~graviti.manager.tag.TagManager` instance.

        Returns:
            Required :class:`~graviti.manager.tag.TagManager` instance.

        """
        return TagManager(self)

    def checkout(self, revision: str) -> None:
        """Checkout to a commit.

        Arguments:
            revision: The information to locate the specific commit, which can be the commit id,
                the branch, or the tag.

        """
        response = get_revision(
            self.access_key,
            self.url,
            self.owner,
            self.name,
            revision=revision,
        )

        response["name"] = revision
        revision_type = RevisionType[response["type"]]

        self._data = revision_type.value.from_response(self, response)

    def edit(
        self,
        *,
        name: Optional[str] = None,
        alias: Optional[str] = None,
        default_branch: Optional[str] = None,
    ) -> None:
        """Update the meta data of the dataset.

        Arguments:
            name: The new name of the dataset.
            alias: The new alias of the dataset.
            default_branch: The new default branch of the dataset.

        """
        response = update_dataset(
            self.access_key,
            self.url,
            self.owner,
            self.name,
            name=name,
            alias=alias,
            default_branch=default_branch,
        )
        self.name = response["name"]
        self.alias = response["alias"]
        self.default_branch = response["default_branch"]
        self.updated_at = convert_iso_to_datetime(response["updated_at"])


class DatasetManager:
    """This class defines the operations on the dataset on Graviti.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.

    """

    def __init__(self, access_key: str, url: str, owner: str) -> None:
        self.access_key = access_key
        self.url = url
        self.owner = owner

    def _generate(self, offset: int, limit: int) -> Generator[Dataset, None, int]:
        access_key = self.access_key
        url = self.url
        response = list_datasets(access_key, url, limit=limit, offset=offset)

        for item in response["datasets"]:
            yield Dataset(access_key, url, item)

        return response["total_count"]  # type: ignore[no-any-return]

    def create(
        self,
        name: str,
        alias: str = "",
        config: Optional[str] = None,
    ) -> Dataset:
        """Create a Graviti dataset with given name.

        Arguments:
            name: The name of the dataset, unique for a user.
            alias: Alias of the dataset, default is "".
            config: The auth storage config name.

        Returns:
            The created :class:`~graviti.manager.dataset.Dataset` instance.

        """
        response = create_dataset(
            self.access_key,
            self.url,
            name=name,
            alias=alias,
            config=config,
            with_draft=False,
        )

        return Dataset(self.access_key, self.url, response)

    def get(self, dataset: str) -> Dataset:
        """Get a Graviti dataset with given name.

        Arguments:
            dataset: The name of the dataset, unique for a user.

        Returns:
            The requested :class:`~graviti.manager.dataset.Dataset` instance.

        Raises:
            ResourceNameError: When the required dataset does not exist.

        """
        if not dataset:
            raise ResourceNameError("dataset", dataset)

        response = get_dataset(self.access_key, self.url, owner=self.owner, dataset=dataset)

        return Dataset(self.access_key, self.url, response)

    def list(self) -> LazyPagingList[Dataset]:
        """List Graviti datasets.

        Returns:
            The LazyPagingList of :class:`~graviti.manager.dataset.Dataset` instances.

        """
        return LazyPagingList(self._generate, LIMIT)

    def delete(self, name: str) -> None:
        """Delete a Graviti dataset with given name.

        Arguments:
            name: The name of the dataset, unique for a user.

        """
        delete_dataset(self.access_key, self.url, self.owner, name)
