#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the Dataset and DatasetManager."""

from typing import Any, Dict, Generator, Optional, Tuple, Type, TypeVar

from tensorbay.utility import AttrsMixin, attr, common_loads

from graviti.dataframe import DataFrame
from graviti.exception import ResourceNotExistError
from graviti.manager.branch import BranchManager
from graviti.manager.commit import Commit, CommitManager
from graviti.manager.draft import DraftManager
from graviti.manager.lazy import LazyPagingList
from graviti.manager.tag import TagManager
from graviti.openapi import (
    create_dataset,
    delete_dataset,
    get_dataset,
    list_datasets,
    update_dataset,
)
from graviti.utility import ReprMixin, ReprType, UserMutableMapping


class Dataset(  # pylint: disable=too-many-instance-attributes
    UserMutableMapping[str, DataFrame], AttrsMixin, ReprMixin
):
    """This class defines the basic concept of the dataset on Graviti.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        dataset_id: Dataset ID.
        name: The name of the dataset, unique for a user.
        alias: Dataset alias.
        default_branch: The default branch of dataset.
        commit_id: The commit ID.
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

    _dataset_id: str = attr(key="id")

    access_key: str = attr()
    url: str = attr()
    name: str = attr()
    alias: str = attr()
    default_branch: str = attr()
    created_at: str = attr()
    updated_at: str = attr()
    owner: str = attr()
    is_public: bool = attr()
    config: str = attr()

    def __init__(
        self,
        access_key: str,
        url: str,
        dataset_id: str,
        name: str,
        *,
        alias: str,
        default_branch: str,
        commit_id: str,
        created_at: str,
        updated_at: str,
        owner: str,
        is_public: bool,
        config: str,
    ) -> None:
        self.access_key = access_key
        self.url = url
        self._dataset_id = dataset_id
        self.name = name
        self.alias = alias
        self.default_branch = default_branch
        self.created_at = created_at
        self.updated_at = updated_at
        self.owner = owner
        self.is_public = is_public
        self.config = config
        self._data: Commit = self.branches.get(default_branch)
        if commit_id != self._data.commit_id:
            self._data = self.commits.get(commit_id)

    def _repr_head(self) -> str:
        return f'{self.__class__.__name__}("{self.owner}/{self.name}")'

    @classmethod
    def from_pyobj(cls: Type[_T], contents: Dict[str, Any]) -> _T:
        """Create a :class:`Dataset` instance from python dict.

        Arguments:
            contents: A python dict containing all the information of the dataset::

                    {
                        "access_key": <str>
                        "url": <str>
                        "id": <str>
                        "name": <str>
                        "alias": <str>
                        "default_branch": <str>
                        "commit_id": <str>
                        "created_at": <str>
                        "updated_at": <str>
                        "owner": <str>
                        "is_public": <bool>
                        "config": <str>
                    }

        Returns:
            A :class:`Dataset` instance created from the input python dict.

        """
        dataset = common_loads(cls, contents)
        default_branch = dataset.default_branch
        dataset._data = dataset.branches.get(default_branch)
        return dataset

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
        try:
            self._data = self.branches.get(revision)
        except ResourceNotExistError:
            self._data = self.commits.get(revision)

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
        self.updated_at = response["updated_at"]

    def search(self, sheet: str, criteria: Dict[str, Any]) -> DataFrame:
        """Create a search.

        Arguments:
            sheet: The sheet name.
            criteria: The criteria of search.

        Returns:
            The created :class:`~graviti.dataframe.DataFrame` instance.

        """
        return self.HEAD.search(sheet, criteria)


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

    def _generate(self, offset: int = 0, limit: int = 128) -> Generator[Dataset, None, int]:
        arguments = {"access_key": self.access_key, "url": self.url}
        response = list_datasets(**arguments, limit=limit, offset=offset)

        for item in response["datasets"]:
            item.update(arguments)
            yield Dataset.from_pyobj(item)

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
        arguments = {"access_key": self.access_key, "url": self.url}
        response = create_dataset(
            **arguments,
            name=name,
            alias=alias,
            config=config,
            with_draft=False,
        )
        response.update(arguments)

        return Dataset.from_pyobj(response)

    def get(self, dataset: str) -> Dataset:
        """Get a Graviti dataset with given name.

        Arguments:
            dataset: The name of the dataset, unique for a user.

        Returns:
            The requested :class:`~graviti.manager.dataset.Dataset` instance.

        Raises:
            ResourceNotExistError: When the required dataset does not exist.

        """
        if not dataset:
            raise ResourceNotExistError(resource="dataset", identification=dataset)

        arguments = {"access_key": self.access_key, "url": self.url}
        response = get_dataset(**arguments, owner=self.owner, dataset=dataset)
        response.update(arguments)

        return Dataset.from_pyobj(response)

    def list(self) -> LazyPagingList[Dataset]:
        """List Graviti datasets.

        Returns:
            The LazyPagingList of :class:`~graviti.manager.dataset.Dataset` instances.

        """
        return LazyPagingList(self._generate, 128)

    def delete(self, name: str) -> None:
        """Delete a Graviti dataset with given name.

        Arguments:
            name: The name of the dataset, unique for a user.

        """
        delete_dataset(self.access_key, self.url, self.owner, name)
