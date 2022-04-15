#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the Dataset and DatasetManager."""

from typing import Any, Dict, Generator, Iterator, KeysView, Mapping, Optional

from tensorbay.dataset import Notes, RemoteData
from tensorbay.label import Catalog
from tensorbay.utility import URL

from graviti.client import (
    get_catalog,
    get_dataset,
    get_notes,
    list_data_details,
    list_datasets,
    list_segments,
)
from graviti.dataframe import DataFrame
from graviti.exception import ResourceNotExistError
from graviti.manager.branch import BranchManager
from graviti.manager.commit import CommitManager
from graviti.manager.draft import DraftManager
from graviti.manager.lazy import PagingList
from graviti.manager.tag import TagManager
from graviti.schema import Extractors, catalog_to_schema, get_extractors
from graviti.utility import LazyFactory, LazyList, NestedDict

LazyLists = NestedDict[str, LazyList[Any]]


class Dataset(Mapping[str, DataFrame]):  # pylint: disable=too-many-instance-attributes
    """This class defines the basic concept of the dataset on Graviti.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        dataset_id: Dataset ID.
        name: The name of the dataset, unique for a user.
        alias: Dataset alias.
        commit_id: The commit ID.
        branch_name: The branch name.

    """

    _data: Dict[str, DataFrame]

    def __init__(
        self,
        access_key: str,
        url: str,
        dataset_id: str,
        name: str,
        *,
        alias: str,
        commit_id: str,
        branch_name: Optional[str] = None,
    ) -> None:
        self._access_key = access_key
        self._url = url
        self._dataset_id = dataset_id
        self._name = name
        self._alias = alias
        self._commit_id = commit_id
        self._branch_name = branch_name

        self._is_public: Optional[bool] = None

    def __len__(self) -> int:
        return self._get_data().__len__()

    def __getitem__(self, key: str) -> DataFrame:
        return self._get_data().__getitem__(key)

    def __iter__(self) -> Iterator[str]:
        return self._get_data().__iter__()

    def _get_lazy_lists(self, factory: LazyFactory, extractors: Extractors) -> LazyLists:
        lazy_lists: LazyLists = {}
        for key, arguments in extractors.items():
            if isinstance(arguments, tuple):
                lazy_lists[key] = factory.create_list(*arguments)
            else:
                lazy_lists[key] = self._get_lazy_lists(factory, arguments)
        return lazy_lists

    def _init_data(self) -> None:
        self._data = {}
        response = list_segments(
            self._url,
            self._access_key,
            self._dataset_id,
            commit=self._commit_id,
        )
        for sheet in response["segments"]:
            sheet_name = sheet["name"]
            data_details = list_data_details(
                self._url,
                self._access_key,
                self._dataset_id,
                sheet_name,
                commit=self._commit_id,
            )

            def factory_getter(
                offset: int, limit: int, sheet_name: str = sheet_name
            ) -> Dict[str, Any]:
                return list_data_details(
                    self._url,
                    self._access_key,
                    self._dataset_id,
                    sheet_name,
                    commit=self._commit_id,
                    offset=offset,
                    limit=limit,
                )

            factory = LazyFactory(
                data_details["totalCount"],
                128,
                factory_getter,
            )
            catalog = get_catalog(
                self._url,
                self._access_key,
                self._dataset_id,
                commit=self._commit_id,
            )

            first_data_details = data_details["dataDetails"][0]
            remote_data = RemoteData.from_response_body(
                first_data_details,
                url=URL(
                    first_data_details["url"], updater=lambda: "update is not supported currently"
                ),
            )
            notes = get_notes(
                self._url,
                self._access_key,
                self._dataset_id,
                commit=self._commit_id,
            )

            schema = catalog_to_schema(
                Catalog.loads(catalog["catalog"]), remote_data, Notes.loads(notes)
            )
            lazy_lists = self._get_lazy_lists(factory, get_extractors(schema))
            self._data[sheet_name] = DataFrame.from_lazy_lists(lazy_lists)

    def _get_data(self) -> Dict[str, DataFrame]:
        if not hasattr(self, "_data"):
            self._init_data()

        return self._data

    def keys(self) -> KeysView[str]:
        """Return a new view of the keys in dict.

        Returns:
            The keys in dict.

        """
        return self._get_data().keys()

    @property
    def access_key(self) -> str:
        """Return the access key of the user.

        Returns:
            The access key of the user.

        """
        return self._access_key

    @property
    def url(self) -> str:
        """Return the url of the graviti website.

        Returns:
            The url of the graviti website.

        """
        return self._url

    @property
    def dataset_id(self) -> str:
        """Return the ID of the dataset.

        Returns:
            The ID of the dataset.

        """
        return self._dataset_id

    @property
    def name(self) -> str:
        """Return the name of the dataset.

        Returns:
            The name of the dataset.

        """
        return self._name

    @property
    def alias(self) -> str:
        """Return the alias of the dataset.

        Returns:
            The alias of the dataset.

        """
        return self._alias

    @property
    def commit_id(self) -> str:
        """Return the commit id of the dataset.

        Returns:
            The commit id of the dataset.

        """
        return self._commit_id

    @property
    def branch_name(self) -> Optional[str]:
        """Return the branch name of the dataset.

        Returns:
            The branch name of the dataset.

        """
        return self._branch_name

    @property
    def is_public(self) -> bool:
        """Return whether the dataset is public.

        Returns:
            Whether the dataset is public.

        """
        if self._is_public is None:
            info = get_dataset(url=self.url, access_key=self.access_key, dataset_id=self.dataset_id)
            self._is_public = info["isPublic"]
        return self._is_public

    @property
    def branches(self) -> BranchManager:
        """Get class :class:`~graviti.manager.dataset.BranchManager` instance.

        Returns:
            Required :class:`~graviti.manager.dataset.BranchManager` instance.

        """
        return BranchManager(self)

    @property
    def drafts(self) -> DraftManager:
        """Get class :class:`~graviti.manager.dataset.DraftManager` instance.

        Return:
            Required :class:`~graviti.manager.dataset.DraftManager` instance.

        """

    @property
    def commits(self) -> CommitManager:
        """Get class :class:`~graviti.manager.dataset.CommitManager` instance.

        Returns:
            Required :class:`~graviti.manager.dataset.CommitManager` instance.

        """
        return CommitManager(self)

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
        try:
            branch = self.branches.get(revision)
            self._branch_name = branch.name
            self._commit_id = branch.commit_id
        except ResourceNotExistError:
            self._commit_id = self.commits.get(revision).commit_id
            self._branch_name = None

        if hasattr(self, "_data"):
            delattr(self, "_data")

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

    def upload(self, jobs: int = 1) -> None:
        """Upload the local dataset to Graviti.

        Arguments:
            jobs: The number of the max workers in multi-thread upload.

        """


class DatasetManager:
    """This class defines the operations on the dataset on Graviti.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.

    """

    def __init__(self, access_key: str, url: str) -> None:
        self._access_key = access_key
        self._url = url

    def _generate(
        self, name: Optional[str] = None, offset: int = 0, limit: int = 128
    ) -> Generator[Dataset, None, int]:
        response = list_datasets(self._url, self._access_key, name=name, limit=limit, offset=offset)

        for item in response["datasets"]:
            yield Dataset(
                self._access_key,
                self._url,
                item["id"],
                item["name"],
                branch_name=item["defaultBranch"],
                commit_id=item["commitId"],
                alias=item["alias"],
            )

        return response["totalCount"]  # type: ignore[no-any-return]

    def create(
        self,
        name: str,
        alias: str = "",
        config_name: Optional[str] = None,
        is_public: bool = False,
    ) -> Dataset:
        """Create a Graviti dataset with given name.

        Arguments:
            name: The name of the dataset, unique for a user.
            alias: Alias of the dataset, default is "".
            config_name: The auth storage config name.
            is_public: Whether the dataset is a public dataset.

        Return:
            The created :class:`~graviti.dataset.dataset.Dataset` instance.

        """

    def get(self, name: str) -> Dataset:
        """Get a Graviti dataset with given name.

        Arguments:
            name: The name of the dataset, unique for a user.

        Returns:
            The requested :class:`~graviti.dataset.dataset.Dataset` instance.

        Raises:
            ResourceNotExistError: When the required dataset does not exist.

        """
        if not name:
            raise ResourceNotExistError(resource="dataset", identification=name)

        try:
            return next(self._generate(name))
        except StopIteration as error:
            raise ResourceNotExistError(resource="dataset", identification=name) from error

    def list(self) -> PagingList[Dataset]:
        """List Graviti datasets.

        Returns:
            The PagingList of :class:`~graviti.dataset.dataset.Dataset` instances.

        """
        return PagingList(
            lambda offset, limit: self._generate(None, offset, limit),
            128,
        )

    def delete(self, name: str) -> None:
        """Delete a Graviti dataset with given name.

        Arguments:
            name: The name of the dataset, unique for a user.

        """
