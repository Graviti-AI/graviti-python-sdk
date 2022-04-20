#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the Dataset and DatasetManager."""

from typing import Any, Dict, Generator, Iterator, KeysView, Mapping, Optional, Tuple, Type, TypeVar

from tensorbay.dataset import Notes, RemoteData
from tensorbay.label import Catalog
from tensorbay.utility import URL, AttrsMixin, attr, common_loads

from graviti.client import get_catalog, get_notes, list_data_details, list_segments
from graviti.dataframe import DataFrame
from graviti.exception import ResourceNotExistError
from graviti.manager.branch import BranchManager
from graviti.manager.commit import CommitManager
from graviti.manager.draft import DraftManager
from graviti.manager.lazy import PagingList
from graviti.manager.tag import TagManager
from graviti.openapi import create_dataset, delete_dataset, get_dataset, list_datasets
from graviti.schema import Extractors, catalog_to_schema, get_extractors
from graviti.utility import LazyFactory, LazyList, NestedDict, ReprMixin, ReprType

LazyLists = NestedDict[str, LazyList[Any]]


class DatasetAccessInfo(Mapping[str, str], ReprMixin):
    """This class defines the basic structure of the dataset access info.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        owner: The owner of the dataset.
        dataset: The name of the dataset, unique for a user.

    """

    _repr_attrs: Tuple[str, ...] = (
        "access_key",
        "url",
        "owner",
        "dataset",
    )

    def __init__(self, access_key: str, url: str, owner: str, dataset: str) -> None:
        self.access_key = access_key
        self.url = url
        self.owner = owner
        self.dataset = dataset

    def __len__(self) -> int:
        return len(self._repr_attrs)

    def __getitem__(self, key: str) -> str:
        try:
            return getattr(self, key)  # type: ignore[no-any-return]
        except AttributeError:
            raise KeyError(key) from None

    def __iter__(self) -> Iterator[str]:
        yield from self._repr_attrs


class Dataset(  # pylint: disable=too-many-instance-attributes
    Mapping[str, DataFrame], AttrsMixin, ReprMixin
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

    _repr_type = ReprType.MAPPING
    _repr_attrs: Tuple[str, ...] = (
        "alias",
        "default_branch",
        "commit_id",
        "created_at",
        "updated_at",
        "is_public",
        "config",
        "branch",
    )

    _dataset_id: str = attr(key="id")

    alias: str = attr()
    default_branch: str = attr()
    commit_id: str = attr()
    created_at: str = attr()
    updated_at: str = attr()
    is_public: bool = attr()
    config: str = attr()

    _data: Dict[str, DataFrame]

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
        self._dataset_id = dataset_id
        self.alias = alias
        self.default_branch = default_branch
        self.commit_id = commit_id
        self.created_at = created_at
        self.updated_at = updated_at
        self.is_public = is_public
        self.config = config
        self.branch: Optional[str] = default_branch
        self._access_info = DatasetAccessInfo(access_key, url, owner, name)

    def __len__(self) -> int:
        return self._get_data().__len__()

    def __getitem__(self, key: str) -> DataFrame:
        return self._get_data().__getitem__(key)

    def __iter__(self) -> Iterator[str]:
        return self._get_data().__iter__()

    def _repr_head(self) -> str:
        return f'{self.__class__.__name__}("{self.owner}/{self.name}")'

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
            self.url,
            self.access_key,
            self._dataset_id,
            commit=self.commit_id,
        )
        for sheet in response["segments"]:
            sheet_name = sheet["name"]
            data_details = list_data_details(
                self.url,
                self.access_key,
                self._dataset_id,
                sheet_name,
                commit=self.commit_id,
            )

            def factory_getter(
                offset: int, limit: int, sheet_name: str = sheet_name
            ) -> Dict[str, Any]:
                return list_data_details(
                    self.url,
                    self.access_key,
                    self._dataset_id,
                    sheet_name,
                    commit=self.commit_id,
                    offset=offset,
                    limit=limit,
                )

            factory = LazyFactory(
                data_details["totalCount"],
                128,
                factory_getter,
            )
            catalog = get_catalog(
                self.url,
                self.access_key,
                self._dataset_id,
                commit=self.commit_id,
            )

            first_data_details = data_details["dataDetails"][0]
            remote_data = RemoteData.from_response_body(
                first_data_details,
                url=URL(
                    first_data_details["url"], updater=lambda: "update is not supported currently"
                ),
            )
            notes = get_notes(
                self.url,
                self.access_key,
                self._dataset_id,
                commit=self.commit_id,
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
        dataset.branch = dataset.default_branch
        dataset._access_info = DatasetAccessInfo(
            contents["access_key"], contents["url"], contents["owner"], contents["name"]
        )
        return dataset

    @property
    def access_key(self) -> str:
        """Return the access key of the user.

        Returns:
            The access key of the user.

        """
        return self._access_info.access_key

    @property
    def url(self) -> str:
        """Return the url of the graviti website.

        Returns:
            The url of the graviti website.

        """
        return self._access_info.url

    @property
    def owner(self) -> str:
        """Return the owner of the dataset.

        Returns:
            The owner of the dataset.

        """
        return self._access_info.owner

    @property
    def name(self) -> str:
        """Return the name of the dataset.

        Returns:
            The name of the dataset.

        """
        return self._access_info.dataset

    @property
    def branches(self) -> BranchManager:
        """Get class :class:`~graviti.manager.branch.BranchManager` instance.

        Returns:
            Required :class:`~graviti.manager.branch.BranchManager` instance.

        """
        return BranchManager(self._access_info, self.commit_id)

    @property
    def drafts(self) -> DraftManager:
        """Get class :class:`~graviti.manager.draft.DraftManager` instance.

        Return:
            Required :class:`~graviti.manager.draft.DraftManager` instance.

        """

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

        Return:
            Required :class:`~graviti.manager.tag.TagManager` instance.

        """

    def checkout(self, revision: str) -> None:
        """Checkout to a commit.

        Arguments:
            revision: The information to locate the specific commit, which can be the commit id,
                the branch, or the tag.

        """
        try:
            branch = self.branches.get(revision)
            self.branch = branch.name
            self.commit_id = branch.commit_id
        except ResourceNotExistError:
            self.commit_id = self.commits.get(revision).commit_id
            self.branch = None

        if hasattr(self, "_data"):
            delattr(self, "_data")


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

        return response["totalCount"]  # type: ignore[no-any-return]

    def create(
        self,
        name: str,
        alias: str = "",
        is_public: bool = False,
        config: Optional[str] = None,
    ) -> Dataset:
        """Create a Graviti dataset with given name.

        Arguments:
            name: The name of the dataset, unique for a user.
            alias: Alias of the dataset, default is "".
            is_public: Whether the dataset is a public dataset.
            config: The auth storage config name.

        Returns:
            The created :class:`~graviti.manager.dataset.Dataset` instance.

        """
        arguments: Dict[str, Any] = {
            "access_key": self.access_key,
            "url": self.url,
            "name": name,
            "alias": alias,
            "is_public": is_public,
            "config": config,
        }
        response = create_dataset(**arguments)
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

        arguments: Dict[str, Any] = {
            "access_key": self.access_key,
            "url": self.url,
            "owner": self.owner,
        }
        response = get_dataset(**arguments, dataset=dataset)
        response.update(arguments, name=dataset)

        return Dataset.from_pyobj(response)

    def list(self) -> PagingList[Dataset]:
        """List Graviti datasets.

        Returns:
            The PagingList of :class:`~graviti.manager.dataset.Dataset` instances.

        """
        return PagingList(self._generate, 128)

    def delete(self, name: str) -> None:
        """Delete a Graviti dataset with given name.

        Arguments:
            name: The name of the dataset, unique for a user.

        """
        delete_dataset(self.access_key, self.url, self.owner, name)
