#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the Dataset and DatasetManager."""

import logging
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, Generator, Optional, Tuple, TypeVar

from graviti.dataframe import DataFrame
from graviti.exception import ResourceNameError, StatusError
from graviti.manager.action import ActionManager
from graviti.manager.branch import Branch, BranchManager
from graviti.manager.commit import Commit, CommitManager
from graviti.manager.common import LIMIT
from graviti.manager.draft import DraftManager
from graviti.manager.lazy import LazyPagingList
from graviti.manager.permission import (
    AZUREObjectPermissionManager,
    ObjectPermissionManager,
    OSSObjectPermissionManager,
    S3ObjectPermissionManager,
)
from graviti.manager.search import SearchManager
from graviti.manager.storage_config import StorageConfig
from graviti.manager.tag import Tag, TagManager
from graviti.openapi import (
    create_dataset,
    delete_dataset,
    get_dataset,
    get_revision,
    list_datasets,
    update_dataset,
)
from graviti.utility import (
    ReprMixin,
    ReprType,
    UserMutableMapping,
    check_type,
    convert_iso_to_datetime,
)

if TYPE_CHECKING:
    from graviti import Workspace

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
logger.addHandler(handler)


class RevisionType(Enum):
    """RevisionType is an enumeration type including "BRANCH", "COMMIT" and "TAG"."""

    BRANCH = Branch
    COMMIT = Commit
    TAG = Tag


class ObjectPermissionManagerType(Enum):
    """ObjectPermissionManagerType is an enumeration type including "OSS", "S3" and "AZURE"."""

    OSS = OSSObjectPermissionManager
    S3 = S3ObjectPermissionManager
    AZURE = AZUREObjectPermissionManager


class Dataset(  # pylint: disable=too-many-instance-attributes
    UserMutableMapping[str, DataFrame], ReprMixin
):
    """This class defines the basic concept of the dataset on Graviti.

    Arguments:
        workspace: Class :class:`~graviti.workspace.Workspace` instance.
        response: The response of the OpenAPI associated with the dataset::

                {
                    "id": <str>
                    "name": <str>
                    "alias": <str>
                    "workspace": <str>
                    "default_branch": <str>
                    "commit_id": <Optional[str]>
                    "cover_url": <str>
                    "creator": <str>
                    "created_at": <str>
                    "updated_at": <str>
                    "is_public": <bool>
                    "storage_config": <str>
                    "backend_type": <str>
                }

    Attributes:
        name: The name of the dataset, unique for a user.
        alias: Dataset alias.
        workspace: The workspace of the dataset.
        default_branch: The default branch of dataset.
        commit_id: The commit ID of the dataset.
        creator: The creator of the dataset.
        created_at: The time when the dataset was created.
        updated_at: The time when the dataset was last modified.
        is_public: Whether the dataset is public.
        storage_config: The storage config of dataset.

    """

    _T = TypeVar("_T", bound="Dataset")

    _repr_type = ReprType.INSTANCE

    _repr_attrs: Tuple[str, ...] = (
        "alias",
        "default_branch",
        "creator",
        "created_at",
        "updated_at",
        "is_public",
        "storage_config",
    )

    def __init__(self, workspace: "Workspace", response: Dict[str, Any]) -> None:
        self._id: str = response["id"]

        self.workspace = workspace
        self.name: str = response["name"]
        self.alias: str = response["alias"]
        self.default_branch: str = response["default_branch"]
        self.creator: str = response["creator"]
        self.created_at = convert_iso_to_datetime(response["created_at"])
        self.updated_at = convert_iso_to_datetime(response["updated_at"])
        self.is_public: str = response["is_public"]
        self.storage_config = StorageConfig(workspace, response["storage_config"])

        self.object_permission_manager: ObjectPermissionManager = ObjectPermissionManagerType[
            response["backend_type"]
        ].value(self)

        self._data: Commit = Branch(self, response["default_branch"], response["commit_id"])

    def _repr_head(self) -> str:
        return f'{self.__class__.__name__}("{self.workspace.name}/{self.name}")'

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

    @property
    def searches(self) -> SearchManager:
        """Get class :class:`~graviti.manager.search.SearchManager` instance.

        Returns:
            Required :class:`~graviti.manager.search.SearchManager` instance.

        """
        return SearchManager(self)

    @property
    def actions(self) -> ActionManager:
        """Get class :class:`~graviti.manager.action.ActionManager` instance.

        Returns:
            Required :class:`~graviti.manager.action.ActionManager` instance.

        """
        return ActionManager(self)

    def checkout(self, revision: str) -> None:
        """Checkout to a commit.

        Arguments:
            revision: The information to locate the specific commit, which can be the commit id,
                the branch, or the tag.

        """
        _workspace = self.workspace
        response = get_revision(
            _workspace.access_key,
            _workspace.url,
            _workspace.name,
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
        _workspace = self.workspace
        response = update_dataset(
            _workspace.access_key,
            _workspace.url,
            _workspace.name,
            self.name,
            name=name,
            alias=alias,
            default_branch=default_branch,
        )
        self.name = response["name"]
        self.alias = response["alias"]
        self.default_branch = response["default_branch"]
        self.updated_at = convert_iso_to_datetime(response["updated_at"])

    def commit(
        self, title: str, description: Optional[str] = None, jobs: int = 8, quiet: bool = False
    ) -> None:
        """Create, upload and commit the draft to push the local dataset to Graviti.

        Arguments:
            title: The commit title.
            description: The commit description.
            jobs: The number of the max workers in multi-thread upload, the default is 8.
            quiet: Set to True to stop showing the upload process bar.

        Raises:
            StatusError: When the HEAD of the dataset is not a branch.
            StatusError: When the dataset has no modifications.

        """
        head = self.HEAD
        if not isinstance(head, Branch):
            raise StatusError(
                "It is not allowed to commit a dataset whose HEAD is not a branch. "
                "Please checkout a branch first"
            )
        modified = head.operations or any(df.operations for df in head.values())
        if not modified:
            raise StatusError("It is not allowed to commit a dataset without any modifications")

        # pylint: disable=protected-access
        draft = self.drafts.create(title, description, head.name)
        logger.info("%s created successfully", draft._repr_head())

        head._upload_to_draft(draft.number, jobs, quiet)
        logger.info("%s uploaded successfully", draft._repr_head())

        branch = draft.commit(title, description)
        logger.info("%s committed successfully", draft._repr_head())
        logger.info("The HEAD of the dataset after commit: \n%s", branch)


class DatasetManager:
    """This class defines the operations on the dataset on Graviti.

    Arguments:
        workspace: Class :class:`~graviti.workspace.Workspace` instance.

    """

    def __init__(self, workspace: "Workspace") -> None:
        self._workspace = workspace

    def _generate(self, offset: int, limit: int) -> Generator[Dataset, None, int]:
        _workspace = self._workspace
        response = list_datasets(_workspace.access_key, _workspace.url, limit=limit, offset=offset)

        for item in response["datasets"]:
            yield Dataset(_workspace, item)

        return response["total_count"]  # type: ignore[no-any-return]

    def create(
        self,
        name: str,
        alias: str = "",
        storage_config: Optional[str] = None,
    ) -> Dataset:
        """Create a Graviti dataset with given name.

        Arguments:
            name: The name of the dataset, unique for a user.
            alias: Alias of the dataset, default is "".
            storage_config: The auth storage config name.

        Returns:
            The created :class:`~graviti.manager.dataset.Dataset` instance.

        """
        _workspace = self._workspace
        response = create_dataset(
            _workspace.access_key,
            _workspace.url,
            name=name,
            alias=alias,
            storage_config=storage_config,
            with_draft=False,
        )

        return Dataset(_workspace, response)

    def get(self, name: str) -> Dataset:
        """Get a Graviti dataset with given name.

        Arguments:
            name: The name of the dataset, unique for a user.

        Returns:
            The requested :class:`~graviti.manager.dataset.Dataset` instance.

        Raises:
            ResourceNameError: When the required dataset does not exist.

        """
        check_type("name", name, str)
        if not name:
            raise ResourceNameError("dataset", name)

        _workspace = self._workspace
        response = get_dataset(_workspace.access_key, _workspace.url, _workspace.name, dataset=name)

        return Dataset(_workspace, response)

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
        check_type("name", name, str)

        _workspace = self._workspace
        delete_dataset(_workspace.access_key, _workspace.url, _workspace.name, name)
