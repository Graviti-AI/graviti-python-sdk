#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the Commit and CommitManager."""

from typing import TYPE_CHECKING, Any, Dict, Generator, Optional, Tuple, TypeVar

from tensorbay.utility import AttrsMixin, attr

from graviti.dataframe import DataFrame
from graviti.manager.lazy import LazyPagingList
from graviti.manager.sheets import Sheets
from graviti.openapi import (
    create_search,
    get_commit_sheet,
    get_revision,
    list_commit_data,
    list_commit_sheets,
    list_commits,
)
from graviti.portex import PortexType
from graviti.utility import LazyFactory

if TYPE_CHECKING:
    from graviti.manager.dataset import Dataset

ROOT_COMMIT_ID = "00000000000000000000000000000000"


class Commit(Sheets, AttrsMixin):
    """This class defines the structure of a commit.

    Arguments:
        dataset: Class :class:`~graviti.dataset.dataset.Dataset` instance.
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
        dataset: "Dataset",
        commit_id: str,
        parent_commit_id: str,
        title: str,
        description: str,
        committer: str,
        committed_at: str,
    ) -> None:
        self._dataset = dataset
        self.commit_id = commit_id
        self.parent_commit_id = parent_commit_id
        self.title = title
        self.description = description
        self.committer = committer
        self.committed_at = committed_at

    def _repr_head(self) -> str:
        return f'{self.__class__.__name__}("{self.commit_id}")'

    def _list_data(self, offset: int, limit: int, sheet_name: str) -> Dict[str, Any]:
        return list_commit_data(  # type: ignore[no-any-return]
            access_key=self._dataset.access_key,
            url=self._dataset.url,
            owner=self._dataset.owner,
            dataset=self._dataset.name,
            commit_id=self.commit_id,
            sheet=sheet_name,
            offset=offset,
            limit=limit,
        )["data"]

    def _list_sheets(self) -> Dict[str, Any]:
        return list_commit_sheets(
            access_key=self._dataset.access_key,
            url=self._dataset.url,
            owner=self._dataset.owner,
            dataset=self._dataset.name,
            commit_id=self.commit_id,
        )

    def _get_sheet(self, sheet_name: str) -> Dict[str, Any]:
        return get_commit_sheet(
            access_key=self._dataset.access_key,
            url=self._dataset.url,
            owner=self._dataset.owner,
            dataset=self._dataset.name,
            commit_id=self.commit_id,
            sheet=sheet_name,
            with_record_count=True,
        )

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

    def search(self, sheet: str, criteria: Dict[str, Any]) -> DataFrame:
        """Create a search.

        Arguments:
            sheet: The sheet name.
            criteria: The criteria of search.

        Returns:
            The created :class:`~graviti.dataframe.DataFrame` instance.

        """

        def _getter(offset: int, limit: int) -> Dict[str, Any]:
            return create_search(
                self._dataset.access_key,
                self._dataset.url,
                self._dataset.owner,
                self._dataset.name,
                commit_id=self.commit_id,
                sheet=sheet,
                criteria=criteria,
                offset=offset,
                limit=limit,
            )

        total_count = _getter(0, 1)["total_count"]
        schema = PortexType.from_yaml(self._get_sheet(sheet)["schema"])

        factory = LazyFactory(
            total_count,
            128,
            _getter,
            schema.to_pyarrow(),
        )
        paging_lists = factory.create_lists(schema.get_keys())
        return DataFrame._from_paging(paging_lists, schema)  # pylint: disable=protected-access


class NamedCommit(Commit):  # pylint: disable=too-many-instance-attributes
    """This class defines the structure of a named commit.

    :class:`NamedCommit` is the base class of :class:`~graviti.manager.branch.Branch`
    and :class:`~graviti.manager.tag.Tag`.

    Arguments:
        dataset: Class :class:`~graviti.dataset.dataset.Dataset` instance.
        name: The name of the named commit.
        commit_id: The commit id.
        parent_commit_id: The parent commit id.
        title: The commit title.
        description: The commit description.
        committer: The commit user.
        committed_at: The time when the draft is committed.

    """

    _T = TypeVar("_T", bound="NamedCommit")

    _repr_attrs = ("commit_id",) + Commit._repr_attrs

    name: str = attr()

    def __init__(  # pylint: disable=too-many-arguments, super-init-not-called
        self,
        dataset: "Dataset",
        name: str,
        commit_id: str,
        parent_commit_id: str,
        title: str,
        description: str,
        committer: str,
        committed_at: str,
    ) -> None:
        self._dataset = dataset
        self.name = name
        self.commit_id = commit_id
        if self.commit_id == ROOT_COMMIT_ID:
            return

        self.parent_commit_id = parent_commit_id
        self.title = title
        self.description = description
        self.committer = committer
        self.committed_at = committed_at

    def _repr_head(self) -> str:
        return f'{self.__class__.__name__}("{self.name}")'

    def to_pyobj(self) -> Dict[str, str]:
        """Dump the instance to a python dict.

        Returns:
            A python dict containing all the information of the named commit::

                {
                    "name": <str>
                    "commit_id": <str>
                    "parent_commit_id": <str>
                    "title": <str>
                    "description": <str>
                    "committer":  <str>
                    "committed_at": <str>
                }

        """
        return self._dumps()


class CommitManager:
    """This class defines the operations on the commit in Graviti.

    Arguments:
        dataset: :class:`~graviti.manager.dataset.Dataset` instance.

    """

    def __init__(self, dataset: "Dataset") -> None:
        self._dataset = dataset

    def _generate(
        self, revision: Optional[str], offset: int = 0, limit: int = 128
    ) -> Generator[Commit, None, int]:
        if revision is None:
            revision = self._dataset.HEAD.commit_id

        response = list_commits(
            self._dataset.access_key,
            self._dataset.url,
            self._dataset.owner,
            self._dataset.name,
            revision=revision,
            offset=offset,
            limit=limit,
        )

        for item in response["commits"]:
            yield Commit(self._dataset, **item)

        return response["total_count"]  # type: ignore[no-any-return]

    def get(self, revision: Optional[str] = None) -> Commit:
        """Get the certain commit with the given revision.

        Arguments:
            revision: The information to locate the specific commit, which can be the commit id,
                the branch name, or the tag name. If it is not given, get the current commit.

        Returns:
            The :class:`.Commit` instance with the given revision.

        """
        if revision is None:
            revision = self._dataset.HEAD.commit_id

        response = get_revision(
            self._dataset.access_key,
            self._dataset.url,
            self._dataset.owner,
            self._dataset.name,
            revision=revision,
        )
        return Commit(self._dataset, **response)

    def list(self, revision: Optional[str] = None) -> LazyPagingList[Commit]:
        """List the commits.

        Arguments:
            revision: The information to locate the specific commit, which can be the commit id,
                the branch name, or the tag name.
                If it is given, list the commits before the given commit.
                If it is not given, list the commits before the current commit.

        Returns:
            The LazyPagingList of :class:`commits<.Commit>` instances.

        """
        return LazyPagingList(
            lambda offset, limit: self._generate(revision, offset, limit),
            128,
        )
