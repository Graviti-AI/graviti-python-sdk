#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the Commit and CommitManager."""

from typing import TYPE_CHECKING, Any, Dict, Generator, Optional, Tuple

from tensorbay.utility import AttrsMixin, attr

from graviti.dataframe import DataFrame
from graviti.exception import NoCommitsError
from graviti.manager.common import CURRENT_COMMIT, check_head_status
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
from graviti.paging import LazyFactory
from graviti.portex import PortexType

if TYPE_CHECKING:
    from graviti.manager.dataset import Dataset


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

    _repr_attrs: Tuple[str, ...] = ("parent_commit_id", "title", "committer", "committed_at")

    commit_id: str = attr()
    parent_commit_id: Optional[str] = attr()
    title: str = attr()
    description: str = attr(default="")
    committer: str = attr()
    committed_at: str = attr()

    def __init__(  # pylint: disable=too-many-arguments
        self,
        dataset: "Dataset",
        commit_id: str,
        parent_commit_id: Optional[str],
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
        if self.commit_id is None:
            raise NoCommitsError("No commit on the current branch. Please commit a draft first")

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
                    "parent_commit_id": <Optional[str]>
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

        Raises:
            NoCommitsError: When there is no commit on the current branch.

        Returns:
            The created :class:`~graviti.dataframe.DataFrame` instance.

        """
        if self.commit_id is None:
            raise NoCommitsError("No commit on the current branch. Please commit a draft first")

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
            lambda offset, limit: _getter(offset, limit)["data"],
            schema.to_pyarrow(),
        )

        return DataFrame._from_factory(factory, schema)  # pylint: disable=protected-access


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

    _repr_attrs = ("commit_id",) + Commit._repr_attrs

    name: str = attr()
    commit_id: Optional[str] = attr()  # type: ignore[assignment]

    def __init__(  # pylint: disable=too-many-arguments, super-init-not-called
        self,
        dataset: "Dataset",
        name: str,
        commit_id: Optional[str],
        parent_commit_id: Optional[str],
        title: str,
        description: str,
        committer: str,
        committed_at: str,
    ) -> None:
        self._dataset = dataset
        self.name = name
        self.commit_id = commit_id
        if self.commit_id is None:
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
                    "commit_id": <Optional[str]>
                    "parent_commit_id": <Optional[str]>
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

    def _generate(self, revision: str, offset: int, limit: int) -> Generator[Commit, None, int]:
        head = self._dataset.HEAD

        response = list_commits(
            self._dataset.access_key,
            self._dataset.url,
            self._dataset.owner,
            self._dataset.name,
            revision=revision,
            offset=offset,
            limit=limit,
        )

        commits = response["commits"]
        if offset == 0 and commits:
            check_head_status(head, revision, commits[0]["commit_id"])

        for item in commits:
            yield Commit(self._dataset, **item)

        return response["total_count"]  # type: ignore[no-any-return]

    def get(self, revision: str = CURRENT_COMMIT) -> Commit:
        """Get the certain commit with the given revision.

        Arguments:
            revision: The information to locate the specific commit, which can be the commit id,
                the branch name, or the tag name. The default value is the current commit of the
                dataset.

        Raises:
            NoCommitsError: When revision is not given and the commit id of current dataset is None,
                or when the given branch has no commit history yet.

        Returns:
            The :class:`.Commit` instance with the given revision.

        """
        head = self._dataset.HEAD
        if revision is CURRENT_COMMIT:
            revision = head.commit_id
            if revision is None:
                raise NoCommitsError("No commits on the default branch yet")

        response = get_revision(
            self._dataset.access_key,
            self._dataset.url,
            self._dataset.owner,
            self._dataset.name,
            revision=revision,
        )
        if response["commit_id"] is None:
            raise NoCommitsError("No commits on the default branch yet")

        del response["type"]
        check_head_status(head, revision, response["commit_id"])

        return Commit(self._dataset, **response)

    def list(self, revision: str = CURRENT_COMMIT) -> LazyPagingList[Commit]:
        """List the commits.

        Arguments:
            revision: The information to locate the specific commit, which can be the commit id,
                the branch name, or the tag name.
                If it is given, list the commits before the given commit.
                If it is not given, list the commits before the current commit.

        Returns:
            The LazyPagingList of :class:`commits<.Commit>` instances.

        """
        if revision is CURRENT_COMMIT:
            revision = self._dataset.HEAD.commit_id
            if revision is None:
                return []  # type: ignore[unreachable]

        return LazyPagingList(lambda offset, limit: self._generate(revision, offset, limit), 24)
