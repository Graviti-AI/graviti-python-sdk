#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the Commit and CommitManager."""

from datetime import datetime
from functools import partial
from typing import TYPE_CHECKING, Any, Dict, Generator, List, Optional, Tuple, Type, TypeVar, Union

from graviti.dataframe import DataFrame
from graviti.exception import NoCommitsError
from graviti.manager.common import CURRENT_COMMIT, LIMIT, check_head_status
from graviti.manager.lazy import LazyPagingList
from graviti.manager.sheets import Sheets
from graviti.openapi import (
    create_search,
    get_commit,
    get_commit_sheet,
    get_revision,
    list_commit_data,
    list_commit_sheets,
    list_commits,
)
from graviti.operation import SheetOperation
from graviti.paging import LazyLowerCaseFactory
from graviti.portex import PortexRecordBase
from graviti.utility import check_type, convert_iso_to_datetime

if TYPE_CHECKING:
    from graviti.manager.dataset import Dataset

_C = TypeVar("_C", bound="Commit")
_NC = TypeVar("_NC", bound="NamedCommit")


class Commit(Sheets):
    """This class defines the structure of a commit.

    Arguments:
        dataset: Class :class:`~graviti.dataset.dataset.Dataset` instance.
        commit_id: The commit id.

    """

    _repr_attrs: Tuple[str, ...] = ("parent", "title", "committer", "committed_at")

    _commit_info: Dict[str, Any]

    def __init__(
        self,
        dataset: "Dataset",
        commit_id: Optional[str],
    ) -> None:
        self._dataset = dataset
        self.commit_id = commit_id
        self.operations: List[SheetOperation] = []

    def _repr_head(self) -> str:
        return f'{self.__class__.__name__}("{self.commit_id}")'

    def _process_commit_info(self, commit_info: Dict[str, Any]) -> Dict[str, Any]:
        commit_info["committed_at"] = convert_iso_to_datetime(commit_info["committed_at"])
        parent_commit_id = commit_info.pop("parent_commit_id")
        commit_info["parent"] = (
            None if parent_commit_id is None else Commit(self._dataset, parent_commit_id)
        )
        return commit_info

    def _get_commit_info(self) -> Dict[str, Any]:
        if self.commit_id is None:
            raise AttributeError(
                "This attribute is not available due to this branch has no commit history."
            )
        if not hasattr(self, "_commit_info"):
            commit_info = get_commit(
                self._dataset.access_key,
                self._dataset.url,
                self._dataset.owner,
                self._dataset.name,
                commit_id=self.commit_id,
            )
            self._commit_info = self._process_commit_info(commit_info)
        return self._commit_info

    def _list_data(self, offset: int, limit: int, sheet_name: str) -> Dict[str, Any]:
        return list_commit_data(  # type: ignore[no-any-return]
            access_key=self._dataset.access_key,
            url=self._dataset.url,
            owner=self._dataset.owner,
            dataset=self._dataset.name,
            commit_id=self.commit_id,  # type: ignore[arg-type]
            sheet=sheet_name,
            offset=offset,
            limit=limit,
        )["data"]

    def _list_sheets(self) -> Dict[str, Any]:
        if self.commit_id is None:
            return {"sheets": []}

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
            commit_id=self.commit_id,  # type: ignore[arg-type]
            sheet=sheet_name,
            with_record_count=True,
        )

    def _init_dataframe(self, sheet_name: str) -> DataFrame:
        df = super()._init_dataframe(sheet_name)
        df.searcher = partial(self.search, sheet_name)
        return df

    @classmethod
    def from_response(cls: Type[_C], dataset: "Dataset", contents: Dict[str, Any]) -> _C:
        """Create a :class:`Commit` instance from python dict.

        Arguments:
            dataset: The dataset of the commit.
            contents: A python dict containing all the information of the commit::

                {
                    "commit_id": <str>
                    "parent_commit_id": <Optional[str]>
                    "title": <str>
                    "description": <str>
                    "committer":  <str>
                    "committed_at": <str>
                }

        Returns:
            A :class:`Commit` instance created from the input python dict.

        """
        obj = cls(dataset, contents["commit_id"])
        obj._commit_info = obj._process_commit_info(contents.copy())
        return obj

    @property
    def parent(self) -> Optional["Commit"]:
        """Return the parent of the commit.

        Returns:
            The parent of the commit.

        """
        return self._get_commit_info()["parent"]  # type: ignore[no-any-return]

    @property
    def title(self) -> str:
        """Return the title of the commit.

        Returns:
            The title of the commit.

        """
        return self._get_commit_info()["title"]  # type: ignore[no-any-return]

    @property
    def description(self) -> str:
        """Return the description of the commit.

        Returns:
            The description of the commit.

        """
        return self._get_commit_info()["description"]  # type: ignore[no-any-return]

    @property
    def committer(self) -> str:
        """Return the committer of the commit.

        Returns:
            The committer of the commit.

        """
        return self._get_commit_info()["committer"]  # type: ignore[no-any-return]

    @property
    def committed_at(self) -> datetime:
        """Return the time when the draft is committed.

        Returns:
            The time when the draft is committed.

        """
        return self._get_commit_info()["committed_at"]  # type: ignore[no-any-return]

    def search(
        self, sheet: str, criteria: Dict[str, Any], schema: Optional[PortexRecordBase] = None
    ) -> DataFrame:
        """Create a search.

        Arguments:
            sheet: The sheet name.
            criteria: The criteria of search.
            schema: The schema of the search result DataFrame.

        Raises:
            NoCommitsError: When there is no commit on the current branch.

        Returns:
            The created :class:`~graviti.dataframe.DataFrame` instance.

        """
        if self.commit_id is None:
            raise NoCommitsError("No commit on the current branch. Please commit a draft first")

        search_id = create_search(
            self._dataset.access_key,
            self._dataset.url,
            self._dataset.owner,
            self._dataset.name,
            commit_id=self.commit_id,
            sheet=sheet,
            criteria=criteria,
            offset=0,
            limit=1,
        )["search_id"]

        def _getter(
            offset: Optional[int] = None,
            limit: Optional[int] = None,
            getter_criteria: Dict[str, Any] = criteria,
        ) -> List[Dict[str, Any]]:
            return create_search(  # type: ignore[no-any-return]
                self._dataset.access_key,
                self._dataset.url,
                self._dataset.owner,
                self._dataset.name,
                commit_id=self.commit_id,  # type: ignore[arg-type]
                sheet=sheet,
                search_id=search_id,
                criteria=getter_criteria,
                offset=offset,
                limit=limit,
            )["data"]

        count_criteria = criteria.copy()
        count_criteria["select"] = [{"count": {"$count": ["$."]}}]
        total_count = _getter(getter_criteria=count_criteria)[0]["count"]

        if schema is None:
            schema = PortexRecordBase.from_yaml(self._get_sheet(sheet)["schema"])

        factory = LazyLowerCaseFactory(
            total_count,
            LIMIT,
            _getter,
            schema.to_pyarrow(),
        )

        return DataFrame._from_factory(  # pylint: disable=protected-access
            factory, schema, object_permission_manager=self._dataset.object_permission_manager
        )


class NamedCommit(Commit):
    """This class defines the structure of a named commit.

    :class:`NamedCommit` is the base class of :class:`~graviti.manager.branch.Branch`
    and :class:`~graviti.manager.tag.Tag`.

    Arguments:
        dataset: Class :class:`~graviti.dataset.dataset.Dataset` instance.
        name: The name of the named commit.
        commit_id: The commit id.

    """

    _repr_attrs = ("commit_id",) + Commit._repr_attrs

    def __init__(
        self,
        dataset: "Dataset",
        name: str,
        commit_id: Optional[str],
    ) -> None:
        super().__init__(dataset, commit_id)
        self.name = name

    def _repr_head(self) -> str:
        return f'{self.__class__.__name__}("{self.name}")'

    @classmethod
    def from_response(cls: Type[_NC], dataset: "Dataset", contents: Dict[str, Any]) -> _NC:
        """Create a :class:`NamedCommit` instance from python dict.

        Arguments:
            dataset: The dataset of the NamedCommit.
            contents: A python dict containing all the information of the NamedCommit::

                {
                    "name": <str>
                    "commit_id": <Optional[str]>
                    "parent_commit_id": <Optional[str]>
                    "title": <str>
                    "description": <str>
                    "committer":  <str>
                    "committed_at": <str>
                }

        Returns:
            A :class:`NamedCommit` instance created from the input python dict.

        """
        obj = cls(dataset, contents["name"], contents["commit_id"])
        obj._commit_info = obj._process_commit_info(contents.copy())
        return obj


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
            yield Commit.from_response(self._dataset, item)

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
            _revision = head.commit_id
            if _revision is None:
                raise NoCommitsError("No commits on the default branch yet")
        else:
            check_type("revision", revision, str)
            _revision = revision

        response = get_revision(
            self._dataset.access_key,
            self._dataset.url,
            self._dataset.owner,
            self._dataset.name,
            revision=_revision,
        )
        if response["commit_id"] is None:
            raise NoCommitsError("No commits on the default branch yet")

        check_head_status(head, _revision, response["commit_id"])

        return Commit.from_response(self._dataset, response)

    def list(
        self, revision: str = CURRENT_COMMIT
    ) -> Union[LazyPagingList[Commit], List[Optional[Commit]]]:
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
            _revision = self._dataset.HEAD.commit_id
            if _revision is None:
                return []
        else:
            check_type("revision", revision, str)
            _revision = revision

        return LazyPagingList(
            lambda offset, limit: self._generate(
                _revision, offset, limit  # type: ignore[arg-type]
            ),
            LIMIT,
        )
