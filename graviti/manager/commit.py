#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the Commit and CommitManager."""

from typing import TYPE_CHECKING, Dict, Generator, Optional, Tuple, Type, TypeVar

from tensorbay.client.struct import Commit as TensorbayCommit
from tensorbay.utility import AttrsMixin, attr, common_loads

from graviti.client import list_commits
from graviti.exception import ResourceNotExistError
from graviti.manager.lazy import PagingList
from graviti.utility import ReprMixin

if TYPE_CHECKING:
    from graviti.manager.dataset import Dataset

ROOT_COMMIT_ID = "00000000000000000000000000000000"


class Commit(AttrsMixin, ReprMixin):
    """This class defines the structure of a commit.

    Arguments:
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
        commit_id: str,
        parent_commit_id: str,
        title: str,
        description: str,
        committer: str,
        committed_at: str,
    ) -> None:
        self.commit_id = commit_id
        self.parent_commit_id = parent_commit_id
        self.title = title
        self.description = description
        self.committer = committer
        self.committed_at = committed_at

    def _repr_head(self) -> str:
        return f'{self.__class__.__name__}("{self.commit_id}")'

    @classmethod
    def from_pyobj(cls: Type[_T], contents: Dict[str, str]) -> _T:
        """Create a :class:`Commit` instance from python dict.

        Arguments:
            contents: A python dict containing all the information of the commit::

                    {
                        "commit_id": <str>
                        "parent_commit_id": <str>
                        "title": <str>
                        "description": <str>
                        "committer":  <str>
                        "committed_at": <str>
                    }

        Returns:
            A :class:`Commit` instance created from the input python dict.

        """
        return common_loads(cls, contents)

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


class NamedCommit(Commit):
    """This class defines the structure of a named commit.

    :class:`NamedCommit` is the base class of :class:`~graviti.manager.branch.Branch`
    and :class:`~graviti.manager.tag.Tag`.

    Arguments:
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

    def __init__(  # pylint: disable=too-many-arguments
        self,
        name: str,
        commit_id: str,
        parent_commit_id: str,
        title: str,
        description: str,
        committer: str,
        committed_at: str,
    ) -> None:
        super().__init__(commit_id, parent_commit_id, title, description, committer, committed_at)
        self.name = name

    def _repr_head(self) -> str:
        return f'{self.__class__.__name__}("{self.name}")'

    @classmethod
    def from_pyobj(cls: Type[_T], contents: Dict[str, str]) -> _T:
        """Create a :class:`NamedCommit` instance from python dict.

        Arguments:
            contents: A python dict containing all the information of the named commit::

                    {
                        "name": <str>
                        "commit_id": <str>
                        "parent_commit_id": <str>
                        "title": <str>
                        "description": <str>
                        "committer":  <str>
                        "committed_at": <str>
                    }

        Returns:
            A :class:`NamedCommit` instance created from the input python dict.

        """
        return common_loads(cls, contents)

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
        dataset: :class:`~graviti.dataset.dataset.Dataset` instance.

    """

    def __init__(self, dataset: "Dataset") -> None:
        self._dataset = dataset

    def _generate(
        self, revision: Optional[str], offset: int = 0, limit: int = 128
    ) -> Generator[TensorbayCommit, None, int]:
        if revision is None:
            revision = self._dataset.commit_id

        if not revision:
            raise TypeError("The given revision is illegal")

        response = list_commits(
            self._dataset.url,
            self._dataset.access_key,
            self._dataset.dataset_id,
            commit=revision,
            offset=offset,
            limit=limit,
        )

        for item in response["commits"]:
            yield TensorbayCommit.loads(item)

        return response["totalCount"]  # type: ignore[no-any-return]

    def get(self, revision: Optional[str] = None) -> TensorbayCommit:
        """Get the certain commit with the given revision.

        Arguments:
            revision: The information to locate the specific commit, which can be the commit id,
                the branch name, or the tag name. If it is not given, get the current commit.

        Raises:
            ResourceNotExistError: When the required commit does not exist.

        Returns:
            The :class:`.TensorbayCommit` instance with the given revision.

        """
        try:
            commit = next(self._generate(revision))
        except StopIteration as error:
            raise ResourceNotExistError(resource="commit", identification=revision) from error

        return commit

    def list(self, revision: Optional[str] = None) -> PagingList[TensorbayCommit]:
        """List the commits.

        Arguments:
            revision: The information to locate the specific commit, which can be the commit id,
                the branch name, or the tag name.
                If it is given, list the commits before the given commit.
                If it is not given, list the commits before the current commit.

        Returns:
            The PagingList of :class:`commits<.TensorbayCommit>` instances.

        """
        return PagingList(
            lambda offset, limit: self._generate(revision, offset, limit),
            128,
        )
