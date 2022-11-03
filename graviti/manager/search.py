#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the SearchHistory and SearchManager."""

from typing import TYPE_CHECKING, Any, Dict, Generator, Optional

from graviti.manager.commit import Commit
from graviti.manager.common import LIMIT
from graviti.manager.lazy import LazyPagingList
from graviti.openapi import (
    create_search_history,
    delete_search_history,
    get_search_history,
    list_search_histories,
)
from graviti.utility import ReprMixin, check_type, convert_iso_to_datetime

if TYPE_CHECKING:
    from graviti.manager.dataset import Dataset


class SearchHistory(ReprMixin):  # pylint: disable=too-many-instance-attributes
    """This class defines the structure of the search history of Graviti Data Platform.

    Arguments:
        dataset: Class :class:`~graviti.dataset.dataset.Dataset` instance.
        response: The response of the OpenAPI associated with the search history::

                {
                    "id": <str>
                    "commit_id": <str>,
                    "draft_number": <int>,
                    "sheet": <str>,
                    "criteria": <dict>,
                    "total_count": <int>,
                    "creator": <str>,
                    "created_at": <str>,
                },

    Attributes:
        search_id: The id of this search history.
        commit: The commit of this search history.
        draft_number: The draft number of this search history.
        sheet: The sheet name of this search history.
        criteria: The criteria of this search history.
        total_count: The total count of this search history.
        creator: The creator of this search history.
        created_at: The create time of this search history.

    """

    _repr_attrs = (
        "commit",
        "draft_number",
        "sheet",
        "criteria",
        "total_count",
        "creator",
        "created_at",
    )

    def __init__(self, dataset: "Dataset", response: Dict[str, Any]) -> None:
        self._dataset = dataset
        self.search_id = response["id"]

        if "commit_id" in response:
            self.commit = Commit(dataset, response["commit_id"])
        else:
            self.draft_number = response["draft_number"]

        self.sheet = response["sheet"]
        self.criteria = response["criteria"]
        self.total_count = response["total_count"]
        self.creator = response["creator"]
        self.created_at = convert_iso_to_datetime(response["created_at"])

    def _repr_head(self) -> str:
        return f'{self.__class__.__name__}("{self.search_id}")'


class SearchManager:
    """This class defines the operations on the searches on Graviti.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.

    """

    def __init__(self, dataset: "Dataset") -> None:
        self._dataset = dataset

    def _generate(
        self,
        offset: int,
        limit: int,
        *,
        commit_id: Optional[str],
        draft_number: Optional[int],
        sheet: Optional[str],
        order_by: Optional[str],
        sort: Optional[str],
    ) -> Generator[SearchHistory, None, int]:
        _dataset = self._dataset
        response = list_search_histories(
            _dataset.access_key,
            _dataset.url,
            _dataset.owner,
            _dataset.name,
            commit_id=commit_id,
            draft_number=draft_number,
            sheet=sheet,
            order_by=order_by,
            sort=sort,
            limit=limit,
            offset=offset,
        )

        for item in response["searches"]:
            yield SearchHistory(_dataset, item)

        return response["total_count"]  # type: ignore[no-any-return]

    def _create(
        self,
        commit_id: Optional[str],
        draft_number: Optional[int],
        sheet: str,
        criteria: Dict[str, Any],
    ) -> SearchHistory:
        _dataset = self._dataset
        response = create_search_history(
            _dataset.access_key,
            _dataset.url,
            _dataset.owner,
            _dataset.name,
            commit_id=commit_id,
            draft_number=draft_number,
            sheet=sheet,
            criteria=criteria,
        )

        return SearchHistory(_dataset, response)

    def get(self, search_id: str) -> SearchHistory:
        """Get a Graviti search history with given search id.

        Arguments:
            search_id: The id of the search history.

        Returns:
            The requested :class:`~graviti.manager.search.SearchManager` instance.

        """
        check_type("search_id", search_id, str)

        _dataset = self._dataset
        response = get_search_history(
            _dataset.access_key,
            _dataset.url,
            _dataset.owner,
            _dataset.name,
            search_id=search_id,
        )

        return SearchHistory(self._dataset, response)

    def list(
        self,
        *,
        commit_id: Optional[str],
        draft_number: Optional[int],
        sheet: Optional[str],
        order_by: Optional[str],
        sort: Optional[str],
    ) -> LazyPagingList[SearchHistory]:
        """List Graviti search histories.

        Arguments:
            commit_id: The commit id.
            draft_number: The draft number.
            sheet: The name of the sheet.
            order_by: Return the requests ordered by which field, default is "created_at".
            sort: Return the requests sorted in ASC or DESC order, default is DESC.

        Returns:
            The LazyPagingList of :class:`~graviti.manager.search.SearchHistory` instances.

        """
        return LazyPagingList(
            lambda offset, limit: self._generate(
                offset,
                limit,
                commit_id=commit_id,
                draft_number=draft_number,
                sheet=sheet,
                order_by=order_by,
                sort=sort,
            ),
            LIMIT,
        )

    def delete(self, search_id: str) -> None:
        """Delete a Graviti search history with given search id.

        Arguments:
            search_id: The id of the search history.

        """
        check_type("search_id", search_id, str)

        _dataset = self._dataset
        delete_search_history(
            _dataset.access_key, _dataset.url, _dataset.owner, _dataset.name, search_id=search_id
        )
