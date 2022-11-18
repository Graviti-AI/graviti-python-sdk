#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Interfaces about the search."""

from typing import Any, Dict, Optional

from graviti.openapi.requests import open_api_do
from graviti.utility import SortParam


def create_search_history(
    access_key: str,
    url: str,
    workspace: str,
    dataset: str,
    *,
    commit_id: Optional[str] = None,
    draft_number: Optional[int] = None,
    sheet: str,
    criteria: Dict[str, Any],
) -> Dict[str, Any]:
    """Execute the OpenAPI `POST /v2/datasets/{workspace}/{dataset}/searches`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        workspace: The name of the workspace.
        dataset: The name of the dataset.
        commit_id: The commit id.
        draft_number: The draft number.
        sheet: The name of the sheet.
        criteria: The criteria of the search.

    Returns:
        The response of OpenAPI.

    Examples:
        >>> create_search_history(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com",
        ...     "portex-test",
        ...     "BDD100K",
        ...     commit_id="89c33716f3834e188ac1ff749c6c270d",
        ...     sheet="train",
        ...     criteria={"where": {"$any_match": ["$.box2ds", {"$eq": ["$.category", 7]}]}},
        ... )
        {
            "id": "53dbbedf35064f21a7b85def60de840e",
            "commit_id": "89c33716f3834e188ac1ff749c6c270d",
            "sheet": "train",
            "criteria": {
                "where": {
                    "$any_match": ["$.box2ds", { "$eq": ["$.category", 7] }]
                }
            },
            "creator": "linjiX",
            "created_at": "2021-03-05T18:58:10Z",
            "record_count": null
        }

    """
    url = f"{url}/v2/datasets/{workspace}/{dataset}/searches"

    post_data: Dict[str, Any] = {"sheet": sheet, "criteria": criteria}

    if commit_id is not None:
        post_data["commit_id"] = commit_id

    if draft_number is not None:
        post_data["draft_number"] = draft_number

    return open_api_do(  # type: ignore[no-any-return]
        "POST", access_key, url, json=post_data
    ).json()


def list_search_histories(
    access_key: str,
    url: str,
    workspace: str,
    dataset: str,
    *,
    commit_id: Optional[str] = None,
    draft_number: Optional[int] = None,
    sheet: Optional[str] = None,
    sort: SortParam = None,
    offset: Optional[int] = None,
    limit: Optional[int] = None,
) -> Dict[str, Any]:
    """Execute the OpenAPI `GET /v2/datasets/{workspace}/{dataset}/searches`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        workspace: The name of the workspace.
        dataset: The name of the dataset.
        commit_id: The commit id.
        draft_number: The draft number.
        sheet: The name of the sheet.
        sort: The column and the direction the list result sorted by.
        offset: The offset of the page. The default value of this param in OpenAPIv2 is 0.
        limit: The limit of the page. The default value of this param in OpenAPIv2 is 128.

    Returns:
        The response of OpenAPI.

    Examples:
        >>> list_search_histories(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com",
        ...     "portex-test",
        ...     "BDD100K",
        ... )
        {
            "searches": [
                {
                    "id": "a97a31b29c154b82b3049b7f46904b69",
                    "draft_number": 2,
                    "sheet": "test",
                    "criteria": {
                        "where": {
                            "$any_match": ["$.box2ds", { "$eq": ["$.category", 7] }]
                        }
                    },
                    "creator": "linjiX",
                    "created_at": "2021-03-06T18:58:10Z",
                    "record_count": 200
                },
                {
                    "id": "53dbbedf35064f21a7b85def60de840e",
                    "commit_id": "89c33716f3834e188ac1ff749c6c270d",
                    "sheet": "train",
                    "criteria": {
                        "where": {
                            "$any_match": ["$.box2ds", { "$eq": ["$.category", 7] }]
                        }
                    },
                    "creator": "linjiX",
                    "created_at": "2021-03-05T18:58:10Z",
                    "record_count": 1000
                }
            ],
            "offset": 0,
            "record_size": 2,
            "total_count": 2
        }

    """
    url = f"{url}/v2/datasets/{workspace}/{dataset}/searches"

    params = {
        "commit_id": commit_id,
        "draft_number": draft_number,
        "sheet": sheet,
        "sort": sort,
        "offset": offset,
        "limit": limit,
    }

    return open_api_do("GET", access_key, url, params=params).json()  # type: ignore[no-any-return]


def get_search_history(
    access_key: str,
    url: str,
    workspace: str,
    dataset: str,
    *,
    search_id: str,
) -> Dict[str, Any]:
    """Execute the OpenAPI `GET /v2/datasets/{workspace}/{dataset}/searches/{search_id}`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        workspace: The name of the workspace.
        dataset: The name of the dataset.
        search_id: The search id.

    Returns:
        The response of OpenAPI.

    Examples:
        >>> get_search_history(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com",
        ...     "portex-test",
        ...     "BDD100K",
        ...     search_id="53dbbedf35064f21a7b85def60de840e",
        ... )
        {
            "id": "53dbbedf35064f21a7b85def60de840e",
            "commit_id": "89c33716f3834e188ac1ff749c6c270d",
            "sheet": "train",
            "criteria": {
                "where": {
                    "$any_match": ["$.box2ds", { "$eq": ["$.category", 7] }]
                }
            },
            "creator": "linjiX",
            "created_at": "2021-03-05T18:58:10Z",
            "record_count": 1000
        }

    """
    url = f"{url}/v2/datasets/{workspace}/{dataset}/searches/{search_id}"
    return open_api_do("GET", access_key, url).json()  # type: ignore[no-any-return]


def delete_search_history(
    access_key: str,
    url: str,
    workspace: str,
    dataset: str,
    *,
    search_id: str,
) -> None:
    """Execute the OpenAPI `DELETE /v2/datasets/{workspace}/{dataset}/searches/{search_id}`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        workspace: The name of the workspace.
        dataset: The name of the dataset.
        search_id: The search id.

    Examples:
        >>> delete_search_history(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com",
        ...     "portex-test",
        ...     "BDD100K",
        ...     search_id="53dbbedf35064f21a7b85def60de840e",
        ... )

    """
    url = f"{url}/v2/datasets/{workspace}/{dataset}/searches/{search_id}"
    open_api_do("DELETE", access_key, url)


def get_search_record_count(
    access_key: str,
    url: str,
    workspace: str,
    dataset: str,
    *,
    search_id: str,
) -> int:
    """Execute the OpenAPI `GET /v2/datasets/{workspace}/{dataset}/searches/{search_id}\
    /record-count`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        workspace: The name of the workspace.
        dataset: The name of the dataset.
        search_id: The search id.

    Returns:
        The response of OpenAPI.

    Examples:
        >>> get_search_record_count(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com",
        ...     "portex-test",
        ...     "BDD100K",
        ...     search_id="53dbbedf35064f21a7b85def60de840e",
        ... )
        1000

    """
    url = f"{url}/v2/datasets/{workspace}/{dataset}/searches/{search_id}/record-count"

    return open_api_do("GET", access_key, url).json()  # type: ignore[no-any-return]


def list_search_records(
    access_key: str,
    url: str,
    workspace: str,
    dataset: str,
    *,
    search_id: str,
    sort: SortParam = None,
    offset: Optional[int] = None,
    limit: Optional[int] = None,
) -> Dict[str, Any]:
    """Execute the OpenAPI `GET /v2/datasets/{workspace}/{dataset}/searches/{search_id}/records`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        workspace: The name of the workspace.
        dataset: The name of the dataset.
        search_id: The search id.
        sort: The column and the direction the list result sorted by.
        offset: The offset of the page. The default value of this param in OpenAPIv2 is 0.
        limit: The limit of the page. The default value of this param in OpenAPIv2 is 128.

    Returns:
        The response of OpenAPI.

    Examples:
        >>> list_search_records(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com",
        ...     "portex-test",
        ...     "BDD100K",
        ...     search_id="53dbbedf35064f21a7b85def60de840e",
        ... )
        {
            "records": [
                {
                    "__record_key": "2312312312321",
                    "filename": "0000f77c-6257be58.jpg",
                    "image": {
                        "key": "dcc197970e607f7576d978972f6fb312911ce005",
                        "extension": ".jpg",
                        "size": 52344,
                        "width": 800,
                        "heithg": 600
                    },
                    "attribute": {
                        "weather": "clear",
                        "scene": "city street",
                        "timeofday": "daytime"
                    }
                },
                ...(total 5 items)
            ]
        }

    """
    url = f"{url}/v2/datasets/{workspace}/{dataset}/searches/{search_id}/records"

    params: Dict[str, Any] = {
        "sort": sort,
        "offset": offset,
        "limit": limit,
    }

    return open_api_do("GET", access_key, url, params=params).json()  # type: ignore[no-any-return]
