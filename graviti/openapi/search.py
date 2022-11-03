#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Interfaces about the search."""

from typing import Any, Dict, Optional

from graviti.openapi.requests import open_api_do


def create_search(
    access_key: str,
    url: str,
    owner: str,
    dataset: str,
    *,
    commit_id: str,
    sheet: str,
    criteria: Dict[str, Any],
    search_id: Optional[str] = None,
    offset: Optional[int] = None,
    limit: Optional[int] = None,
) -> Dict[str, Any]:
    """Execute the OpenAPI `POST /v2/datasets/{owner}/{dataset}/commits/{commit_id}/sheets\
    /{sheet}/search`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        owner: The owner of the dataset.
        dataset: Name of the dataset, unique for a user.
        commit_id: The commit id.
        sheet: The sheet name.
        criteria: The criteria of the search.
        search_id: The search id of this search.
        offset: The offset of the page. The default value of this param in OpenAPIv2 is 0.
        limit: The limit of the page. The default value of this param in OpenAPIv2 is to get
            all search results.

    Returns:
        The response of OpenAPI.

    Examples:
        >>> create_search(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com",
        ...     "graviti-example",
        ...     "BDD100K",
        ...     commit_id = "fde63f357daf46088639e9f57fd81cad",
        ...     sheet = "train",
        ...     criteria = {
        ...         "where": {
        ...            "$or": [
        ...                {
        ...                    "$eq": ["$.filename", "0000f77c-6257be58.jpg"],
        ...                },
        ...                {
        ...                    "$and": [
        ...                        {
        ...                            "$eq": ["$.attribute.weather", "clear"]
        ...                        },
        ...                        {
        ...                            "$eq": ["$.attribute.timeofday", "daytime"]
        ...                        }
        ...                    ]
        ...                }
        ...            ]
        ...        },
        ...        "offset": 0,
        ...        "limit": 128
        ...    }
        ... )
        {
            "data": [
                {
                    "__record_key": "123750493121329585",
                    "filename": "0000f77c-6257be58.jpg",
                    "image": {
                        "url": "https://content-store-prod-vers",
                        "checksum": "dcc197970e607f7576d978972f6fb312911ce005"
                    },
                    "attribute": {
                        "weather": "clear",
                        "scene": "city street",
                        "timeofday": "daytime"
                    }
                },
                ...(total 128 items)
            ],
            "search_id": "5c7de503c88446e8b37a258f71783d7d"
        }

        Use search_id to avoid creating new search.

        >>> create_search(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com",
        ...     "graviti-example",
        ...     "BDD100K",
        ...     commit_id = "fde63f357daf46088639e9f57fd81cad",
        ...     sheet = "train",
        ...     criteria = {"where": {"$eq": ["$.filename", "0000f77c-6257be58.jpg"]}}
        ...     search_id = "5c7de503c88446e8b37a258f71783d7d"
        ... )
        {
            "data": [
                {
                    "__record_key": "123750493121329585",
                    "filename": "0000f77c-6257be58.jpg",
                    "image": {
                        "url": "https://content-store-prod-vers",
                        "checksum": "dcc197970e607f7576d978972f6fb312911ce005"
                    },
                    "attribute": {
                        "weather": "clear",
                        "scene": "city street",
                        "timeofday": "daytime"
                    }
                },
                ...(total 128 items)
            ],
            "search_id": "5c7de503c88446e8b37a258f71783d7d"
        }

    """
    url = f"{url}/v2/datasets/{owner}/{dataset}/commits/{commit_id}/sheets/{sheet}/search"

    criteria = criteria.copy()
    if offset is not None:
        criteria["offset"] = offset
    if limit is not None:
        criteria["limit"] = limit
    post_data: Dict[str, Any] = {"criteria": criteria}
    if search_id:
        post_data["search_id"] = search_id

    return open_api_do(  # type: ignore[no-any-return]
        "POST", access_key, url, json=post_data
    ).json()


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
            "total_count": null
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
    order_by: Optional[str] = None,
    sort: Optional[str] = None,
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
        order_by: Return the requests ordered by which field, default is "created_at".
        sort: Return the requests sorted in ASC or DESC order, default is DESC.
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
                    "total_count": 200
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
                    "total_count": 1000
                }
            ],
            "offset": 0,
            "record_size": 2,
            "total_count": 2
        }

    """
    url = f"{url}/v2/datasets/{workspace}/{dataset}/searches"

    params: Dict[str, Any] = {}
    if commit_id is not None:
        params["commit_id"] = commit_id
    if draft_number is not None:
        params["draft_number"] = draft_number
    if sheet is not None:
        params["sheet"] = sheet
    if order_by is not None:
        params["order_by"] = order_by
    if sort is not None:
        params["sort"] = sort
    if offset is not None:
        params["offset"] = offset
    if limit is not None:
        params["limit"] = limit

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
            "total_count": 1000
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


def get_search_total_count(
    access_key: str,
    url: str,
    workspace: str,
    dataset: str,
    *,
    search_id: str,
) -> int:
    """Execute the OpenAPI `GET /v2/datasets/{workspace}/{dataset}/searches/{search_id}\
    /total-count`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        workspace: The name of the workspace.
        dataset: The name of the dataset.
        search_id: The search id.

    Returns:
        The response of OpenAPI.

    Examples:
        >>> get_search_total_count(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com",
        ...     "portex-test",
        ...     "BDD100K",
        ...     search_id="53dbbedf35064f21a7b85def60de840e",
        ... )
        1000

    """
    url = f"{url}/v2/datasets/{workspace}/{dataset}/searches/{search_id}/total_count"

    return open_api_do("GET", access_key, url).json()  # type: ignore[no-any-return]


def list_search_records(
    access_key: str,
    url: str,
    workspace: str,
    dataset: str,
    *,
    search_id: str,
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

    params: Dict[str, Any] = {}
    if offset is not None:
        params["offset"] = offset
    if limit is not None:
        params["limit"] = limit

    return open_api_do("GET", access_key, url, params=params).json()  # type: ignore[no-any-return]
