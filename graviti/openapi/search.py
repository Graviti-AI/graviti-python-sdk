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
