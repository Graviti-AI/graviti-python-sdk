#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Interfaces about the search."""

from typing import Any, Dict
from urllib.parse import urljoin

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
    offset: int = 0,
    limit: int = 128,
) -> Dict[str, Any]:
    """Execute the OpenAPI `POST /v2/datasets/{owner}/{dataset}/commits/{commit_id}/sheets\
    /{sheet}/search?offset={offset}&limit={limit}`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        owner: The owner of the dataset.
        dataset: Name of the dataset, unique for a user.
        commit_id: The commit id.
        sheet: The sheet name.
        criteria: The criteria of the search.
        offset: The offset of the page.
        limit: The limit of the page.

    Returns:
        The response of OpenAPI.

    Examples:
        >>> create_search(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com/",
        ...     "czhual",
        ...     "BDD100K",
        ...     commit_id = "fde63f357daf46088639e9f57fd81cad",
        ...     sheet = "train",
        ...     criteria = {
        ...         "opt": "or",
        ...         "value": [
        ...             {
        ...                 "opt": "eq",
        ...                 "key": "filename",
        ...                 "value": "0000f77c-6257be58.jpg"
        ...             },
        ...             {
        ...                 "opt": "and",
        ...                 "value": [
        ...                     {
        ...                         "opt": "eq",
        ...                         "key": "attribute.weather",
        ...                         "value": "clear"
        ...                     },
        ...                     {
        ...                         "opt": "eq",
        ...                         "key": "attribute.timeofday",
        ...                         "value": "daytime"
        ...                     }
        ...                 ]
        ...             }
        ...         ]
        ...     }
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
            "offset": 0,
            "record_size": 128,
            "total_count": 200
        }

    """
    url = urljoin(
        url,
        f"v2/datasets/{owner}/{dataset}/commits/{commit_id}/sheets/{sheet}/\
search?offset={offset}&limit={limit}",
    )
    post_data = {"criteria": criteria}

    return open_api_do(  # type: ignore[no-any-return]
        "POST", access_key, url, json=post_data
    ).json()
