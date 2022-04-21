#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Interfaces about the data."""

from typing import Any, Dict, Optional
from urllib.parse import urljoin

from graviti.openapi.requests import open_api_do


def _list_data(
    access_key: str,
    url: str,
    *,
    columns: Optional[str],
    order_by: Optional[str],
    offset: int,
    limit: int,
) -> Dict[str, Any]:

    params: Dict[str, Any] = {"offset": offset, "limit": limit}
    if columns is not None:
        params["columns"] = columns
    if order_by is not None:
        params["order_by"] = order_by

    return open_api_do("GET", access_key, url, params=params).json()  # type: ignore[no-any-return]


def list_draft_data(
    access_key: str,
    url: str,
    owner: str,
    dataset: str,
    *,
    draft_number: int,
    sheet: str,
    columns: Optional[str] = None,
    order_by: Optional[str] = None,
    offset: int = 0,
    limit: int = 128,
) -> Dict[str, Any]:
    """Execute the OpenAPI `GET /v2/datasets/{owner}/{dataset}/drafts/{draft_number}\
    /sheets/{sheet}/data`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        owner: The owner of the dataset.
        dataset: Name of the dataset, unique for a user.
        draft_number: The draft number.
        sheet: The sheet name.
        columns: The string of column names separated by commas.
            Multiple indexes can be expressed using dots. None means to get all columns.
        order_by: The string determine the order of sorting by the order in
            which column names appear.
        offset: The offset of the page.
        limit: The limit of the page.

    Returns:
        The response of OpenAPI.

    Examples:
        >>> list_draft_data(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com/",
        ...     "czhual",
        ...     "MNIST",
        ...     draft_number = 1,
        ...     sheet = "train",
        ... )
        {
            "data": [
                {
                    "filename": "0000f77c-6257be58.jpg",
                    "image": {
                        "url": "https://content-store-prod-vers",
                        "checksum": "dcc197970e607f7576d978972f6fb312911ce005",
                        "type": "remoteFile"
                    },
                    "attribute": {
                        "weather": "clear",
                        "scene": "city street",
                        "timeofday": "daytime"
                    },
                    "box2ds": [
                        {
                            "xmin": 1125.902264,
                            "xmax": 1156.978645,
                            "ymin": 133.184488,
                            "ymax": 210.875445,
                            "category": "traffic light",
                            "attribute": {
                                "occluded": false,
                                "truncated": false,
                                "trafficLightColor": "G"
                            }
                        },
                        {
                            "xmin": 1156.978645,
                            "xmax": 1191.50796,
                            "ymin": 136.637417,
                            "ymax": 210.875443,
                            "category": "traffic light",
                            "attribute": {
                                "occluded": false,
                                "truncated": false,
                                "trafficLightColor": "G"
                            }
                        },
                ...
                    ]
                },
                ...(total 128 items)
            ],
            "offset": 0,
            "record_size": 128,
            "total_count": 70000
        }
    """
    url = urljoin(url, f"v2/datasets/{owner}/{dataset}/drafts/{draft_number}/sheets/{sheet}/data")

    return _list_data(
        access_key, url, columns=columns, order_by=order_by, offset=offset, limit=limit
    )


def list_commit_data(
    access_key: str,
    url: str,
    owner: str,
    dataset: str,
    *,
    commit_id: str,
    sheet: str,
    columns: Optional[str] = None,
    order_by: Optional[str] = None,
    offset: int = 0,
    limit: int = 128,
) -> Dict[str, Any]:
    """Execute the OpenAPI `GET /v2/datasets/{owner}/{dataset}/commits/{commit_id}/sheets\
    /{sheet}/data`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        owner: The owner of the dataset.
        dataset: Name of the dataset, unique for a user.
        commit_id: The commit id.
        sheet: The sheet name.
        columns: The string of column names separated by commas.
            Multiple indexes can be expressed using dots. None means to get all columns.
        order_by: The string determine the order of sorting by  the order in
            which column names appear.
        offset: The offset of the page.
        limit: The limit of the page.

    Returns:
        The response of OpenAPI.

    Examples:
        >>> list_commit_data(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com/",
        ...     "czhual",
        ...     "MNIST",
        ...     commit_id = "fde63f357daf46088639e9f57fd81cad",
        ...     sheet = "train",
        ... )

    """
    url = urljoin(url, f"v2/datasets/{owner}/{dataset}/commits/{commit_id}/sheets/{sheet}/data")

    return _list_data(
        access_key, url, columns=columns, order_by=order_by, offset=offset, limit=limit
    )
