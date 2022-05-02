#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Interfaces about the data."""

from typing import Any, Dict, List, Optional
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
        columns: The string of column names separated by ``|``.
            Multiple indexes can be expressed using ``.``. None means to get all columns.
        order_by: The string of column names separated by ``|`` whose order determines the
            precedence of the sort. The rest are sorted by `__record_key` first. Multiple
            indexes can be expressed using ``.``.
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
        ...     order_by = "filename|attribute.weather",
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
        columns: The string of column names separated by ``|``.
            Multiple indexes can be expressed using ``.``. None means to get all columns.
        order_by: The string of column names separated by ``|`` whose order determines the
            precedence of the sort. The rest are sorted by `__record_key` first. Multiple
            indexes can be expressed using ``.``.
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
        ...     order_by = "filename|attribute.weather",
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
    url = urljoin(url, f"v2/datasets/{owner}/{dataset}/commits/{commit_id}/sheets/{sheet}/data")

    return _list_data(
        access_key, url, columns=columns, order_by=order_by, offset=offset, limit=limit
    )


def update_data(
    access_key: str,
    url: str,
    owner: str,
    dataset: str,
    *,
    draft_number: int,
    sheet: str,
    data: List[Dict[str, Any]],
) -> None:
    """Execute the OpenAPI `PATCH /v2/datasets/{owner}/{dataset}/drafts/{draft_number}\
    /sheets/{sheet}/data`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        owner: The owner of the dataset.
        dataset: Name of the dataset, unique for a user.
        draft_number: The draft number.
        sheet: The sheet name.
        data: The update data.

    Examples:
        >>> update_dataset(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com/",
        ...     "czhual",
        ...     "OxfordIIITPet",
        ...     draft_number = 1,
        ...     sheet = "train",
        ...     data = [
        ...         {
        ...             "__record_key": "123750493121329585",
        ...             "filename": "0000f77c-6257be58.jpg",
        ...             "image": {
        ...                 "checksum": "dcc197970e607f7576d978972f6fb312911ce005"
        ...             },
        ...             "attribute": {
        ...                 "weather": "clear",
        ...                 "scene": "city street",
        ...                 "timeofday": "daytime"
        ...             },
        ...         },
        ...         {
        ...             "__record_key": "123750493121329585",
        ...             "filename": "0000f77c-62c2a288.jpg",
        ...             "image": {
        ...                 "checksum": "dcc197970e607f7576d978972f6fb2a2881ce004"
        ...             },
        ...             "attribute": {
        ...                 "weather": "clear",
        ...                 "scene": "highway",
        ...                 "timeofday": "dawn/dusk"
        ...             },
        ...         }
        ...     ],
        ... )

    """
    url = urljoin(url, f"v2/datasets/{owner}/{dataset}/drafts/{draft_number}/sheets/{sheet}/data")
    patch_data = {"data": data}

    open_api_do("PATCH", access_key, url, json=patch_data)


def add_data(
    access_key: str,
    url: str,
    owner: str,
    dataset: str,
    *,
    draft_number: int,
    sheet: str,
    data: List[Dict[str, Any]],
    strategy_arguments: Optional[Dict[str, Any]] = None,
) -> None:
    """Execute the OpenAPI `POST /v2/datasets/{owner}/{dataset}/drafts/{draft_number}\
    /sheets/{sheet}/data`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        owner: The owner of the dataset.
        dataset: Name of the dataset, unique for a user.
        draft_number: The draft number.
        sheet: The sheet name.
        data: The update data.
        strategy_arguments: Arguments required by the ``__record_key`` generation strategy
            of the sheet.

    Examples:
        >>> update_dataset(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com/",
        ...     "czhual",
        ...     "OxfordIIITPet",
        ...     draft_number = 1,
        ...     sheet = "train",
        ...     data = [
        ...         {
        ...             "filename": "0000f77c-6257be58.jpg",
        ...             "image": {
        ...                 "checksum": "dcc197970e607f7576d978972f6fb312911ce005"
        ...             },
        ...             "attribute": {
        ...                 "weather": "clear",
        ...                 "scene": "city street",
        ...                 "timeofday": "daytime"
        ...             },
        ...         },
        ...         {
        ...             "filename": "0000f77c-62c2a288.jpg",
        ...             "image": {
        ...                 "checksum": "dcc197970e607f7576d978972f6fb2a2881ce004"
        ...             },
        ...             "attribute": {
        ...                 "weather": "clear",
        ...                 "scene": "highway",
        ...                 "timeofday": "dawn/dusk"
        ...             },
        ...         }
        ...     ],
        ... )

    """
    url = urljoin(url, f"v2/datasets/{owner}/{dataset}/drafts/{draft_number}/sheets/{sheet}/data")
    post_data: Dict[str, Any] = {"data": data}
    if strategy_arguments is not None:
        post_data["strategy_arguments"] = strategy_arguments

    open_api_do("POST", access_key, url, json=post_data)


def get_policy(
    access_key: str,
    url: str,
    owner: str,
    dataset: str,
    *,
    draft_number: int,
    sheet: str,
    is_internal: Optional[bool] = None,
    expired: Optional[int] = None,
) -> Dict[str, Any]:
    """Execute the OpenAPI `GET /v2/datasets/{owner}/{dataset}/drafts/{draft_number}/\
    sheets/{sheet}/policy`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        owner: The owner of the dataset.
        dataset: Name of the dataset, unique for a user.
        draft_number: The draft number.
        sheet: The sheet name.
        is_internal: Whether to return the intranet upload address, the default value i
            the OpenAPI is False.
        expired: Token expiry time in seconds. It cannot be negative. The default value in
            the OpenAPI is 60. If it is greater than 300, it will be processed as 300.

    Returns:
        The response of OpenAPI.

    Examples:
        >>> get_policy(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com/",
        ...     "czhual",
        ...     "MNIST",
        ...     draft_number = 1,
        ...     sheet = "train",
        ... )
        {
            "result": {
                "oss_access_key_id": "OSSACCESSKEYID",
                "Signature": "QbkCDeZtX37gb2zoemel3VCxz3k=",
                "policy": "eyJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiR",
                "success_action_status": "200",
                "multiple_upload_limit": 10
            },
            "extra": {
                "backend_type": "oss",
                "host": "https://content-store-fat-v",
                "object_prefix": ""
            },
            "expire_at": "2022-03-03T18:58:10Z"
        }

    """
    url = urljoin(url, f"v2/datasets/{owner}/{dataset}/drafts/{draft_number}/sheets/{sheet}/policy")
    params: Dict[str, Any] = {}
    if is_internal is not None:
        params["is_internal"] = is_internal
    if expired is not None:
        params["expired"] = expired

    return open_api_do("GET", access_key, url, params=params).json()  # type: ignore[no-any-return]
