#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Interfaces about the record."""

from typing import Any, Dict, List, Optional, Tuple, Union

from graviti.openapi.requests import open_api_do
from graviti.utility.typing import SortParam


def _list_records(
    access_key: str,
    url: str,
    *,
    columns: Optional[str],
    sort: SortParam,
    offset: Optional[int],
    limit: Optional[int],
) -> Dict[str, Any]:

    params = {
        "columns": columns,
        "sort": sort,
        "offset": offset,
        "limit": limit,
    }

    return open_api_do("GET", access_key, url, params=params).json()  # type: ignore[no-any-return]


def list_draft_records(
    access_key: str,
    url: str,
    workspace: str,
    dataset: str,
    *,
    draft_number: int,
    sheet: str,
    columns: Optional[str] = None,
    sort: SortParam = None,
    offset: Optional[int] = None,
    limit: Optional[int] = None,
) -> Dict[str, Any]:
    """Execute the OpenAPI `GET /v2/datasets/{workspace}/{dataset}/drafts/{draft_number}\
    /sheets/{sheet}/records`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        workspace: The workspace of the dataset.
        dataset: Name of the dataset, unique for a user.
        draft_number: The draft number.
        sheet: The sheet name.
        columns: The string of column names separated by ``,``.
            Multiple indexes can be expressed using ``.``. None means to get all columns.
        sort: The column and the direction the list result sorted by.
        offset: The offset of the page. The default value of this param in OpenAPIv2 is 0.
        limit: The limit of the page. The default value of this param in OpenAPIv2 is 128.

    Returns:
        The response of OpenAPI.

    Examples:
        >>> list_draft_records(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com",
        ...     "graviti-example",
        ...     "MNIST",
        ...     draft_number=1,
        ...     sheet="train",
        ... )
        {
            "records": [
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
    url = f"{url}/v2/datasets/{workspace}/{dataset}/drafts/{draft_number}/sheets/{sheet}/records"

    return _list_records(access_key, url, columns=columns, sort=sort, offset=offset, limit=limit)


def list_commit_records(
    access_key: str,
    url: str,
    workspace: str,
    dataset: str,
    *,
    commit_id: str,
    sheet: str,
    columns: Optional[str] = None,
    sort: SortParam = None,
    offset: Optional[int] = None,
    limit: Optional[int] = None,
) -> Dict[str, Any]:
    """Execute the OpenAPI `GET /v2/datasets/{workspace}/{dataset}/commits/{commit_id}/sheets\
    /{sheet}/records`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        workspace: The workspace of the dataset.
        dataset: Name of the dataset, unique for a user.
        commit_id: The commit id.
        sheet: The sheet name.
        columns: The string of column names separated by ``,``.
            Multiple indexes can be expressed using ``.``. None means to get all columns.
        sort: The column and the direction the list result sorted by.
        offset: The offset of the page. The default value of this param in OpenAPIv2 is 0.
        limit: The limit of the page. The default value of this param in OpenAPIv2 is 128.

    Returns:
        The response of OpenAPI.

    Examples:
        >>> list_commit_records(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com",
        ...     "graviti-example",
        ...     "MNIST",
        ...     commit_id="fde63f357daf46088639e9f57fd81cad",
        ...     sheet="train",
        ... )
        {
            "records": [
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
    url = f"{url}/v2/datasets/{workspace}/{dataset}/commits/{commit_id}/sheets/{sheet}/records"

    return _list_records(access_key, url, columns=columns, sort=sort, offset=offset, limit=limit)


def update_records(
    access_key: str,
    url: str,
    workspace: str,
    dataset: str,
    *,
    draft_number: int,
    sheet: str,
    records: Union[List[Dict[str, Any]], Tuple[Dict[str, Any], ...]],
) -> None:
    """Execute the OpenAPI `PATCH /v2/datasets/{workspace}/{dataset}/drafts/{draft_number}\
    /sheets/{sheet}/records`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        workspace: The workspace of the dataset.
        dataset: Name of the dataset, unique for a user.
        draft_number: The draft number.
        sheet: The sheet name.
        records: The records to be updated.

    Examples:
        >>> update_records(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com",
        ...     "graviti-example",
        ...     "OxfordIIITPet",
        ...     draft_number=1,
        ...     sheet="train",
        ...     records=[
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
    url = f"{url}/v2/datasets/{workspace}/{dataset}/drafts/{draft_number}/sheets/{sheet}/records"
    patch_data = {"records": records}

    open_api_do("PATCH", access_key, url, json=patch_data)


def add_records(
    access_key: str,
    url: str,
    workspace: str,
    dataset: str,
    *,
    draft_number: int,
    sheet: str,
    records: Union[List[Dict[str, Any]], Tuple[Dict[str, Any], ...]],
    strategy_arguments: Optional[Dict[str, Any]] = None,
) -> None:
    """Execute the OpenAPI `POST /v2/datasets/{workspace}/{dataset}/drafts/{draft_number}\
    /sheets/{sheet}/records`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        workspace: The workspace of the dataset.
        dataset: Name of the dataset, unique for a user.
        draft_number: The draft number.
        sheet: The sheet name.
        records: The records to be added.
        strategy_arguments: Arguments required by the ``__record_key`` generation strategy
            of the sheet.

    Examples:
        >>> add_records(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com",
        ...     "graviti-example",
        ...     "OxfordIIITPet",
        ...     draft_number=1,
        ...     sheet="train",
        ...     records=[
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
    url = f"{url}/v2/datasets/{workspace}/{dataset}/drafts/{draft_number}/sheets/{sheet}/records"
    post_data: Dict[str, Any] = {"records": records}
    if strategy_arguments is not None:
        post_data["strategy_arguments"] = strategy_arguments

    open_api_do("POST", access_key, url, json=post_data)


def delete_records(
    access_key: str,
    url: str,
    workspace: str,
    dataset: str,
    *,
    draft_number: int,
    sheet: str,
    record_keys: List[str],
) -> None:
    """Execute the OpenAPI `DELETE /v2/datasets/{workspace}/{dataset}/drafts/{draft_number}\
    /sheets/{sheet}/records`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        workspace: The workspace of the dataset.
        dataset: Name of the dataset, unique for a user.
        draft_number: The draft number.
        sheet: The sheet name.
        record_keys: The keys of the records to be deleted.

    Examples:
        >>> delete_records(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com",
        ...     "graviti-example",
        ...     "OxfordIIITPet",
        ...     draft_number=1,
        ...     sheet="train",
        ...     record_keys=["123750493121329585", "123750493121329586"],
        ... )

    """
    url = f"{url}/v2/datasets/{workspace}/{dataset}/drafts/{draft_number}/sheets/{sheet}/records"
    open_api_do("DELETE", access_key, url, json={"record_keys": record_keys})
