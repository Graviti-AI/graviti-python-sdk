#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Interfaces about the dataset."""

from typing import Any, Dict, Optional
from urllib.parse import urljoin

from graviti.openapi.request import open_api_do


def create_dataset(
    access_key: str,
    url: str,
    name: str,
    *,
    alias: str = "",
    is_public: bool = False,
    config: Optional[str] = None,
) -> Dict[str, Any]:
    """Execute the OpenAPI `POST /v2/datasets`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        name: Name of the dataset, unique for a user.
        alias: Alias of the dataset, default is "".
        is_public: Whether the dataset is a public dataset.
        config: The auth storage config name.

    Returns:
        The response of OpenAPI.

    Examples:
        >>> create_dataset(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com/",
        ...     "MNIST",
        ... )
        {
           "id": "154e35bae8954f09969ef8c9445efd2c"
        }

    """
    url = urljoin(url, "v2/datasets")
    post_data = {
        "name": name,
        "alias": alias,
        "is_public": is_public,
    }
    if config is not None:
        post_data["config"] = config

    return open_api_do(  # type: ignore[no-any-return]
        "POST", access_key, url, json=post_data
    ).json()


def get_dataset(access_key: str, url: str, owner: str, dataset: str) -> Dict[str, Any]:
    """Execute the OpenAPI `GET /v2/datasets/{owner}/{dataset}`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        owner: The owner of the dataset.
        dataset: Name of the dataset, unique for a user.

    Returns:
        The response of OpenAPI.

    Examples:
        >>> get_dataset(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com/",
        ...     "czhual",
        ...     "OxfordIIITPet"
        ... )
        {
           "id": "2bc95d506db2401b898067f1045d7f68",
           "name": "OxfordIIITPet",
           "alias": "Oxford-IIIT Pet",
           "default_branch": "main",
           "commit_id": "a0d4065872f245e4ad1d0d1186e3d397",
           "cover_url": "https://tutu.s3.cn-northwest-1.amazonaws.com.cn/",
           "created_at": "2021-03-03T18:58:10Z",
           "updated_at": "2021-03-03T18:58:10Z",
           "owner": "czhual",
           "is_public": false,
           "config": "exampleConfigName"
        }

    """
    url = urljoin(url, f"v2/datasets/{owner}/{dataset}")
    return open_api_do("GET", access_key, url).json()  # type: ignore[no-any-return]


def list_datasets(
    access_key: str,
    url: str,
    *,
    offset: int = 0,
    limit: int = 128,
) -> Dict[str, Any]:
    """Execute the OpenAPI `GET /v2/datasets`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        offset: The offset of the page.
        limit: The limit of the page.

    Returns:
        The response of OpenAPI.

    Examples:
        >>> list_datasets("ACCESSKEY-********", "https://api.graviti.com/")
        {
           "datasets": [
               {
                   "id": "2bc95d506db2401b898067f1045d7f68",
                   "name": "OxfordIIITPet",
                   "alias": "Oxford-IIIT Pet",
                   "default_branch": "main",
                   "commit_id": "a0d4065872f245e4ad1d0d1186e3d397",
                   "cover_url": "https://tutu.s3.cn-northwest-1.amazonaws.com.cn/",
                   "created_at": "2021-03-03T18:58:10Z",
                   "updated_at": "2021-03-03T18:58:10Z",
                   "owner": "czhual",
                   "is_public": false,
                   "config": "exampleConfigName"
               }
            ],
           "offset": 0,
           "record_size": 1,
           "total_count": 1
        }

    """
    url = urljoin(url, "v2/datasets")
    params: Dict[str, Any] = {"offset": offset, "limit": limit}

    return open_api_do("GET", access_key, url, params=params).json()  # type: ignore[no-any-return]


def update_dataset(
    access_key: str,
    url: str,
    owner: str,
    dataset: str,
    *,
    name: Optional[str] = None,
    alias: Optional[str] = None,
    is_public: Optional[bool] = None,
    default_branch: Optional[str] = None,
) -> None:
    """Execute the OpenAPI `PATCH /v2/datasets/{owner}/{dataset}`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        owner: The owner of the dataset.
        dataset: Name of the dataset, unique for a user.
        name: New name of the dataset, unique for a user.
        alias: New alias of the dataset.
        is_public: Whether the dataset is public.
        default_branch: User's chosen branch.

    Examples:
        >>> update_dataset(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com/",
        ...     "czhual",
        ...     "OxfordIIITPet",
        ...     name="OxfordIIITPets",
        ...     alias="Oxford-IIIT Pet",
        ...     is_public=True,
        ...     default_branch="main",
        ... )

    """
    url = urljoin(url, f"v2/datasets/{owner}/{dataset}")
    patch_data: Dict[str, Any] = {}

    if name is not None:
        patch_data["name"] = name

    if alias is not None:
        patch_data["alias"] = alias

    if is_public is not None:
        patch_data["is_public"] = is_public

    if default_branch is not None:
        patch_data["default_branch"] = default_branch

    open_api_do("PATCH", access_key, url, json=patch_data)


def delete_dataset(
    access_key: str,
    url: str,
    owner: str,
    dataset: str,
) -> None:
    """Execute the OpenAPI `DELETE /v2/datasets/{owner}/{dataset}`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        owner: The owner of the dataset.
        dataset: Name of the dataset, unique for a user.

    Examples:
        >>> delete_dataset(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com/",
        ...     "czhual",
        ...     "OxfordIIITPet",
        ... )

    """
    url = urljoin(url, f"v2/datasets/{owner}/{dataset}")
    open_api_do("DELETE", access_key, url)
