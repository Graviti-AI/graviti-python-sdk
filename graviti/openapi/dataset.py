#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Interfaces about the dataset."""

from typing import Any, Dict, Optional

from graviti.openapi.requests import open_api_do


def create_dataset(
    access_key: str,
    url: str,
    name: str,
    *,
    alias: str = "",
    config: Optional[str] = None,
    with_draft: Optional[bool] = None,
) -> Dict[str, Any]:
    """Execute the OpenAPI `POST /v2/datasets`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        name: Name of the dataset, unique for a user.
        alias: Alias of the dataset, default is "".
        config: The auth storage config name.
        with_draft: Whether to create a draft after the dataset is created. The default value of
            this parameter in OpenAPIv2 is False.

    Returns:
        The response of OpenAPI.

    Examples:
        >>> create_dataset(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com",
        ...     "MNIST",
        ... )
        {
           "id": "2bc95d506db2401b898067f1045d7f68",
           "name": "OxfordIIITPet",
           "alias": "",
           "default_branch": "main",
           "commit_id": None,
           "cover_url": "https://tutu.s3.cn-northwest-1.amazonaws.com.cn/",
           "created_at": "2021-03-03T18:58:10Z",
           "updated_at": "2021-03-03T18:58:10Z",
           "owner": "graviti-example",
           "is_public": false,
           "config": "exampleConfigName"
        }

    """
    url = f"{url}/v2/datasets"
    post_data: Dict[str, Any] = {"name": name, "alias": alias}

    if config is not None:
        post_data["config"] = config
    if with_draft is not None:
        post_data["with_draft"] = with_draft

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
        ...     "https://api.graviti.com",
        ...     "graviti-example",
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
           "owner": "graviti-example",
           "is_public": false,
           "config": "exampleConfigName"
        }

    """
    url = f"{url}/v2/datasets/{owner}/{dataset}"
    return open_api_do("GET", access_key, url).json()  # type: ignore[no-any-return]


def list_datasets(
    access_key: str,
    url: str,
    *,
    offset: Optional[int] = None,
    limit: Optional[int] = None,
) -> Dict[str, Any]:
    """Execute the OpenAPI `GET /v2/datasets`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        offset: The offset of the page. The default value of this param in OpenAPIv2 is 0.
        limit: The limit of the page. The default value of this param in OpenAPIv2 is 128.

    Returns:
        The response of OpenAPI.

    Examples:
        >>> list_datasets("ACCESSKEY-********", "https://api.graviti.com")
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
                   "owner": "graviti-example",
                   "is_public": false,
                   "config": "exampleConfigName"
               }
            ],
           "offset": 0,
           "record_size": 1,
           "total_count": 1
        }

    """
    url = f"{url}/v2/datasets"

    params = {}
    if offset is not None:
        params["offset"] = offset
    if limit is not None:
        params["limit"] = limit

    return open_api_do("GET", access_key, url, params=params).json()  # type: ignore[no-any-return]


def update_dataset(
    access_key: str,
    url: str,
    owner: str,
    dataset: str,
    *,
    name: Optional[str] = None,
    alias: Optional[str] = None,
    default_branch: Optional[str] = None,
) -> Dict[str, Any]:
    """Execute the OpenAPI `PATCH /v2/datasets/{owner}/{dataset}`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        owner: The owner of the dataset.
        dataset: Name of the dataset, unique for a user.
        name: New name of the dataset, unique for a user.
        alias: New alias of the dataset.
        default_branch: User's chosen branch.

    Returns:
        The response of OpenAPI.

    Examples:
        >>> update_dataset(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com",
        ...     "graviti-example",
        ...     "OxfordIIITPet",
        ...     name="OxfordIIITPets",
        ...     alias="Oxford-IIIT Pet",
        ...     default_branch="main",
        ... )
        {
           "id": "2bc95d506db2401b898067f1045d7f68",
           "name": "OxfordIIITPets",
           "alias": "Oxford-IIIT Pet",
           "default_branch": "main",
           "commit_id": "a0d4065872f245e4ad1d0d1186e3d397",
           "cover_url": "https://tutu.s3.cn-northwest-1.amazonaws.com.cn/",
           "created_at": "2021-03-03T18:58:10Z",
           "updated_at": "2021-03-04T18:58:10Z",
           "owner": "graviti-example",
           "is_public": false,
           "config": "exampleConfigName"
        }

    """
    url = f"{url}/v2/datasets/{owner}/{dataset}"
    patch_data: Dict[str, Any] = {}

    if name is not None:
        patch_data["name"] = name

    if alias is not None:
        patch_data["alias"] = alias

    if default_branch is not None:
        patch_data["default_branch"] = default_branch

    return open_api_do(  # type: ignore[no-any-return]
        "PATCH", access_key, url, json=patch_data
    ).json()


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
        ...     "https://api.graviti.com",
        ...     "graviti-example",
        ...     "OxfordIIITPet",
        ... )

    """
    url = f"{url}/v2/datasets/{owner}/{dataset}"
    open_api_do("DELETE", access_key, url)
