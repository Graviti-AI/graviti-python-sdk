#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Interfaces about the storage config."""

from typing import Any, Dict, Optional

from graviti.openapi.requests import open_api_do
from graviti.utility import SortParam


def list_storage_configs(
    access_key: str,
    url: str,
    workspace: str,
    *,
    sort: SortParam = None,
    offset: Optional[int] = None,
    limit: Optional[int] = None,
) -> Dict[str, Any]:
    """Execute the OpenAPI `GET /v2/workspaces/{workspace}/storage-configs`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        workspace: The name of the workspace.
        sort: The column and the direction the list result sorted by.
        offset: The offset of the page. The default value of this param in OpenAPIv2 is 0.
        limit: The limit of the page. The default value of this param in OpenAPIv2 is 128.

    Returns:
        The response of OpenAPI.

    Examples:
        >>> list_storage_configs("ACCESSKEY-********", "https://api.graviti.com", "portex-test")
        {
            "default_storage_config": "AliCloud-oss-cn-shanghai",
            "storage_configs": [
                {
                    "id": "2bc95d506db2401b898067f1045d7f60",
                    "name": "AliCloud-oss-cn-shanghai",
                    "config_type": "GRAVITI",
                    "backend_type": "OSS"
                },
                {
                    "id": "2bc95d506db2401b898067f1045d7f61",
                    "name": "OSSConfig",
                    "config_type": "AUTHORIZED",
                    "backend_type": "OSS"
                }
            ],
            "offset": 0,
            "record_size": 2,
            "total_count": 2
        }

    """
    url = f"{url}/v2/workspaces/{workspace}/storage-configs"

    params: Dict[str, Any] = {}
    if sort is not None:
        params["sort"] = sort
    if offset is not None:
        params["offset"] = offset
    if limit is not None:
        params["limit"] = limit

    return open_api_do("GET", access_key, url, params=params).json()  # type: ignore[no-any-return]


def get_storage_config(
    access_key: str, url: str, workspace: str, storage_config: str
) -> Dict[str, Any]:
    """Execute the OpenAPI `GET /v2/workspaces/{workspace}/storage-configs/{storage_config}`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        workspace: The name of the workspace.
        storage_config: The name of the storage config.

    Returns:
        The response of OpenAPI.

    Examples:
        >>> get_storage_config(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com",
        ...     "portex-test",
        ...     "AliCloud-oss-cn-shanghai",
        ... )
        {
            "id": "2bc95d506db2401b898067f1045d7f60",
            "name": "AliCloud-oss-cn-shanghai",
            "config_type": "GRAVITI",
            "backend_type": "OSS",
        }

    """
    url = f"{url}/v2/workspaces/{workspace}/storage-configs/{storage_config}"
    return open_api_do("GET", access_key, url).json()  # type: ignore[no-any-return]


def update_storage_configs(
    access_key: str, url: str, workspace: str, *, default_storage_config: str
) -> None:
    """Execute the OpenAPI `PATCH /v2/workspaces/{workspace}/storage-configs`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        workspace: The name of the workspace.
        default_storage_config: New default storage config.

    Examples:
        >>> update_storage_config(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com",
        ...     "portex-test",
        ...     "AliCloud-oss-cn-shanghai",
        ... )

    """
    url = f"{url}/v2/workspaces/{workspace}/storage-configs"
    patch_data = {"deafault_storage_config": default_storage_config}

    open_api_do("PATCH", access_key, url, json=patch_data)
