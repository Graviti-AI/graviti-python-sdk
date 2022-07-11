#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Interfaces about the dataset policy."""

from typing import Any, Dict, Optional

from graviti.openapi.requests import open_api_do


def get_policy(
    access_key: str,
    url: str,
    owner: str,
    dataset: str,
    *,
    action: Optional[str] = None,
    is_internal: Optional[bool] = None,
    expired: Optional[int] = None,
) -> Dict[str, str]:
    """Execute the OpenAPI `GET /v2/datasets/{owner}/{dataset}/policy`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        owner: The owner of the dataset.
        dataset: Name of the dataset, unique for a user.
        action: The specific actions including "get" and "put". The default value in the OpenAPI
            is "get". Supports multiple actions, which need to be separated by ``|``, like
            "get|put".
        is_internal: Whether to return the intranet upload address, the default value in
            the OpenAPI is False.
        expired: Token expiry time in seconds. It cannot be negative.

    Returns:
        The response of OpenAPI.

    Examples:
        Request permission to get dataset data:

        >>> get_policy(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com",
        ...     "graviti-example",
        ...     "MNIST",
        ... )
        {
            "access_key_id":"LTAI4FjgXD3yFJUaasdasd",
            "access_key_secret":"LTAI4FjgXD3yFJJKasdad",
            "token":"CAISrgJ1q6Ft5B2yfSjIr5bkKILdaseqw",
            "expired_at":"2022-07-12T06:07:52Z",
            "backend_type":"oss",
            "endpoint":"content-store-dev.oss-cn-qingdao.aliyuncs.com",
            "bucket":"content-store-dev",
        }

        Request permission to put dataset data:

        >>> get_policy(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com",
        ...     "graviti-example",
        ...     "MNIST",
        ...     action="PUT",
        ... )
        {
            "access_key_id":"LTAI4FjgXD3yFJUatsajDS",
            "access_key_secret":"LTAI4FjgXD3yFJJKDShjas",
            "token":"CAISrgJ1q6Ft5B2yfSjIr5bkKILdm7pZ5",
            "expired_at":"2022-07-12T06:06:52Z",
            "backend_type":"oss",
            "endpoint":"content-store-dev.oss-cn-qingdao.aliyuncs.com",
            "bucket":"content-store-dev",
            "object_prefix":"051dd0676cc74f548a7e9b7ace45c26b/"
        }

    """
    url = f"{url}/v2/datasets/{owner}/{dataset}/policy"
    params: Dict[str, Any] = {}

    if action is not None:
        params["action"] = action
    if is_internal is not None:
        params["is_internal"] = is_internal
    if expired is not None:
        params["expired"] = expired

    return open_api_do("GET", access_key, url, params=params).json()  # type: ignore[no-any-return]
