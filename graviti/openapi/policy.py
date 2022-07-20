#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Interfaces about the dataset policy."""

from typing import Any, Dict, Optional

from graviti.openapi.requests import open_api_do


def get_object_policy(
    access_key: str,
    url: str,
    owner: str,
    dataset: str,
    *,
    actions: str,
    is_internal: Optional[bool] = None,
    expired: Optional[int] = None,
) -> Dict[str, Any]:
    """Execute the OpenAPI `GET /v2/datasets/{owner}/{dataset}/policy/object`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        owner: The owner of the dataset.
        dataset: Name of the dataset, unique for a user.
        actions: The specific actions including "GET" and "PUT". Supports multiple actions,
            which need to be separated by ``|``, like "GET|PUT".
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
        ...     actions="GET",
        ... )
        {
            "backend_type":"OSS",
            "policy": {
                "AccessKeyId":"LTAI4FjgXD3yFJUaasdasd",
                "AccessKeySecret":"LTAI4FjgXD3yFJJKasdad",
                "SecurityToken":"CAISrgJ1q6Ft5B2yfSjIr5bkKILdaseqw",
                "bucket":"content-store-dev",
                "endpoint":"content-store-dev.oss-cn-qingdao.aliyuncs.com",
                "expireAt":"2022-07-12T06:07:52Z"
            }
        }

        Request permission to put dataset data:

        >>> get_policy(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com",
        ...     "graviti-example",
        ...     "MNIST",
        ...     actions="PUT",
        ... )
        {
            "backend_type":"OSS",
            "policy": {
                "AccessKeyId":"LTAI4FjgXD3yFJUaasdasd",
                "AccessKeySecret":"LTAI4FjgXD3yFJJKasdad",
                "SecurityToken":"CAISrgJ1q6Ft5B2yfSjIr5bkKILdaseqw",
                "bucket":"content-store-dev",
                "endpoint":"content-store-dev.oss-cn-qingdao.aliyuncs.com",
                "expireAt":"2022-07-12T06:07:52Z",
                "prefix":"051dd0676cc74f548a7e9b7ace45c26b/"
            }
        }

    """
    url = f"{url}/v2/datasets/{owner}/{dataset}/policy/object"
    params: Dict[str, Any] = {"actions": actions}

    if is_internal is not None:
        params["is_internal"] = is_internal
    if expired is not None:
        params["expired"] = expired

    return open_api_do("GET", access_key, url, params=params).json()  # type: ignore[no-any-return]
