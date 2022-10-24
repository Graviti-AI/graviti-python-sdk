#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Interfaces about the dataset object."""

from typing import Any, Dict, List, Optional

from graviti.openapi.requests import open_api_do


def get_object_permission(
    access_key: str,
    url: str,
    workspace: str,
    dataset: str,
    *,
    actions: str,
    is_internal: Optional[bool] = None,
    expired: Optional[int] = None,
) -> Dict[str, Any]:
    """Execute the OpenAPI `GET /v2/datasets/{workspace}/{dataset}/objects/permissions`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        workspace: The workspace of the dataset.
        dataset: Name of the dataset, unique for a user.
        actions: The specific actions including "GET" and "PUT". Supports multiple actions,
            which need to be separated by ``|``, like "GET|PUT".
        is_internal: Whether to return the intranet upload address, the default value in
            the OpenAPI is False.
        expired: Token expiry time in seconds. It cannot be negative.

    Returns:
        The response of OpenAPI.

    Examples:
        Request permission to get dataset data from OSS:

        >>> get_object_permission(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com",
        ...     "graviti-example",
        ...     "MNIST",
        ...     actions="GET",
        ... )
        {
            "backend_type":"OSS",
            "expire_at":"2022-07-12T06:07:52Z",
            "permission": {
                "AccessKeyId":"LTAI4FjgXD3yFJUaasdasd",
                "AccessKeySecret":"LTAI4FjgXD3yFJJKasdad",
                "SecurityToken":"CAISrgJ1q6Ft5B2yfSjIr5bkKILdaseqw",
                "bucket":"content-store-dev",
                "endpoint":"content-store-dev.oss-cn-qingdao.aliyuncs.com"
            }
        }

        Request permission to put dataset data to OSS:

        >>> get_object_permission(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com",
        ...     "graviti-example",
        ...     "MNIST",
        ...     actions="PUT",
        ... )
        {
            "backend_type":"OSS",
            "expire_at":"2022-07-12T06:07:52Z",
            "permission": {
                "AccessKeyId":"LTAI4FjgXD3yFJUaasdasd",
                "AccessKeySecret":"LTAI4FjgXD3yFJJKasdad",
                "SecurityToken":"CAISrgJ1q6Ft5B2yfSjIr5bkKILdaseqw",
                "bucket":"content-store-dev",
                "endpoint":"content-store-dev.oss-cn-qingdao.aliyuncs.com",
                "prefix":"051dd0676cc74f548a7e9b7ace45c26b/"
            }
        }

        Request permission to get dataset data from AZURE:

        >>> get_object_permission(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com",
        ...     "graviti-example",
        ...     "MNIST",
        ...     actions="GET",
        ... )
        {
            "backend_type":"AZURE",
            "expire_at":"2022-07-12T06:07:52Z",
            "permission": {
                "container_name":"graviti210304",
                "account_name":"gra220303",
                "sas_param":"se=2022-07-21T10%3A07Z&sig=*******",
                "endpoint_prefix":"https://gra220303.blob.core.window.net/graviti210304/"
            }
        }

        Request permission to put dataset data to AZURE:

        >>> get_object_permission(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com",
        ...     "graviti-example",
        ...     "MNIST",
        ...     actions="PUT",
        ... )
        {
            "backend_type":"AZURE",
            "expire_at":"2022-07-12T06:07:52Z",
            "permission": {
                "container_name":"graviti210304",
                "account_name":"gra220303",
                "prefix":"examplePrefix/",
                "sas_param":"se=2022-07-21T10%3A07Z&sig=*******",
                "endpoint_prefix":"https://gra220303.blob.core.window.net/graviti210304/"
            }
        }

        Request permission to get dataset data from S3:

        >>> get_object_permission(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com",
        ...     "graviti-example",
        ...     "MNIST",
        ...     actions="GET",
        ... )
        {
            "backend_type":"S3",
            "expire_at":"2022-07-12T06:07:52Z",
            "permission": {
                "AccessKeyId":"ASIAQHT******",
                "AccessKeySecret":"Y6x2a2cHIlJdx******",
                "SecurityToken":"FwoGZXIvYXdzEH0aDGYBu******",
                "bucket":"fat-dataplatform",
                "endpoint":"s3.amazonaws.com",
                "region":"us-west-1"
            }
        }

        Request permission to put dataset data to S3:

        >>> get_object_permission(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com",
        ...     "graviti-example",
        ...     "MNIST",
        ...     actions="PUT",
        ... )
        {
            "backend_type":"S3",
            "expire_at":"2022-07-12T06:07:52Z",
            "permission": {
                "AccessKeyId":"ASIAQHT******",
                "AccessKeySecret":"Y6x2a2cHIlJdx******",
                "SecurityToken":"FwoGZXIvYXdzEH0aDGYBu******",
                "bucket":"fat-dataplatform",
                "endpoint":"s3.amazonaws.com",
                "prefix":"051dd0676cc74f548a7e9b7ace45c26b/",
                "region":"us-west-1"
            }
        }

    """
    url = f"{url}/v2/datasets/{workspace}/{dataset}/objects/permissions"
    params: Dict[str, Any] = {"actions": actions}

    if is_internal is not None:
        params["is_internal"] = is_internal
    if expired is not None:
        params["expired"] = expired

    return open_api_do("GET", access_key, url, params=params).json()  # type: ignore[no-any-return]


def copy_objects(
    access_key: str,
    url: str,
    workspace: str,
    target_dataset: str,
    *,
    source_dataset: str,
    keys: List[str],
) -> Dict[str, List[str]]:
    """Execute the OpenAPI `POST /v2/datasets/{workspace}/{target_dataset}/objects/copy`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        workspace: The workspace of the dataset.
        target_dataset: The name of the target dataset.
        source_dataset: The name of the source dataset.
        keys: The keys of the objects which need to be copied.

    Returns:
        The response of OpenAPI.

    Examples:
        >>> copy_objects(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com",
        ...     "graviti-example",
        ...     "MNIST",
        ...     source_dataset="EMINST",
        ...     keys=["xxxxx/xxxxx, xxxxx/xxxxx"]
        ... )
        {
            keys: [
                "yyyyyyy/yyyyyy",
                "yyyyyyy/yyyyyy"
            ]
        }


    """
    url = f"{url}/v2/datasets/{workspace}/{target_dataset}/objects/copy"
    post_data = {"source_dataset": source_dataset, "keys": keys}

    return open_api_do(  # type: ignore[no-any-return]
        "POST", access_key, url, json=post_data
    ).json()
