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
        Request permission to get dataset data from OSS:

        >>> get_policy(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com",
        ...     "graviti-example",
        ...     "MNIST",
        ...     actions="GET",
        ... )
        {
            "backend_type":"OSS",
            "expire_at":"2022-07-12T06:07:52Z",
            "policy": {
                "AccessKeyId":"LTAI4FjgXD3yFJUaasdasd",
                "AccessKeySecret":"LTAI4FjgXD3yFJJKasdad",
                "SecurityToken":"CAISrgJ1q6Ft5B2yfSjIr5bkKILdaseqw",
                "bucket":"content-store-dev",
                "endpoint":"content-store-dev.oss-cn-qingdao.aliyuncs.com"
            }
        }

        Request permission to put dataset data to OSS:

        >>> get_policy(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com",
        ...     "graviti-example",
        ...     "MNIST",
        ...     actions="PUT",
        ... )
        {
            "backend_type":"OSS",
            "expire_at":"2022-07-12T06:07:52Z",
            "policy": {
                "AccessKeyId":"LTAI4FjgXD3yFJUaasdasd",
                "AccessKeySecret":"LTAI4FjgXD3yFJJKasdad",
                "SecurityToken":"CAISrgJ1q6Ft5B2yfSjIr5bkKILdaseqw",
                "bucket":"content-store-dev",
                "endpoint":"content-store-dev.oss-cn-qingdao.aliyuncs.com",
                "prefix":"051dd0676cc74f548a7e9b7ace45c26b/"
            }
        }

        Request permission to get dataset data from AZURE:

        >>> get_policy(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com",
        ...     "graviti-example",
        ...     "MNIST",
        ...     actions="GET",
        ... )
        {
            "backend_type":"AZURE",
            "expire_at":"2022-07-12T06:07:52Z",
            "policy": {
                "container_name":"graviti210304",
                "account_name":"gra220303",
                "sas_param":"se=2022-07-21T10%3A07Z&sig=*******",
                "endpoint_prefix":"https://gra220303.blob.core.window.net/graviti210304/"
            }
        }

        Request permission to put dataset data to AZURE:

        >>> get_policy(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com",
        ...     "graviti-example",
        ...     "MNIST",
        ...     actions="PUT",
        ... )
        {
            "backend_type":"AZURE",
            "expire_at":"2022-07-12T06:07:52Z",
            "policy": {
                "container_name":"graviti210304",
                "account_name":"gra220303",
                "prefix":"examplePrefix/",
                "sas_param":"se=2022-07-21T10%3A07Z&sig=*******",
                "endpoint_prefix":"https://gra220303.blob.core.window.net/graviti210304/"
            }
        }

        Request permission to get dataset data from S3:

        >>> get_policy(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com",
        ...     "graviti-example",
        ...     "MNIST",
        ...     actions="GET",
        ... )
        {
            "backend_type":"S3",
            "expire_at":"2022-07-12T06:07:52Z",
            "policy": {
                "AccessKeyId":"ASIAQHT******",
                "AccessKeySecret":"Y6x2a2cHIlJdx******",
                "SecurityToken":"FwoGZXIvYXdzEH0aDGYBu******",
                "bucket":"fat-dataplatform",
                "endpoint":"s3.amazonaws.com",
                "region":"us-west-1"
            }
        }

        Request permission to put dataset data to S3:

        >>> get_policy(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com",
        ...     "graviti-example",
        ...     "MNIST",
        ...     actions="PUT",
        ... )
        {
            "backend_type":"S3",
            "expire_at":"2022-07-12T06:07:52Z",
            "policy": {
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
    url = f"{url}/v2/datasets/{owner}/{dataset}/policy/object"
    params: Dict[str, Any] = {"actions": actions}

    if is_internal is not None:
        params["is_internal"] = is_internal
    if expired is not None:
        params["expired"] = expired

    return open_api_do("GET", access_key, url, params=params).json()  # type: ignore[no-any-return]
