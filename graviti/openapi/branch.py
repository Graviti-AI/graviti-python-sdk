#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Interfaces about the branch."""

from typing import Any, Dict, Optional

from graviti.openapi.requests import open_api_do


def create_branch(
    access_key: str,
    url: str,
    workspace: str,
    dataset: str,
    *,
    name: str,
    revision: str,
) -> Dict[str, Any]:
    """Execute the OpenAPI `POST /v2/datasets/{workspace}/{dataset}/branches`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        workspace: The workspace of the dataset.
        dataset: Name of the dataset, unique for a user.
        name: The name of the branch.
        revision: The information to locate the specific commit, which can be the commit id,
            the branch name, or the tag name.

    Returns:
        The response of OpenAPI.

    Examples:
        >>> create_branch(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com",
        ...     "graviti-example",
        ...     "MNIST",
        ...     name="branch-1",
        ...     revision="main"
        ... )
        {
            "name": "main",
            "commit_id": "fde63f357daf46088639e9f57fd81cad",
            "parent_commit_id": "f68b1375454f459b8a486b8d1f4d9ddb",
            "title": "first commit",
            "description": "desc",
            "committer": "graviti-example",
            "committed_at": "2021-03-03T18:58:10Z"
        }

    """
    url = f"{url}/v2/datasets/{workspace}/{dataset}/branches"
    post_data = {"name": name, "revision": revision}
    return open_api_do(  # type: ignore[no-any-return]
        "POST", access_key, url, json=post_data
    ).json()


def list_branches(
    access_key: str,
    url: str,
    workspace: str,
    dataset: str,
    *,
    offset: Optional[int] = None,
    limit: Optional[int] = None,
) -> Dict[str, Any]:
    """Execute the OpenAPI `GET /v2/datasets/{workspace}/{dataset}/branches`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        workspace: The workspace of the dataset.
        dataset: Name of the dataset, unique for a user.
        offset: The offset of the page. The default value of this param in OpenAPIv2 is 0.
        limit: The limit of the page. The default value of this param in OpenAPIv2 is 128.

    Returns:
        The response of OpenAPI.

    Examples:
        >>> list_branches(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com",
        ...     "graviti-example",
        ...     "MNIST"
        ... )
        {
           "branches": [
               {
                   "name": "main",
                   "commit_id": "fde63f357daf46088639e9f57fd81cad",
                   "parent_commit_id": "f68b1375454f459b8a486b8d1f4d9ddb",
                   "title": "first commit",
                   "description": "desc",
                   "committer": "graviti-example",
                   "committed_at": "2021-03-03T18:58:10Z"
               }
           ],
           "offset": 0,
           "record_size": 1,
           "total_count": 1
        }

    """
    url = f"{url}/v2/datasets/{workspace}/{dataset}/branches"

    params = {}
    if offset is not None:
        params["offset"] = offset
    if limit is not None:
        params["limit"] = limit

    return open_api_do("GET", access_key, url, params=params).json()  # type: ignore[no-any-return]


def get_branch(
    access_key: str,
    url: str,
    workspace: str,
    dataset: str,
    *,
    branch: str,
) -> Dict[str, Any]:
    """Execute the OpenAPI `GET /v2/datasets/{workspace}/{dataset}/branches/{branch}`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        workspace: The workspace of the dataset.
        dataset: Name of the dataset, unique for a user.
        branch: The name of the branch.

    Returns:
        The response of OpenAPI.

    Examples:
        >>> get_branch(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com",
        ...     "graviti-example",
        ...     "MNIST",
        ...     branch="main",
        ... )
        {
            "name": "main",
            "commit_id": "fde63f357daf46088639e9f57fd81cad",
            "parent_commit_id": "f68b1375454f459b8a486b8d1f4d9ddb",
            "title": "first commit",
            "description": "desc",
            "committer": "graviti-example",
            "committed_at": "2021-03-03T18:58:10Z"
        }

    """
    url = f"{url}/v2/datasets/{workspace}/{dataset}/branches/{branch}"
    return open_api_do("GET", access_key, url).json()  # type: ignore[no-any-return]


def delete_branch(
    access_key: str,
    url: str,
    workspace: str,
    dataset: str,
    *,
    branch: str,
) -> None:
    """Execute the OpenAPI `DELETE /v2/datasets/{workspace}/{dataset}/branches/{branch}`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        workspace: The workspace of the dataset.
        dataset: Name of the dataset, unique for a user.
        branch: The name of the branch.

    Examples:
        >>> delete_branch(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com",
        ...     "graviti-example",
        ...     "MNIST",
        ...     branch="branch-1",
        ... )

    """
    url = f"{url}/v2/datasets/{workspace}/{dataset}/branches/{branch}"
    open_api_do("DELETE", access_key, url)
