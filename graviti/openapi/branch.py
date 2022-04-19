#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Interfaces about the branch."""

from typing import Any, Dict
from urllib.parse import urljoin

from graviti.openapi.request import open_api_do


def create_branch(
    url: str,
    access_key: str,
    owner: str,
    dataset: str,
    *,
    name: str,
    revision: str,
) -> None:
    """Execute the OpenAPI `POST /v2/datasets/{owner}/{dataset}/branches`.

    Arguments:
        url: The URL of the graviti website.
        access_key: User's access key.
        owner: The owner of the dataset.
        dataset: Name of the dataset, unique for a user.
        name: The name of the branch.
        revision: The information to locate the specific commit, which can be the commit id,
            the branch name, or the tag name.

    Examples:
        >>> create_branch(
        ...     "https://api.graviti.com/",
        ...     "ACCESSKEY-********",
        ...     "czhual",
        ...     "MNIST",
        ...     name="branch-1",
        ...     revision="main"
        ... )

    """
    url = urljoin(url, f"v2/datasets/{owner}/{dataset}/branches")
    post_data = {"name": name, "revision": revision}
    open_api_do("POST", access_key, url, json=post_data)


def list_branches(
    url: str,
    access_key: str,
    owner: str,
    dataset: str,
    *,
    offset: int = 0,
    limit: int = 128,
) -> Dict[str, Any]:
    """Execute the OpenAPI `GET /v2/datasets/{owner}/{dataset}/branches`.

    Arguments:
        url: The URL of the graviti website.
        access_key: User's access key.
        owner: The owner of the dataset.
        dataset: Name of the dataset, unique for a user.
        offset: The offset of the page.
        limit: The limit of the page.

    Returns:
        The response of OpenAPI.

    Examples:
        >>> list_branches(
        ...     "https://api.graviti.com/",
        ...     "ACCESSKEY-********",
        ...     "czhual",
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
                   "committer": "graviti",
                   "committed_at": "2021-03-03T18:58:10Z"
               }
           ],
           "offset": 0,
           "record_size": 1,
           "total_count": 1
        }

    """
    url = urljoin(url, f"v2/datasets/{owner}/{dataset}/branches")
    params = {"offset": offset, "limit": limit}
    return open_api_do("GET", access_key, url, params=params).json()  # type: ignore[no-any-return]


def get_branch(
    url: str,
    access_key: str,
    owner: str,
    dataset: str,
    *,
    branch: str,
) -> Dict[str, Any]:
    """Execute the OpenAPI `GET /v2/datasets/{owner}/{dataset}/branches/{branch}`.

    Arguments:
        url: The URL of the graviti website.
        access_key: User's access key.
        owner: The owner of the dataset.
        dataset: Name of the dataset, unique for a user.
        branch: The name of the branch.

    Returns:
        The response of OpenAPI.

    Examples:
        >>> get_branch(
        ...     "https://api.graviti.com/",
        ...     "ACCESSKEY-********",
        ...     "czhual",
        ...     "MNIST",
        ...     branch="main",
        ... )
        {
            "name": "main",
            "commit_id": "fde63f357daf46088639e9f57fd81cad",
            "parent_commit_id": "f68b1375454f459b8a486b8d1f4d9ddb",
            "title": "first commit",
            "description": "desc",
            "committer": "graviti",
            "committed_at": "2021-03-03T18:58:10Z"
        }

    """
    url = urljoin(url, f"v2/datasets/{owner}/{dataset}/branches/{branch}")
    return open_api_do("GET", access_key, url).json()  # type: ignore[no-any-return]


def delete_branch(
    url: str,
    access_key: str,
    owner: str,
    dataset: str,
    *,
    branch: str,
) -> None:
    """Execute the OpenAPI `DELETE /v2/datasets/{owner}/{dataset}/branches/{branch}`.

    Arguments:
        url: The URL of the graviti website.
        access_key: User's access key.
        owner: The owner of the dataset.
        dataset: Name of the dataset, unique for a user.
        branch: The name of the branch.

    Examples:
        >>> delete_branch(
        ...     "https://api.graviti.com/",
        ...     "ACCESSKEY-********",
        ...     "czhual",
        ...     "MNIST",
        ...     branch="branch-1",
        ... )

    """
    url = urljoin(url, f"v2/datasets/{owner}/{dataset}/branches/{branch}")
    open_api_do("DELETE", access_key, url)
