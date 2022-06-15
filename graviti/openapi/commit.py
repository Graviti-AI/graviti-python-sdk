#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Interfaces about the commit."""

from typing import Any, Dict, Optional

from graviti.openapi.requests import open_api_do


def commit_draft(
    access_key: str,
    url: str,
    owner: str,
    dataset: str,
    *,
    draft_number: int,
    title: str,
    description: Optional[str] = None,
) -> Dict[str, str]:
    """Execute the OpenAPI `POST /v2/datasets/{owner}/{dataset}/commits`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        owner: The owner of the dataset.
        dataset: Name of the dataset, unique for a user.
        draft_number: The draft number.
        title: The draft title.
        description: The draft description.

    Returns:
        The response of OpenAPI.

    Examples:
        >>> commit_draft(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com",
        ...     "graviti-example",
        ...     "MNIST",
        ...     draft_number=2,
        ...     title="commit-2",
        ... )
        {
           "commit_id": "85c57a7f03804ccc906632248dc8c359",
           "parent_commit_id": "784ba0d3bf0a41f6a7bfd771d8c00fcb",
           "title": "upload data",
           "description": "",
           "committer": "graviti-example",
           "committed_at": "2021-03-03T18:58:10Z"
        }

    """
    url = f"{url}/v2/datasets/{owner}/{dataset}/commits"
    post_data: Dict[str, Any] = {"draft_number": draft_number, "title": title}

    if description:
        post_data["description"] = description

    return open_api_do(  # type: ignore[no-any-return]
        "POST", access_key, url, json=post_data
    ).json()


def list_commits(
    access_key: str,
    url: str,
    owner: str,
    dataset: str,
    *,
    revision: Optional[str] = None,
    offset: Optional[int] = None,
    limit: Optional[int] = None,
) -> Dict[str, Any]:
    """Execute the OpenAPI `GET /v2/datasets/{owner}/{dataset}/commits`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        owner: The owner of the dataset.
        dataset: Name of the dataset, unique for a user.
        revision: The information to locate the specific commit, which can be the commit id,
            the branch name, or the tag name.
        offset: The offset of the page. The default value of this param in OpenAPIv2 is 0.
        limit: The limit of the page. The default value of this param in OpenAPIv2 is 128.

    Returns:
        The response of OpenAPI.

    Examples:
        >>> list_commits(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com",
        ...     "graviti-example",
        ...     "MNIST",
        ... )
        {
           "commits": [
               {
                   "commit_id": "85c57a7f03804ccc906632248dc8c359",
                   "parent_commitId": "784ba0d3bf0a41f6a7bfd771d8c00fcb",
                   "title": "upload data",
                   "description": "",
                   "committer": "graviti-example",
                   "committed_at": "2021-03-03T18:58:10Z"
               }
           ],
           "offset": 0,
           "record_size": 1,
           "total_count": 1
        }

    """
    url = f"{url}/v2/datasets/{owner}/{dataset}/commits"

    params: Dict[str, Any] = {}
    if offset is not None:
        params["offset"] = offset
    if limit is not None:
        params["limit"] = limit
    if revision is not None:
        params["revision"] = revision

    return open_api_do("GET", access_key, url, params=params).json()  # type: ignore[no-any-return]


def get_commit(
    access_key: str, url: str, owner: str, dataset: str, *, commit_id: str
) -> Dict[str, str]:
    """Execute the OpenAPI `GET /v2/datasets/{owner}/{dataset}/commits/{commit_id}`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        owner: The owner of the dataset.
        dataset: Name of the dataset, unique for a user.
        commit_id: The commit ID.

    Returns:
        The response of OpenAPI.

    Examples:
        >>> get_commit(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com",
        ...     "graviti-example",
        ...     "MNIST",
        ...     commit_id="85c57a7f03804ccc906632248dc8c359"
        ... )
        {
            "commit_id": "85c57a7f03804ccc906632248dc8c359",
            "parent_commit_id": "784ba0d3bf0a41f6a7bfd771d8c00fcb",
            "title": "upload data",
            "description": "",
            "committer": "graviti-example",
            "committed_at": "2021-03-03T18:58:10Z"
        }

    """
    url = f"{url}/v2/{owner}/{dataset}/commits/{commit_id}"
    return open_api_do("GET", access_key, url).json()  # type: ignore[no-any-return]


def get_revision(
    access_key: str, url: str, owner: str, dataset: str, *, revision: str
) -> Dict[str, str]:
    """Execute the OpenAPI `GET /v2/datasets/{owner}/{dataset}/revisions/{revision}`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        owner: The owner of the dataset.
        dataset: Name of the dataset, unique for a user.
        revision: The information to locate the specific commit, which can be the commit id,
            the branch name, or the tag name.

    Returns:
        The response of OpenAPI.

    Examples:
        >>> get_revision(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com",
        ...     "MNIST",
        ...     revision="branch-1"
        ... )
        {
           "type": "BRANCH",
           "commit_id": "85c57a7f03804ccc906632248dc8c359",
           "parent_commit_id": "784ba0d3bf0a41f6a7bfd771d8c00fcb",
           "title": "upload data",
           "description": "",
           "committer": "graviti-example",
           "committed_at": "2021-03-03T18:58:10Z"
        }

    """
    url = f"{url}/v2/datasets/{owner}/{dataset}/revisions/{revision}"
    return open_api_do("GET", access_key, url).json()  # type: ignore[no-any-return]
