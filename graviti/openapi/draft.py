#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Interfaces about the draft."""

from typing import Any, Dict, Optional
from urllib.parse import urljoin

from graviti.openapi.requests import open_api_do


def create_draft(
    access_key: str,
    url: str,
    owner: str,
    dataset: str,
    *,
    title: str,
    branch: Optional[str] = None,
    description: Optional[str] = None,
) -> Dict[str, Any]:
    """Execute the OpenAPI `POST /v2/datasets/{owner}/{dataset}/drafts`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        owner: The owner of the dataset.
        dataset: Name of the dataset, unique for a user.
        title: The draft title.
        branch: The specified branch name. None means the default branch of the dataset.
        description: The draft description.

    Returns:
        The response of OpenAPI.

    Examples:
        >>> create_draft(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com/",
        ...     "czhual",
        ...     "MNIST",
        ...     title="draft-2",
        ...     branch="main",
        ... )
        {
           "number": 2,
           "title": "draft-2",
           "description": "",
           "branch": "main",
           "state": "OPEN",
           "parent_commit_id": "85c57a7f03804ccc906632248dc8c359",
           "creator": "czhual",
           "created_at": "2021-03-03T18:58:10Z",
           "updated_at": "2021-03-03T18:58:10Z"
        }

    """
    url = urljoin(url, f"v2/datasets/{owner}/{dataset}/drafts")
    post_data = {"title": title}

    if description:
        post_data["description"] = description
    if branch:
        post_data["branch"] = branch

    return open_api_do(  # type: ignore[no-any-return]
        "POST", access_key, url, json=post_data
    ).json()


def list_drafts(
    access_key: str,
    url: str,
    owner: str,
    dataset: str,
    *,
    state: Optional[str] = None,
    branch: Optional[str] = None,
    offset: int = 0,
    limit: int = 128,
) -> Dict[str, Any]:
    """Execute the OpenAPI `GET /v2/datasets/{owner}/{dataset}/drafts`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        owner: The owner of the dataset.
        dataset: Name of the dataset, unique for a user.
        state: The draft state which includes "OPEN", "CLOSED", "COMMITTED", "ALL" and None.
            None means listing open drafts.
        branch: The branch name. None means listing drafts from all branches.
        offset: The offset of the page.
        limit: The limit of the page.

    Returns:
        The response of OpenAPI.

    Examples:
        >>> list_drafts(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com/",
        ...     "czhual",
        ...     "MNIST",
        ... )
        {
           "drafts": [
               {
                   "number": 2,
                   "title": "draft-2",
                   "description": "",
                   "branch": "main",
                   "state": "OPEN",
                   "parent_commit_id": "85c57a7f03804ccc906632248dc8c359",
                   "creator": "czhual",
                   "created_at": "2021-03-03T18:58:10Z",
                   "updated_at": "2021-03-03T18:58:10Z"
               }
           ],
           "offset": 0,
           "record_size": 1,
           "total_count": 1
        }

    """
    url = urljoin(url, f"v2/datasets/{owner}/{dataset}/drafts")
    params: Dict[str, Any] = {"offset": offset, "limit": limit}

    if state:
        params["state"] = state
    if branch:
        params["branch"] = branch

    return open_api_do("GET", access_key, url, params=params).json()  # type: ignore[no-any-return]


def get_draft(
    access_key: str, url: str, owner: str, dataset: str, *, draft_number: int
) -> Dict[str, Any]:
    """Execute the OpenAPI `GET /v2/datasets/{owner}/{dataset}/drafts/{draft_number}`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        owner: The owner of the dataset.
        dataset: Name of the dataset, unique for a user.
        draft_number: Number of the draft.

    Returns:
        The response of OpenAPI.

    Examples:
        >>> get_draft(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com/",
        ...     "MNIST",
        ...     "czhual",
        ...     draft_number=2,
        ... )
        {
           "number": 2,
           "title": "draft-2",
           "description": "",
           "branch": "main",
           "state": "OPEN",
           "parent_commit_id": "85c57a7f03804ccc906632248dc8c359",
           "creator": "czhual",
           "created_at": "2021-03-03T18:58:10Z",
           "updated_at": "2021-03-03T18:58:10Z"
        }

    """
    url = urljoin(url, f"v2/datasets/{owner}/{dataset}/drafts/{draft_number}")
    return open_api_do("GET", access_key, url).json()  # type: ignore[no-any-return]


def update_draft(
    access_key: str,
    url: str,
    owner: str,
    dataset: str,
    *,
    draft_number: int,
    state: Optional[str] = None,
    title: Optional[str] = None,
    description: Optional[str] = None,
) -> None:
    """Execute the OpenAPI `PATCH /v2/datasets/{owner}/{dataset}/drafts/{draft_number}`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        owner: The owner of the dataset.
        dataset: Name of the dataset, unique for a user.
        draft_number: The updated draft number.
        state: The updated draft state which could be "CLOSED" or None.
            Where None means no change in state.
        title: The draft title.
        description: The draft description.

    Examples:
        Update the title or description of the draft:

        >>> update_draft(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com/",
        ...     "MNIST",
        ...     draft_number=2,
        ...     title="draft-3"
        ... )

        Close the draft:

        >>> update_draft(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com/",
        ...     "MNIST",
        ...     draft_number=2,
        ...     state="CLOSED"
        ... )

    """
    url = urljoin(url, f"v2/datasets/{owner}/{dataset}/drafts/{draft_number}")
    patch_data: Dict[str, Any] = {"draft_number": draft_number}

    if state:
        patch_data["state"] = state
    if title is not None:
        patch_data["title"] = title
    if description is not None:
        patch_data["description"] = description

    open_api_do("PATCH", access_key, url, json=patch_data)
