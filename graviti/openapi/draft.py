#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Interfaces about the draft."""

from typing import Any, Dict, Optional
from urllib.parse import urljoin

from graviti.openapi.request import open_api_do


def create_draft(
    url: str,
    access_key: str,
    dataset_name: str,
    title: str,
    *,
    branch_name: Optional[str] = None,
    description: Optional[str] = None,
) -> Dict[str, int]:
    """Execute the OpenAPI `POST /v2/datasets/{dataset_name}/drafts`.

    Arguments:
        url: The URL of the graviti website.
        access_key: User's access key.
        dataset_name: Name of the dataset, unique for a user.
        title: The draft title.
        branch_name: The specified branch name. None means the default branch of the dataset.
        description: The draft description.

    Returns:
        The response of OpenAPI.

    Examples:
        >>> create_draft(
        ...     "https://api.graviti.com/",
        ...     "ACCESSKEY-********",
        ...     "MNIST",
        ...     "draft-2",
        ...     branch_name="main",
        ... )
        {
            "draftNumber": 2
        }

    """
    url = urljoin(url, f"v2/datasets/{dataset_name}/drafts")
    post_data = {"title": title}

    if description:
        post_data["description"] = description
    if branch_name:
        post_data["branch_name"] = branch_name

    return open_api_do(  # type: ignore[no-any-return]
        "POST", access_key, url, json=post_data
    ).json()


def list_drafts(
    url: str,
    access_key: str,
    dataset_name: str,
    *,
    state: Optional[str] = None,
    branch_name: Optional[str] = None,
    offset: int = 0,
    limit: int = 128,
) -> Dict[str, Any]:
    """Execute the OpenAPI `GET /v2/datasets/{dataset_name}/drafts`.

    Arguments:
        url: The URL of the graviti website.
        access_key: User's access key.
        dataset_name: Name of the dataset, unique for a user.
        state: The draft state which includes "OPEN", "CLOSED", "COMMITTED", "ALL" and None.
            None means listing open drafts.
        branch_name: The branch name.
        offset: The offset of the page.
        limit: The limit of the page.

    Returns:
        The response of OpenAPI.

    Examples:
        >>> list_drafts(
        ...     "https://api.graviti.com/",
        ...     "ACCESSKEY-********",
        ...     "MNIST",
        ... )
        {
           "drafts": [
               {
                   "number": 2,
                   "title": "branch-2",
                   "description": "",
                   "branch_name": "main",
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
    url = urljoin(url, f"v2/datasets/{dataset_name}/drafts")
    params: Dict[str, Any] = {"offset": offset, "limit": limit}

    if state:
        params["state"] = state
    if branch_name:
        params["branch_name"] = branch_name

    return open_api_do("GET", access_key, url, params=params).json()  # type: ignore[no-any-return]


def get_draft(url: str, access_key: str, dataset_name: str, draft_number: int) -> Dict[str, Any]:
    """Execute the OpenAPI `GET /v2/datasets/{dataset_name}/drafts/{draft_number}`.

    Arguments:
        url: The URL of the graviti website.
        access_key: User's access key.
        dataset_name: Name of the dataset, unique for a user.
        draft_number: Number of the draft.

    Returns:
        The response of OpenAPI.

    Examples:
        >>> get_draft(
        ...     "https://api.graviti.com/",
        ...     "ACCESSKEY-********",
        ...     "MNIST",
        ...     2,
        ... )
        {
           "number": 2,
           "title": "branch-2",
           "description": "",
           "branch_name": "main",
           "state": "OPEN",
           "parent_commit_id": "85c57a7f03804ccc906632248dc8c359",
           "creator": "czhual",
           "created_at": "2021-03-03T18:58:10Z",
           "updated_at": "2021-03-03T18:58:10Z"
        }

    """
    url = urljoin(url, f"v2/datasets/{dataset_name}/drafts/{draft_number}")
    return open_api_do("GET", access_key, url).json()  # type: ignore[no-any-return]


def update_draft(
    url: str,
    access_key: str,
    dataset_name: str,
    draft_number: int,
    *,
    state: Optional[str] = None,
    title: Optional[str] = None,
    description: Optional[str] = None,
) -> None:
    """Execute the OpenAPI `PATCH /v2/datasets/{dataset_name}/drafts/{draft_number}`.

    Arguments:
        url: The URL of the graviti website.
        access_key: User's access key.
        dataset_name: Name of the dataset, unique for a user.
        draft_number: The updated draft number.
        state: The updated draft state which could be "CLOSED" or None.
            Where None means no change in state.
        title: The draft title.
        description: The draft description.

    Examples:
        Update the title or description of the draft:

        >>> update_draft(
        ...     "https://api.graviti.com/",
        ...     "ACCESSKEY-********",
        ...     "MNIST",
        ...     2,
        ...     title="draft-3"
        ... )

        Close the draft:

        >>> update_draft(
        ...     "https://api.graviti.com/",
        ...     "ACCESSKEY-********",
        ...     "MNIST",
        ...     2,
        ...     state="CLOSED"
        ... )

    """
    url = urljoin(url, f"v2/datasets/{dataset_name}/drafts/{draft_number}")
    patch_data: Dict[str, Any] = {"draft_number": draft_number}

    if state:
        patch_data["state"] = state
    if title is not None:
        patch_data["title"] = title
    if description is not None:
        patch_data["description"] = description

    open_api_do("PATCH", access_key, url, json=patch_data)
