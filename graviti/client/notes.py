#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Interfaces about notes."""

from typing import Any, Dict, Optional
from urllib.parse import urljoin

from graviti.utility import URL_PATH_PREFIX, open_api_do


def get_notes(
    url: str,
    access_key: str,
    dataset_id: str,
    *,
    draft_number: Optional[int] = None,
    commit: Optional[str] = None,
) -> Dict[str, Any]:
    """Execute the OpenAPI `GET /v1/datasets{id}/notes`.

    Arguments:
        url: The URL of the graviti website.
        access_key: User's access key.
        dataset_id: Dataset ID.
        draft_number: The draft number.
        commit: The information to locate the specific commit, which can be the commit id,
            the branch name, or the tag name.

    Returns:
        The response of OpenAPI.

    Examples:
        Get notes of the dataset with the given id and commit/draft_number:

        >>> get_notes(
        ...     "https://gas.graviti.com/",
        ...     "ACCESSKEY-********",
        ...     "2bc95d506db2401b898067f1045d7f68",
        ...     commit="main"
        ... )
        {
            "isContinuous": false
        }

    """
    url = urljoin(url, f"{URL_PATH_PREFIX}/datasets/{dataset_id}/notes")
    params: Dict[str, Any] = {}

    if draft_number:
        params["draftNumber"] = draft_number
    if commit:
        params["commit"] = commit

    return open_api_do(url, access_key, "GET", params=params).json()  # type: ignore[no-any-return]
