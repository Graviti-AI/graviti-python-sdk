#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Interfaces about the user."""

from typing import Any, Dict

from graviti.openapi.requests import open_api_do


def get_current_user(access_key: str, url: str) -> Dict[str, Any]:
    """Execute the OpenAPI `GET /v2/current-user`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.

    Returns:
        The response of OpenAPI.

    Examples:
        >>> get_current_user("ACCESSKEY-********", "https://api.graviti.com")
        {
            "id": "41438e9df9a82a194e1e76cc31c1d8d4",
            "nickname": "czh ual",
            "email": "********@graviti.com",
            "mobile": null,
            "description": "",
            "workspace": "Graviti-example",
            "team": null
        }

    """
    url = f"{url}/v2/current-user"
    return open_api_do("GET", access_key, url).json()  # type: ignore[no-any-return]
