#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Interfaces about the workspace."""

from typing import Any, Dict

from graviti.openapi.requests import open_api_do


def get_current_workspace(access_key: str, url: str) -> Dict[str, Any]:
    """Execute the OpenAPI `GET /v2/current-workspace`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.

    Returns:
        The response of OpenAPI.

    Examples:
        >>> get_current_workspace("ACCESSKEY-********", "https://api.graviti.com")
        {
            "type": "TEAM",
            "id": "41438e9df9a82a194e1e76cc31c1d8d4",
            "name": "portex-test",
            "alias": "PortexTest",
            "description": "xxx",
            "email": "xxxx@xx.com",
            "location": "xxx",
        }

    """
    url = f"{url}/v2/current-workspace"
    return open_api_do("GET", access_key, url).json()  # type: ignore[no-any-return]


def get_workspace(access_key: str, url: str, workspace: str) -> Dict[str, Any]:
    """Execute the OpenAPI `GET /v2/workspaces/{workspace}`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        workspace: The name of the workspace.

    Returns:
        The response of OpenAPI.

    Examples:
        >>> get_workspace("ACCESSKEY-********", "https://api.graviti.com", "portex-test")
        {
            "type": "TEAM",
            "id": "41438e9df9a82a194e1e76cc31c1d8d4",
            "name": "portex-test",
            "alias": "PortexTest",
            "description": "xxx",
            "email": "xxxx@xx.com",
            "location": "xxx",
        }

    """
    url = f"{url}/v2/workspaces/{workspace}"
    return open_api_do("GET", access_key, url).json()  # type: ignore[no-any-return]
