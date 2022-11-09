#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Interfaces about the actions."""

from typing import Any, Dict, Optional

from requests.models import Response

from graviti.openapi.requests import open_api_do
from graviti.utility import SortParam


def create_action(
    access_key: str,
    url: str,
    workspace: str,
    dataset: str,
    *,
    name: str,
    payload: str,
) -> Dict[str, Any]:
    """Execute the OpenAPI `POST /v2/datasets/{workspace}/{dataset}/actions`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        workspace: The workspace of the dataset.
        dataset: Name of the dataset, unique for a user.
        name: The name of the action.
        payload: The payload of the action.

    Returns:
        The response of OpenAPI.

    """
    url = f"{url}/v2/datasets/{workspace}/{dataset}/actions"
    post_data = {"name": name, "payload": payload}
    return open_api_do(  # type: ignore[no-any-return]
        "POST", access_key, url, json=post_data
    ).json()


def list_actions(
    access_key: str,
    url: str,
    workspace: str,
    dataset: str,
    *,
    sort: SortParam = None,
    offset: Optional[int] = None,
    limit: Optional[int] = None,
) -> Dict[str, Any]:
    """Execute the OpenAPI `GET /v2/datasets/{workspace}/{dataset}/actions`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        workspace: The workspace of the dataset.
        dataset: Name of the dataset, unique for a user.
        sort: The column and the direction the list result sorted by.
        offset: The offset of the page. The default value of this param in OpenAPIv2 is 0.
        limit: The limit of the page. The default value of this param in OpenAPIv2 is 128.

    Returns:
        The response of OpenAPI.

    """
    url = f"{url}/v2/datasets/{workspace}/{dataset}/actions"

    params = {
        "sort": sort,
        "offset": offset,
        "limit": limit,
    }

    return open_api_do("GET", access_key, url, params=params).json()  # type: ignore[no-any-return]


def get_action(
    access_key: str,
    url: str,
    workspace: str,
    dataset: str,
    *,
    action: str,
) -> Dict[str, Any]:
    """Execute the OpenAPI `GET /v2/datasets/{workspace}/{dataset}/actions/{action}`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        workspace: The workspace of the dataset.
        dataset: Name of the dataset, unique for a user.
        action: The name of the aciton.

    Returns:
        The response of OpenAPI.

    """
    url = f"{url}/v2/datasets/{workspace}/{dataset}/actions/{action}"
    return open_api_do("GET", access_key, url).json()  # type: ignore[no-any-return]


def update_action(
    access_key: str,
    url: str,
    workspace: str,
    dataset: str,
    *,
    action: str,
    name: Optional[str] = None,
    state: Optional[str] = None,
    payload: Optional[str] = None,
) -> Dict[str, Any]:
    """Execute the OpenAPI `PATCH /v2/datasets/{workspace}/{dataset}/actions/{action}`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        workspace: The workspace of the dataset.
        dataset: Name of the dataset, unique for a user.
        action: The name of the aciton.
        name: The new name of the action.
        state: The new state of the action.
        payload: The new payload of the action.

    Returns:
        The response of OpenAPI.

    """
    url = f"{url}/v2/datasets/{workspace}/{dataset}/actions/{action}"

    patch_data = {}
    if name is not None:
        patch_data["name"] = name
    if state is not None:
        patch_data["state"] = state
    if payload is not None:
        patch_data["payload"] = payload

    return open_api_do(  # type: ignore[no-any-return]
        "PATCH", access_key, url, json=patch_data
    ).json()


def delete_action(
    access_key: str,
    url: str,
    workspace: str,
    dataset: str,
    *,
    action: str,
) -> None:
    """Execute the OpenAPI `DELETE /v2/datasets/{workspace}/{dataset}/actions/{action}`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        workspace: The workspace of the dataset.
        dataset: Name of the dataset, unique for a user.
        action: The name of the action.

    """
    url = f"{url}/v2/datasets/{workspace}/{dataset}/actions/{action}"
    open_api_do("DELETE", access_key, url)


def create_action_run(
    access_key: str,
    url: str,
    workspace: str,
    dataset: str,
    *,
    action: str,
    arguments: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """Execute the OpenAPI `POST /v2/datasets/{workspace}/{dataset}/actions/{action}/runs`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        workspace: The workspace of the dataset.
        dataset: Name of the dataset, unique for a user.
        action: The name of the action.
        arguments: The arguments of the action run.

    Returns:
        The response of OpenAPI.

    """
    url = f"{url}/v2/datasets/{workspace}/{dataset}/actions/{action}/runs"

    post_data = {"arguments": arguments} if arguments is not None else None

    return open_api_do(  # type: ignore[no-any-return]
        "POST", access_key, url, json=post_data
    ).json()


def list_action_runs(
    access_key: str,
    url: str,
    workspace: str,
    dataset: str,
    *,
    action: str,
    sort: SortParam = None,
    offset: Optional[int] = None,
    limit: Optional[int] = None,
) -> Dict[str, Any]:
    """Execute the OpenAPI `GET /v2/datasets/{workspace}/{dataset}/actions/{action}/runs`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        workspace: The workspace of the dataset.
        dataset: Name of the dataset, unique for a user.
        action: The name of the action.
        sort: The column and the direction the list result sorted by.
        offset: The offset of the page. The default value of this param in OpenAPIv2 is 0.
        limit: The limit of the page. The default value of this param in OpenAPIv2 is 128.

    Returns:
        The response of OpenAPI.

    """
    url = f"{url}/v2/datasets/{workspace}/{dataset}/actions/{action}/runs"

    params = {
        "sort": sort,
        "offset": offset,
        "limit": limit,
    }

    return open_api_do("GET", access_key, url, params=params).json()  # type: ignore[no-any-return]


def get_action_run(
    access_key: str,
    url: str,
    workspace: str,
    dataset: str,
    *,
    action: str,
    run_number: int,
) -> Dict[str, Any]:
    """Execute the OpenAPI `GET /v2/datasets/{workspace}/{dataset}/actions/{action}/runs\
    /{run_number}`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        workspace: The workspace of the dataset.
        dataset: Name of the dataset, unique for a user.
        action: The name of the aciton.
        run_number: The number of the aciton run.

    Returns:
        The response of OpenAPI.

    """
    url = f"{url}/v2/datasets/{workspace}/{dataset}/actions/{action}/runs/{run_number}"
    return open_api_do("GET", access_key, url).json()  # type: ignore[no-any-return]


def cancel_action_run(
    access_key: str,
    url: str,
    workspace: str,
    dataset: str,
    *,
    action: str,
    run_number: int,
) -> Dict[str, Any]:
    """Execute the OpenAPI `POST /v2/datasets/{workspace}/{dataset}/actions/{action}/runs\
    /{run_number}/cancel`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        workspace: The workspace of the dataset.
        dataset: Name of the dataset, unique for a user.
        action: The name of the aciton.
        run_number: The number of the aciton run.

    Returns:
        The response of OpenAPI.

    """
    url = f"{url}/v2/datasets/{workspace}/{dataset}/actions/{action}/runs/{run_number}/cancel"
    return open_api_do("POST", access_key, url).json()  # type: ignore[no-any-return]


def get_action_run_node_log(
    access_key: str,
    url: str,
    workspace: str,
    dataset: str,
    *,
    action: str,
    run_number: int,
    node_id: str,
) -> Response:
    """Execute the OpenAPI `GET /v2/datasets/{workspace}/{dataset}/actions/{action}/runs\
    /{run_number}/nodes/{node_id}/logs`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        workspace: The workspace of the dataset.
        dataset: Name of the dataset, unique for a user.
        action: The name of the aciton.
        run_number: The number of the aciton run.
        node_id: The id of the action run node.

    Returns:
        The response of OpenAPI.

    """
    url = (
        f"{url}/v2/datasets/{workspace}/{dataset}/actions/{action}/runs/{run_number}"
        f"/nodes/{node_id}/logs"
    )

    return open_api_do("GET", access_key, url)
