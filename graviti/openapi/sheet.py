#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Interfaces about the sheet."""

from typing import Any, Dict, Optional

from graviti.openapi.requests import open_api_do


def _list_sheet(
    access_key: str,
    url: str,
    with_record_count: Optional[bool],
    offset: Optional[int],
    limit: Optional[int],
) -> Dict[str, Any]:

    params: Dict[str, Any] = {}
    if offset is not None:
        params["offset"] = offset
    if limit is not None:
        params["limit"] = limit
    if with_record_count is not None:
        params["with_record_count"] = with_record_count

    return open_api_do("GET", access_key, url, params=params).json()  # type: ignore[no-any-return]


def _get_sheet(
    access_key: str,
    url: str,
    with_record_count: Optional[bool],
    schema_format: Optional[str],
) -> Dict[str, Any]:
    params: Dict[str, Any] = {}

    if schema_format is not None:
        params["schema_format"] = schema_format
    if with_record_count is not None:
        params["with_record_count"] = with_record_count

    return open_api_do("GET", access_key, url, params=params).json()  # type: ignore[no-any-return]


def create_sheet(
    access_key: str,
    url: str,
    owner: str,
    dataset: str,
    *,
    draft_number: int,
    name: str,
    schema: str,
    _avro_schema: str,
    _arrow_schema: Optional[str] = None,
    record_key_strategy: Optional[str] = None,
) -> None:
    """Execute the OpenAPI `POST /v2/datasets/{owner}/{dataset}/drafts/{draft_number}/sheets`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        owner: The owner of the dataset.
        dataset: Name of the dataset, unique for a user.
        draft_number: The draft number.
        name: The sheet name.
        schema: The portex schema of the sheet.
        record_key_strategy: The ``__record_key`` generation strategy.
            If None, it is batch auto-increment sorting record key.

    Examples:
        >>> create_sheet(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com",
        ...     "Graviti-example",
        ...     "MNIST",
        ...     draft_number = 1,
        ...     name = "val",
        ...     schema = '{"imports": [{"repo": "https://github.com/Project-OpenBytes/standard@\
main", "types": [{"name": "file.Image"}]}], "type": "record", "fields": [{"name": "filename", \
"type": "string"}, {"name": "image", "type": "file.Image"}]}',
        ...     _avro_schema = '{"type": "record", "name": "root", "namespace": "cn.graviti.portex"\
, "aliases": [], "fields": [{"name": "filename", "type": "string"}, {"name": "image", "type": \
{"type": "record", "name": "image", "namespace": "cn.graviti.portex.root", "aliases": [], \
"fields": [{"name": "checksum", "type": [null, "string"]}]}}]}',
        ... )

    """
    url = f"{url}/v2/datasets/{owner}/{dataset}/drafts/{draft_number}/sheets"
    post_data = {"name": name, "schema": schema, "_avro_schema": _avro_schema}

    if _arrow_schema is not None:
        post_data["_arrow_schema"] = _arrow_schema
    if record_key_strategy is not None:
        post_data["record_key_strategy"] = record_key_strategy

    open_api_do("POST", access_key, url, json=post_data)


def list_draft_sheets(
    access_key: str,
    url: str,
    owner: str,
    dataset: str,
    *,
    draft_number: int,
    with_record_count: Optional[bool] = None,
    offset: Optional[int] = None,
    limit: Optional[int] = None,
) -> Dict[str, Any]:
    """Execute the OpenAPI `GET /v2/datasets/{owner}/{dataset}/drafts/{draft_number}/sheets`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        owner: The owner of the dataset.
        dataset: Name of the dataset, unique for a user.
        draft_number: The draft number.
        with_record_count: Whether return the record count of each sheet. The default value of
            this param in OpenAPI is False.
        offset: The offset of the page. The default value of this param in OpenAPIv2 is 0.
        limit: The limit of the page. The default value of this param in OpenAPIv2 is 128.

    Returns:
        The response of OpenAPI.

    Examples:
        >>> list_draft_sheets(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com",
        ...     "Graviti-example",
        ...     "MNIST",
        ...     draft_number = 1,
        ... )
        {
            "sheets": [
                {
                    "name": "test",
                    "created_at": "2021-03-03T18:58:10Z",
                    "updated_at": "2021-03-04T18:58:10Z",
                },
                {
                    "name": "trainval",
                    "created_at": "2021-03-05T18:58:10Z",
                    "updated_at": "2021-03-06T18:58:10Z",
                }
            ],
            "offset": 0,
            "record_size": 2,
            "total_count": 2
        }

    """
    url = f"{url}/v2/datasets/{owner}/{dataset}/drafts/{draft_number}/sheets"

    return _list_sheet(
        access_key, url, with_record_count=with_record_count, offset=offset, limit=limit
    )


def list_commit_sheets(
    access_key: str,
    url: str,
    owner: str,
    dataset: str,
    *,
    commit_id: str,
    with_record_count: Optional[bool] = None,
    offset: Optional[int] = None,
    limit: Optional[int] = None,
) -> Dict[str, Any]:
    """Execute the OpenAPI `GET /v2/datasets/{owner}/{dataset}/commits/{commit_id}/sheets`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        owner: The owner of the dataset.
        dataset: Name of the dataset, unique for a user.
        commit_id: The commit id.
        with_record_count: Whether return the record count of each sheet. The default value of
            this param in OpenAPI is False.
        offset: The offset of the page. The default value of this param in OpenAPIv2 is 0.
        limit: The limit of the page. The default value of this param in OpenAPIv2 is 128.

    Returns:
        The response of OpenAPI.

    Examples:
        >>> list_commit_sheets(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com",
        ...     "Graviti-example",
        ...     "MNIST",
        ...     commit_id = "fde63f357daf46088639e9f57fd81cad",
        ... )
        {
            "sheets": [
                {
                    "name": "test",
                    "created_at": "2021-03-03T18:58:10Z",
                    "updated_at": "2021-03-04T18:58:10Z",
                },
                {
                    "name": "trainval",
                    "created_at": "2021-03-05T18:58:10Z",
                    "updated_at": "2021-03-06T18:58:10Z",
                }
            ],
            "offset": 0,
            "record_size": 2,
            "total_count": 2
        }

    """
    url = f"{url}/v2/datasets/{owner}/{dataset}/commits/{commit_id}/sheets"

    return _list_sheet(
        access_key, url, with_record_count=with_record_count, offset=offset, limit=limit
    )


def get_draft_sheet(
    access_key: str,
    url: str,
    owner: str,
    dataset: str,
    *,
    draft_number: int,
    sheet: str,
    with_record_count: Optional[bool] = None,
    schema_format: Optional[str] = None,
) -> Dict[str, Any]:
    """Execute the OpenAPI `GET /v2/datasets/{owner}/{dataset}/drafts/{draft_number}/sheets`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        owner: The owner of the dataset.
        dataset: Name of the dataset, unique for a user.
        draft_number: The draft number.
        sheet: The sheet name.
        with_record_count: Whether return the record count of each sheet. The default value of
            this param in OpenAPI is False.
        schema_format: Fill "JSON"/"YAML" to determine whether the schema_format of the returned
            schema is json or yaml. None means "JSON" format.

    Returns:
        The response of OpenAPI.

    Examples:
        >>> get_draft_sheet(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com",
        ...     "Graviti-example",
        ...     "MNIST",
        ...     draft_number = 1,
        ...     sheet = "sheet-2",
        ...     with_record_count=True,
        ... )
        {
            "name": "trainval",
            "created_at": "2021-03-05T18:58:10Z",
            "updated_at": "2021-03-06T18:58:10Z",
            "record_count": 10000,
            "schema": '{"imports": [{"repo": "https://github.com/Project-OpenBytes/...'
        }

    """
    url = f"{url}/v2/datasets/{owner}/{dataset}/drafts/{draft_number}/sheets/{sheet}"

    return _get_sheet(
        access_key, url, with_record_count=with_record_count, schema_format=schema_format
    )


def get_commit_sheet(
    access_key: str,
    url: str,
    owner: str,
    dataset: str,
    *,
    commit_id: str,
    sheet: str,
    with_record_count: Optional[bool] = None,
    schema_format: Optional[str] = None,
) -> Dict[str, Any]:
    """Execute the OpenAPI `GET /v2/datasets/{owner}/{dataset}/commits/{commit_id}/sheets/{sheet}`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        owner: The owner of the dataset.
        dataset: Name of the dataset, unique for a user.
        commit_id: The commit id..
        sheet: The sheet name.
        with_record_count: Whether return the record count of each sheet. The default value of
            this param in OpenAPI is False.
        schema_format: Fill "JSON"/"YAML" to determine whether the schema_format of the returned
            schema is json or yaml. None means "JSON" format.

    Returns:
        The response of OpenAPI.

    Examples:
        >>> get_commit_sheet(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com",
        ...     "Graviti-example",
        ...     "MNIST",
        ...     commit_id = "fde63f357daf46088639e9f57fd81cad",
        ...     sheet = "sheet-2",
        ...     with_record_count=True,
        ... )
        {
            "name": "trainval",
            "created_at": "2021-03-05T18:58:10Z",
            "updated_at": "2021-03-06T18:58:10Z",
            "record_count": 10000,
            "schema": '{"imports": [{"repo": "https://github.com/Project-OpenBytes/...'
        }

    """
    url = f"{url}/v2/datasets/{owner}/{dataset}/commits/{commit_id}/sheets/{sheet}"

    return _get_sheet(
        access_key, url, with_record_count=with_record_count, schema_format=schema_format
    )


def delete_sheet(
    access_key: str,
    url: str,
    owner: str,
    dataset: str,
    *,
    draft_number: int,
    sheet: str,
) -> None:
    """Execute the OpenAPI `DELETE /v2/datasets/{owner}/{dataset}/drafts/\
    {draft_number}/sheets/{sheet}`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        owner: The owner of the dataset.
        dataset: Name of the dataset, unique for a user.
        draft_number: The draft number.
        sheet: The name of the sheet to be deleted.

    Examples:
        >>> delete_sheet(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com",
        ...     "Graviti-example",
        ...     "MNIST",
        ...     draft_number=1,
        ...     sheet="sheet-2"
        ... )

    """
    url = f"{url}/v2/datasets/{owner}/{dataset}/drafts/{draft_number}/sheets/{sheet}"
    open_api_do("DELETE", access_key, url)
