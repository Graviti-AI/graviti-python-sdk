#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Interfaces about the schema."""

from typing import Any, Dict, List, Optional

from graviti.openapi.requests import open_api_do


def update_schema(
    access_key: str,
    url: str,
    owner: str,
    dataset: str,
    *,
    draft_number: int,
    sheet: str,
    patch: Optional[List[Dict[str, Any]]] = None,
    _schema: str,
    _avro_schema: str,
    _arrow_schema: Optional[str] = None,
) -> None:
    """Execute the OpenAPI `PATCH /v2/datasets/{owner}/{dataset}/drafts/{draft_number}\
    /sheets/{sheet}/schema`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        owner: The owner of the dataset.
        dataset: Name of the dataset, unique for a user.
        draft_number: The draft number.
        sheet: The sheet name.
        patch: The list of patch operations which describe the changes to the json portex schema.

    Examples:
        >>> update_schema(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com",
        ...     "Graviti-example",
        ...     "MNIST",
        ...     draft_number = 1,
        ...     sheet = "train",
        ...     _schema = '{"imports": [{"repo": "https://github.com/Project-OpenBytes/standard@\
main", "types": [{"name": "file.Image"}]}], "type": "record", "fields": [{"name": "filename", \
"type": "string"}, {"name": "image", "type": "file.Image"}]}',
        ...     _avro_schema = '{"type": "record", "name": "root", "namespace": "cn.graviti.portex"\
, "aliases": [], "fields": [{"name": "filename", "type": "string"}, {"name": "image", "type": \
{"type": "record", "name": "image", "namespace": "cn.graviti.portex.root", "aliases": [], \
"fields": [{"name": "checksum", "type": [null, "string"]}]}}]}',
        ... )

    """
    url = f"{url}/v2/datasets/{owner}/{dataset}/drafts/{draft_number}/sheets/{sheet}/schema"
    patch_data: Dict[str, Any] = {"_schema": _schema, "_avro_schema": _avro_schema}

    if _arrow_schema is not None:
        patch_data["_arrow_schema"] = _arrow_schema
    if patch is not None:
        patch_data["patch"] = patch

    open_api_do("PATCH", access_key, url, json=patch_data)
