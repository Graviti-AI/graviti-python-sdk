#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Functions about uploading files."""

from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Tuple

import filetype
from requests_toolbelt import MultipartEncoder
from tqdm import tqdm

from graviti.openapi.data import get_policy
from graviti.openapi.requests import do
from graviti.utility import File, config, convert_iso_to_datetime, locked, submit_multithread_tasks

_EXPIRED_IN_SECOND = 240

_PERMISSIONS: Dict[Tuple[str, str, int], Dict[str, Any]] = {}
_DEFAULT_PERMISSION = {"expire_at": datetime.fromtimestamp(0, timezone.utc)}


@locked
def _request_upload_permission(
    access_key: str,
    url: str,
    owner: str,
    dataset: str,
    *,
    draft_number: int,
    sheet: str,
) -> None:
    permission = get_policy(
        access_key,
        url,
        owner,
        dataset,
        draft_number=draft_number,
        sheet=sheet,
        is_internal=config.is_internal,
        expired=_EXPIRED_IN_SECOND,
    )
    permission["expire_at"] = convert_iso_to_datetime(permission.pop("expire_at"))

    _PERMISSIONS[access_key, dataset, draft_number] = permission


def _get_upload_permission(
    access_key: str,
    url: str,
    owner: str,
    dataset: str,
    *,
    draft_number: int,
    sheet: str,
) -> Dict[str, Any]:
    key = (access_key, dataset, draft_number)
    if datetime.now(timezone.utc) >= _PERMISSIONS.get(key, _DEFAULT_PERMISSION)["expire_at"]:
        _request_upload_permission(
            access_key,
            url,
            owner,
            dataset,
            draft_number=draft_number,
            sheet=sheet,
        )

    return deepcopy(_PERMISSIONS[key])


def upload_files(
    access_key: str,
    url: str,
    owner: str,
    dataset: str,
    *,
    draft_number: int,
    sheet: str,
    files: Iterable[File],
    jobs: int,
    pbar: tqdm,
) -> None:
    """Upload multiple local files with multithread.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        owner: The owner of the dataset.
        dataset: Name of the dataset, unique for a user.
        draft_number: The draft number.
        sheet: The sheet name.
        files: The local files to upload.
        jobs: The number of the max workers in multi-thread uploading procession.
        pbar: The process bar for uploading binary files.

    """
    submit_multithread_tasks(
        lambda file: upload_file(
            access_key=access_key,
            url=url,
            owner=owner,
            dataset=dataset,
            draft_number=draft_number,
            sheet=sheet,
            file=file,
            pbar=pbar,
        ),
        files,
        jobs=jobs,
    )


def upload_file(
    access_key: str,
    url: str,
    owner: str,
    dataset: str,
    *,
    draft_number: int,
    sheet: str,
    file: File,
    pbar: tqdm,
) -> None:
    """Upload one local file.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        owner: The owner of the dataset.
        dataset: Name of the dataset, unique for a user.
        draft_number: The draft number.
        sheet: The sheet name.
        file: The local file to upload.
        pbar: The process bar for uploading binary files.

    """
    permission = _get_upload_permission(
        access_key,
        url,
        owner,
        dataset,
        draft_number=draft_number,
        sheet=sheet,
    )

    post_data = permission["result"]

    local_path = file.path
    checksum = file.checksum

    permission_extra = permission["extra"]
    post_data["key"] = permission_extra["object_prefix"] + checksum

    host = permission_extra["host"]
    backend_type = permission_extra["backend_type"]

    if backend_type == "azure":
        url = (
            f'{permission_extra["host"]}{permission_extra["object_prefix"]}'
            f'{checksum}?{permission["result"]["token"]}'
        )
        _put_binary_file_to_azure(url, local_path, post_data)

    elif backend_type == "fps":
        _post_multipart_formdata(
            host,
            local_path,
            post_data,
            checksum,
        )
    elif backend_type == "s3":
        if "signedRequestHeaders" in post_data:
            del post_data["signedRequestHeaders"]

        _post_multipart_formdata(
            host,
            local_path,
            post_data,
        )
    else:
        _post_multipart_formdata(
            host,
            local_path,
            post_data,
        )

    pbar.update()


def _post_multipart_formdata(
    url: str, local_path: Path, data: Dict[str, Any], file_name: str = ""
) -> None:
    with local_path.open("rb") as fp:
        file_type = filetype.guess_mime(local_path)
        if "x-amz-date" in data:
            data["Content-Type"] = file_type

        data["file"] = (file_name, fp, file_type)
        multipart = MultipartEncoder(data)
        do(
            "POST",
            url,
            data=multipart,
            headers={"Content-Type": multipart.content_type},
        )


def _put_binary_file_to_azure(url: str, local_path: Path, data: Dict[str, Any]) -> None:
    with local_path.open("rb") as fp:
        file_type = filetype.guess_mime(local_path)
        request_headers = {
            "x-ms-blob-content-type": file_type,
            "x-ms-blob-type": data["x-ms-blob-type"],
        }
        do("PUT", url, data=fp, headers=request_headers)
