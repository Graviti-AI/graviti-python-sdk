#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the dataset object permission."""

import base64
import hmac
import mimetypes
from datetime import datetime, timezone
from hashlib import sha1, sha256
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Optional
from xml.etree import ElementTree

from graviti.exception import ResponseError
from graviti.openapi import do, get_object_permission
from graviti.utility import UserResponse, config, convert_datetime_to_gmt

if TYPE_CHECKING:
    from graviti.manager import Dataset

_EXPIRED_IN_SECOND = 600


class ObjectPermissionManager:
    """The basic structure of the object permission of the dataset.

    Arguments:
        dataset: Class :class:`~graviti.dataset.dataset.Dataset` instance.

    """

    _get_permission: Optional[Dict[str, Any]] = None
    _put_permission: Optional[Dict[str, Any]] = None

    def __init__(self, dataset: "Dataset") -> None:
        self._dataset = dataset

    def _request_permission(self, actions: str) -> Dict[str, str]:
        _dataset = self._dataset
        return get_object_permission(  # type: ignore[no-any-return]
            _dataset.access_key,
            _dataset.url,
            _dataset.workspace,
            _dataset.name,
            actions=actions,
            is_internal=config.is_internal,
            expired=_EXPIRED_IN_SECOND,
        )["permission"]

    def _clear_get_permission(self) -> None:
        """Clear the get permission."""
        delattr(self, "_get_permission")

    def _clear_put_permission(self) -> None:
        """Clear the put permission."""
        delattr(self, "_put_permission")

    def _init_put_permission(self) -> Dict[str, Any]:
        """Initialize and return the put permission.

        Raises:
            NotImplementedError: The method of the base class should not be called.

        """
        raise NotImplementedError

    @property
    def prefix(self) -> str:
        """Return the prefix of the put permission.

        Returns:
            The prefix of the put permission.

        """
        return self._init_put_permission()["prefix"]  # type: ignore[no-any-return]

    def get_object(self, key: str, _allow_retry: bool = True) -> UserResponse:
        """Get the object from graviti.

        Arguments:
            key: The key of the file.
            _allow_retry: Whether requesting the get permission again is allowed.

        Raises:
            NotImplementedError: The method of the base class should not be called.

        """
        raise NotImplementedError

    def put_object(self, key: str, path: Path, _allow_retry: bool = True) -> None:
        """Put the object to OSS.

        Arguments:
            key: The key of the file.
            path: The path of the file.
            _allow_retry: Whether requesting the put permission again is allowed.

        Raises:
            NotImplementedError: The method of the base class should not be called.

        """
        raise NotImplementedError


class OSSObjectPermissionManager(ObjectPermissionManager):
    """The basic structure of the object permission of the dataset stored in OSS."""

    _RETRY_CODE = {"InvalidAccessKeyId", "AccessDenied"}

    def _init_get_permission(self) -> Dict[str, Any]:
        """Initialize and return the get permission.

        Returns:
            The get permission.

        """
        if self._get_permission is None:
            get_permission: Dict[str, Any] = self._request_permission("GET")
            get_permission["hmac"] = hmac.new(
                get_permission["AccessKeySecret"].encode("utf-8"),
                digestmod=sha1,
            )
            self._get_permission = get_permission
        return self._get_permission

    def _init_put_permission(self) -> Dict[str, Any]:
        """Initialize and return the put permission.

        Returns:
            The put permission.

        """
        if self._put_permission is None:
            put_permission: Dict[str, Any] = self._request_permission("PUT")
            put_permission["hmac"] = hmac.new(
                put_permission["AccessKeySecret"].encode("utf-8"),
                digestmod=sha1,
            )
            self._put_permission = put_permission
        return self._put_permission

    @staticmethod
    def _get_headers(
        permission: Dict[str, Any],
        verb: str,
        key: str,
        mime_type: Optional[str] = None,
    ) -> Dict[str, str]:
        date = convert_datetime_to_gmt(datetime.now(timezone.utc))
        content_type = "" if mime_type is None else mime_type

        signature = (
            f"{verb}\n\n{content_type}\n{date}\n"
            f"x-oss-security-token:{permission['SecurityToken']}\n/{permission['bucket']}/{key}"
        )
        _hmac = permission["hmac"].copy()
        _hmac.update(signature.encode("utf-8"))
        signature = base64.b64encode(_hmac.digest()).decode("utf-8")
        authorization = f"OSS {permission['AccessKeyId']}:{signature}"

        headers = {
            "Authorization": authorization,
            "x-oss-security-token": permission["SecurityToken"],
            "Date": date,
        }
        if mime_type is not None:
            headers["Content-Type"] = mime_type
        return headers

    def get_object(self, key: str, _allow_retry: bool = True) -> UserResponse:
        """Get the object from OSS.

        Arguments:
            key: The key of the file.
            _allow_retry: Whether requesting the get permission again is allowed.

        Raises:
            ResponseError: If post response error.

        Returns:
            The response of OSS get object API.

        """
        permission = self._init_get_permission()
        verb = "GET"

        headers = self._get_headers(permission, verb, key)
        url = f"https://{permission['bucket']}.{permission['endpoint']}/{key}"

        try:
            response = do(verb, url, headers=headers, timeout=config.timeout, stream=True)
            return UserResponse(response)
        except ResponseError as error:
            code = ElementTree.fromstring(error.response.text)[0].text
            if _allow_retry and code in self._RETRY_CODE:
                self._clear_get_permission()
                return self.get_object(key, False)
            raise error from None

    def put_object(self, key: str, path: Path, _allow_retry: bool = True) -> None:
        """Put the object to OSS.

        Arguments:
            key: The key of the file.
            path: The path of the file.
            _allow_retry: Whether requesting the put permission again is allowed.

        Raises:
            ResponseError: If post response error.

        """
        permission = self._init_put_permission()
        verb = "PUT"

        mime_type = mimetypes.guess_type(path)[0]
        headers: Dict[str, Any] = self._get_headers(permission, verb, key, mime_type)
        url = f"https://{permission['bucket']}.{permission['endpoint']}/{key}"

        try:
            with path.open("rb") as fp:
                do(verb, url, headers=headers, data=fp)
        except ResponseError as error:
            code = ElementTree.fromstring(error.response.text)[0].text
            if _allow_retry and code in self._RETRY_CODE:
                self._clear_put_permission()
                self.put_object(key, path, False)
                return

            raise error from None


class AZUREObjectPermissionManager(ObjectPermissionManager):
    """The basic structure of the object permission of the dataset stored in AZURE."""

    _RETRY_CODE = "AuthenticationFailed"

    def _init_get_permission(self) -> Dict[str, Any]:
        """Initialize and return the get permission.

        Returns:
            The get permission.

        """
        if self._get_permission is None:
            self._get_permission = self._request_permission("GET")
        return self._get_permission

    def _init_put_permission(self) -> Dict[str, Any]:
        """Initialize and return the put permission.

        Returns:
            The put permission.

        """
        if self._put_permission is None:
            self._put_permission = self._request_permission("PUT")
        return self._put_permission

    def get_object(self, key: str, _allow_retry: bool = True) -> UserResponse:
        """Get the object from AZURE.

        Arguments:
            key: The key of the file.
            _allow_retry: Whether requesting the get permission again is allowed.

        Raises:
            ResponseError: If post response error.

        Returns:
            The response of AZURE get object API.

        """
        permission = self._init_get_permission()
        verb = "GET"

        url = f"{permission['endpoint_prefix']}/{key}?{permission['sas_param']}"

        try:
            response = do(verb, url, timeout=config.timeout, stream=True)
            return UserResponse(response)
        except ResponseError as error:
            code = ElementTree.fromstring(error.response.text)[0].text
            if _allow_retry and code == self._RETRY_CODE:
                self._clear_get_permission()
                return self.get_object(key, False)

            raise error from None

    def put_object(self, key: str, path: Path, _allow_retry: bool = True) -> None:
        """Put the object to AZURE.

        Arguments:
            key: The key of the file.
            path: The path of the file.
            _allow_retry: Whether requesting the put permission again is allowed.

        Raises:
            ResponseError: If post response error.

        """
        permission = self._init_put_permission()
        verb = "PUT"

        url = f"{permission['endpoint_prefix']}/{key}?{permission['sas_param']}"
        headers = {"x-ms-blob-type": "BlockBlob"}
        mime_type = mimetypes.guess_type(path)[0]
        if mime_type is not None:
            headers["Content-Type"] = mime_type

        try:
            with path.open("rb") as fp:
                do(verb, url, headers=headers, data=fp)
        except ResponseError as error:
            code = ElementTree.fromstring(error.response.text)[0].text
            if _allow_retry and code == self._RETRY_CODE:
                self._clear_put_permission()
                self.put_object(key, path, False)
                return

            raise error from None


class S3ObjectPermissionManager(ObjectPermissionManager):
    """The basic structure of the object permission of the dataset stored in S3."""

    _RETRY_CODE = {"InvalidAccessKeyId", "AccessDenied"}
    _HASHED_PAYLOAD = {"GET": sha256(b"").hexdigest(), "PUT": "UNSIGNED-PAYLOAD"}

    def _init_get_permission(self) -> Dict[str, Any]:
        """Initialize and return the get permission.

        Returns:
            The get permission.

        """
        if self._get_permission is None:
            get_permission: Dict[str, Any] = self._request_permission("GET")
            get_permission["hmac"] = hmac.new(
                f"AWS4{get_permission['AccessKeySecret']}".encode(),
                digestmod=sha256,
            )
            self._get_permission = get_permission
        return self._get_permission

    def _init_put_permission(self) -> Dict[str, Any]:
        """Initialize and return the put permission.

        Returns:
            The put permission.

        """
        if self._put_permission is None:
            put_permission: Dict[str, Any] = self._request_permission("PUT")
            put_permission["hmac"] = hmac.new(
                f"AWS4{put_permission['AccessKeySecret']}".encode(),
                digestmod=sha256,
            )
            self._put_permission = put_permission
        return self._put_permission

    def _get_canonical_request(
        self,
        permission: Dict[str, Any],
        verb: str,
        key: str,
        x_amz_date: str,
    ) -> str:
        hashed_payload = self._HASHED_PAYLOAD[verb]
        canonical_headers = (
            f"host:{permission['bucket']}.{permission['endpoint']}\n"
            f"x-amz-content-sha256:{hashed_payload}\n"
            f"x-amz-date:{x_amz_date}\n"
            f"x-amz-security-token:{permission['SecurityToken']}\n"
        )

        return (
            f"{verb}\n/{key}\n\n{canonical_headers}\nhost;x-amz-content-sha256;x-amz-date;"
            f"x-amz-security-token\n{hashed_payload}"
        )

    @staticmethod
    def _get_string_to_sign(
        permission: Dict[str, Any], simple_date: str, x_amz_date: str, canonical_request: str
    ) -> str:
        scope = f"{simple_date}/{permission['region']}/s3/aws4_request"
        hash_canonical_request = sha256(canonical_request.encode("utf-8")).hexdigest()
        return f"AWS4-HMAC-SHA256\n{x_amz_date}\n{scope}\n{hash_canonical_request}"

    @staticmethod
    def _get_signature(permission: Dict[str, Any], string_to_sign: str, simple_date: str) -> str:
        _hmac = permission["hmac"].copy()
        _hmac.update(simple_date.encode())
        date_region_key = hmac.new(
            _hmac.digest(), f"{permission['region']}".encode(), sha256
        ).digest()
        date_region_service_key = hmac.new(date_region_key, b"s3", sha256).digest()
        signing_key = hmac.new(date_region_service_key, b"aws4_request", sha256).digest()

        return hmac.new(signing_key, string_to_sign.encode("utf-8"), sha256).hexdigest()

    def _get_headers(
        self,
        permission: Dict[str, Any],
        verb: str,
        key: str,
    ) -> Dict[str, str]:
        now = datetime.now(timezone.utc)
        simple_date = f"{now.year:04d}{now.month:02d}{now.day:02}"
        x_amz_date = f"{simple_date}T{now.hour:02d}{now.minute:02d}{now.second:02d}Z"

        canonical_request = self._get_canonical_request(permission, verb, key, x_amz_date)
        string_to_sign = self._get_string_to_sign(
            permission, simple_date, x_amz_date, canonical_request
        )
        signature = self._get_signature(permission, string_to_sign, simple_date)

        credential = (
            f"{permission['AccessKeyId']}/{simple_date}/{permission['region']}/s3/aws4_request"
        )
        authorization = (
            f"AWS4-HMAC-SHA256 Credential={credential}, SignedHeaders=host;"
            f"x-amz-content-sha256;x-amz-date;x-amz-security-token, "
            f"Signature={signature}"
        )

        return {
            "Authorization": authorization,
            "x-amz-security-token": permission["SecurityToken"],
            "x-amz-content-sha256": self._HASHED_PAYLOAD[verb],
            "x-amz-date": x_amz_date,
        }

    def get_object(self, key: str, _allow_retry: bool = True) -> UserResponse:
        """Get the object from S3.

        Arguments:
            key: The key of the file.
            _allow_retry: Whether requesting the get permission again is allowed.

        Raises:
            ResponseError: If post response error.

        Returns:
            The response of S3 get object API.

        """
        permission = self._init_get_permission()
        verb = "GET"

        headers = self._get_headers(permission, verb, key)
        url = f"https://{permission['bucket']}.{permission['endpoint']}/{key}"

        try:
            response = do(verb, url, headers=headers, timeout=config.timeout, stream=True)
            return UserResponse(response)
        except ResponseError as error:
            code = ElementTree.fromstring(error.response.text)[0].text
            if _allow_retry and code in self._RETRY_CODE:
                self._clear_get_permission()
                return self.get_object(key, False)

            raise error from None

    def put_object(self, key: str, path: Path, _allow_retry: bool = True) -> None:
        """Put the object to OSS.

        Arguments:
            key: The key of the file.
            path: The path of the file.
            _allow_retry: Whether requesting the put permission again is allowed.

        Raises:
            ResponseError: If post response error.

        """
        permission = self._init_put_permission()
        verb = "PUT"

        headers: Dict[str, Any] = self._get_headers(permission, verb, key)
        url = f"https://{permission['bucket']}.{permission['endpoint']}/{key}"
        mime_type = mimetypes.guess_type(path)[0]
        if mime_type is not None:
            headers["Content-Type"] = mime_type

        try:
            with path.open("rb") as fp:
                do(verb, url, headers=headers, data=fp)
        except ResponseError as error:
            code = ElementTree.fromstring(error.response.text)[0].text
            if _allow_retry and code in self._RETRY_CODE:
                self._clear_put_permission()
                self.put_object(key, path, False)
                return

            raise error from None
