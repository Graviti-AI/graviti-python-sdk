#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the dataset policy."""

import base64
import hmac
import mimetypes
from datetime import datetime, timezone
from hashlib import sha1, sha256
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Optional
from xml.etree import ElementTree

from graviti.exception import ResponseError
from graviti.openapi import do, get_object_policy
from graviti.utility import UserResponse, config, convert_datetime_to_gmt

if TYPE_CHECKING:
    from graviti.manager import Dataset

_EXPIRED_IN_SECOND = 600


class ObjectPolicyManager:
    """The basic structure of the object policy of the dataset.

    Arguments:
        dataset: Class :class:`~graviti.dataset.dataset.Dataset` instance.

    """

    _get_policy: Optional[Dict[str, Any]] = None
    _put_policy: Optional[Dict[str, Any]] = None

    def __init__(self, dataset: "Dataset") -> None:
        self._dataset = dataset

    def _request_policy(self, actions: str) -> Dict[str, str]:
        dataset = self._dataset
        return get_object_policy(  # type: ignore[no-any-return]
            dataset.access_key,
            dataset.url,
            dataset.owner,
            dataset.name,
            actions=actions,
            is_internal=config.is_internal,
            expired=_EXPIRED_IN_SECOND,
        )["policy"]

    def _clear_get_policy(self) -> None:
        """Clear the get policy."""
        delattr(self, "_get_policy")

    def _clear_put_policy(self) -> None:
        """Clear the put policy."""
        delattr(self, "_put_policy")

    def _init_put_policy(self) -> Dict[str, Any]:
        """Initialize and return the put policy.

        Raises:
            NotImplementedError: The method of the base class should not be called.

        """
        raise NotImplementedError

    @property
    def prefix(self) -> str:
        """Return the prefix of the put policy.

        Returns:
            The prefix of the put policy.

        """
        return self._init_put_policy()["prefix"]  # type: ignore[no-any-return]

    def get_object(self, key: str, _allow_retry: bool = True) -> UserResponse:
        """Get the object from graviti.

        Arguments:
            key: The key of the file.
            _allow_retry: Whether requesting the get policy again is allowed.

        Raises:
            NotImplementedError: The method of the base class should not be called.

        """
        raise NotImplementedError

    def put_object(self, key: str, path: Path, _allow_retry: bool = True) -> None:
        """Put the object to OSS.

        Arguments:
            key: The key of the file.
            path: The path of the file.
            _allow_retry: Whether requesting the put policy again is allowed.

        Raises:
            NotImplementedError: The method of the base class should not be called.

        """
        raise NotImplementedError


class OSSObjectPolicyManager(ObjectPolicyManager):
    """The basic structure of the object policy of the dataset stored in OSS."""

    _RETRY_CODE = {"InvalidAccessKeyId", "AccessDenied"}

    def _init_get_policy(self) -> Dict[str, Any]:
        """Initialize and return the get policy.

        Returns:
            The get policy.

        """
        if self._get_policy is None:
            get_policy: Dict[str, Any] = self._request_policy("GET")
            get_policy["hmac"] = hmac.new(
                get_policy["AccessKeySecret"].encode("utf-8"),
                digestmod=sha1,
            )
            self._get_policy = get_policy
        return self._get_policy

    def _init_put_policy(self) -> Dict[str, Any]:
        """Initialize and return the put policy.

        Returns:
            The put policy.

        """
        if self._put_policy is None:
            put_policy: Dict[str, Any] = self._request_policy("PUT")
            put_policy["hmac"] = hmac.new(
                put_policy["AccessKeySecret"].encode("utf-8"),
                digestmod=sha1,
            )
            self._put_policy = put_policy
        return self._put_policy

    @staticmethod
    def _get_headers(
        policy: Dict[str, Any],
        verb: str,
        key: str,
        mime_type: Optional[str] = None,
    ) -> Dict[str, str]:
        date = convert_datetime_to_gmt(datetime.now(timezone.utc))
        content_type = "" if mime_type is None else mime_type

        signature = (
            f"{verb}\n\n{content_type}\n{date}\nx-oss-security-token:{policy['SecurityToken']}\n"
            f"/{policy['bucket']}/{key}"
        )
        _hmac = policy["hmac"].copy()
        _hmac.update(signature.encode("utf-8"))
        signature = base64.b64encode(_hmac.digest()).decode("utf-8")
        authorization = f"OSS {policy['AccessKeyId']}:{signature}"

        headers = {
            "Authorization": authorization,
            "x-oss-security-token": policy["SecurityToken"],
            "Date": date,
        }
        if mime_type is not None:
            headers["Content-Type"] = mime_type
        return headers

    def get_object(self, key: str, _allow_retry: bool = True) -> UserResponse:
        """Get the object from OSS.

        Arguments:
            key: The key of the file.
            _allow_retry: Whether requesting the get policy again is allowed.

        Raises:
            ResponseError: If post response error.

        Returns:
            The response of OSS get object API.

        """
        policy = self._init_get_policy()
        verb = "GET"

        headers = self._get_headers(policy, verb, key)
        url = f"https://{policy['bucket']}.{policy['endpoint']}/{key}"

        try:
            response = do(verb, url, headers=headers, timeout=config.timeout, stream=True)
            return UserResponse(response)
        except ResponseError as error:
            code = ElementTree.fromstring(error.response.text)[0].text
            if _allow_retry and code in self._RETRY_CODE:
                self._clear_get_policy()
                return self.get_object(key, False)
            raise error from None

    def put_object(self, key: str, path: Path, _allow_retry: bool = True) -> None:
        """Put the object to OSS.

        Arguments:
            key: The key of the file.
            path: The path of the file.
            _allow_retry: Whether requesting the put policy again is allowed.

        Raises:
            ResponseError: If post response error.

        """
        policy = self._init_put_policy()
        verb = "PUT"

        mime_type = mimetypes.guess_type(path)[0]
        headers: Dict[str, Any] = self._get_headers(policy, verb, key, mime_type)
        url = f"https://{policy['bucket']}.{policy['endpoint']}/{key}"

        try:
            with path.open("rb") as fp:
                do(verb, url, headers=headers, data=fp)
        except ResponseError as error:
            code = ElementTree.fromstring(error.response.text)[0].text
            if _allow_retry and code in self._RETRY_CODE:
                self._clear_put_policy()
                self.put_object(key, path, False)
            raise error from None


class AZUREObjectPolicyManager(ObjectPolicyManager):
    """The basic structure of the object policy of the dataset stored in AZURE."""

    _RETRY_CODE = "AuthenticationFailed"

    def _init_get_policy(self) -> Dict[str, Any]:
        """Initialize and return the get policy.

        Returns:
            The get policy.

        """
        if self._get_policy is None:
            self._get_policy = self._request_policy("GET")
        return self._get_policy

    def _init_put_policy(self) -> Dict[str, Any]:
        """Initialize and return the put policy.

        Returns:
            The put policy.

        """
        if self._put_policy is None:
            self._put_policy = self._request_policy("PUT")
        return self._put_policy

    def get_object(self, key: str, _allow_retry: bool = True) -> UserResponse:
        """Get the object from AZURE.

        Arguments:
            key: The key of the file.
            _allow_retry: Whether requesting the get policy again is allowed.

        Raises:
            ResponseError: If post response error.

        Returns:
            The response of AZURE get object API.

        """
        policy = self._init_get_policy()
        verb = "GET"

        url = f"{policy['endpoint_prefix']}/{key}?{policy['sas_param']}"

        try:
            response = do(verb, url, timeout=config.timeout, stream=True)
            return UserResponse(response)
        except ResponseError as error:
            code = ElementTree.fromstring(error.response.text)[0].text
            if _allow_retry and code == self._RETRY_CODE:
                self._clear_get_policy()
                return self.get_object(key, False)
            raise error from None

    def put_object(self, key: str, path: Path, _allow_retry: bool = True) -> None:
        """Put the object to AZURE.

        Arguments:
            key: The key of the file.
            path: The path of the file.
            _allow_retry: Whether requesting the put policy again is allowed.

        Raises:
            ResponseError: If post response error.

        """
        policy = self._init_put_policy()
        verb = "PUT"

        url = f"{policy['endpoint_prefix']}/{key}?{policy['sas_param']}"
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
                self._clear_put_policy()
                self.put_object(key, path, False)
            raise error from None


class S3ObjectPolicyManager(ObjectPolicyManager):
    """The basic structure of the object policy of the dataset stored in S3."""

    _RETRY_CODE = {"InvalidAccessKeyId", "AccessDenied"}
    _HASHED_PAYLOAD = {"GET": sha256(b"").hexdigest(), "PUT": "UNSIGNED-PAYLOAD"}

    def _init_get_policy(self) -> Dict[str, Any]:
        """Initialize and return the get policy.

        Returns:
            The get policy.

        """
        if self._get_policy is None:
            get_policy: Dict[str, Any] = self._request_policy("GET")
            get_policy["hmac"] = hmac.new(
                f"AWS4{get_policy['AccessKeySecret']}".encode(),
                digestmod=sha256,
            )
            self._get_policy = get_policy
        return self._get_policy

    def _init_put_policy(self) -> Dict[str, Any]:
        """Initialize and return the put policy.

        Returns:
            The put policy.

        """
        if self._put_policy is None:
            put_policy: Dict[str, Any] = self._request_policy("PUT")
            put_policy["hmac"] = hmac.new(
                f"AWS4{put_policy['AccessKeySecret']}".encode(),
                digestmod=sha256,
            )
            self._put_policy = put_policy
        return self._put_policy

    def _get_canonical_request(
        self,
        policy: Dict[str, Any],
        verb: str,
        key: str,
        x_amz_date: str,
    ) -> str:
        hashed_payload = self._HASHED_PAYLOAD[verb]
        canonical_headers = (
            f"host:{policy['bucket']}.{policy['endpoint']}\n"
            f"x-amz-content-sha256:{hashed_payload}\n"
            f"x-amz-date:{x_amz_date}\n"
            f"x-amz-security-token:{policy['SecurityToken']}\n"
        )

        return (
            f"{verb}\n/{key}\n\n{canonical_headers}\nhost;x-amz-content-sha256;x-amz-date;"
            f"x-amz-security-token\n{hashed_payload}"
        )

    @staticmethod
    def _get_string_to_sign(
        policy: Dict[str, Any], simple_date: str, x_amz_date: str, canonical_request: str
    ) -> str:
        scope = f"{simple_date}/{policy['region']}/s3/aws4_request"
        hash_canonical_request = sha256(canonical_request.encode("utf-8")).hexdigest()
        return f"AWS4-HMAC-SHA256\n{x_amz_date}\n{scope}\n{hash_canonical_request}"

    @staticmethod
    def _get_signature(policy: Dict[str, Any], string_to_sign: str, simple_date: str) -> str:
        _hmac = policy["hmac"].copy()
        date_key = _hmac.update(f"{simple_date}".encode())
        date_region_key = hmac.new(date_key, f"{policy['region']}".encode(), sha256).digest()
        date_region_service_key = hmac.new(date_region_key, b"s3", sha256).digest()
        signing_key = hmac.new(date_region_service_key, b"aws4_request", sha256).digest()

        return hmac.new(signing_key, string_to_sign.encode("utf-8"), sha256).hexdigest()

    def _get_headers(
        self,
        policy: Dict[str, Any],
        verb: str,
        key: str,
    ) -> Dict[str, str]:
        now = datetime.now(timezone.utc)
        simple_date = f"{now.year:04d}{now.month:02d}{now.day:02}"
        x_amz_date = f"{simple_date}Y{now.hour:02d}{now.minute:02d}{now.second:02d}Z"

        canonical_request = self._get_canonical_request(policy, verb, key, x_amz_date)
        string_to_sign = self._get_string_to_sign(
            policy, simple_date, x_amz_date, canonical_request
        )
        signature = self._get_signature(policy, string_to_sign, simple_date)

        credential = f"{policy['AccessKeyId']}/{simple_date}/{policy['region']}/s3/aws4_request"
        authorization = (
            f"AWS4-HMAC-SHA256 Credential={credential}, SignedHeaders=host;"
            f"x-amz-content-sha256;x-amz-date;x-amz-security-token, "
            f"Signature={signature}"
        )

        return {
            "Authorization": authorization,
            "x-amz-security-token": policy["SecurityToken"],
            "x-amz-content-sha256": self._HASHED_PAYLOAD[verb],
            "x-amz-date": x_amz_date,
        }

    def get_object(self, key: str, _allow_retry: bool = True) -> UserResponse:
        """Get the object from S3.

        Arguments:
            key: The key of the file.
            _allow_retry: Whether requesting the get policy again is allowed.

        Raises:
            ResponseError: If post response error.

        Returns:
            The response of S3 get object API.

        """
        policy = self._init_get_policy()
        verb = "GET"

        headers = self._get_headers(policy, verb, key)
        url = f"https://{policy['bucket']}.{policy['endpoint']}/{key}"

        try:
            response = do(verb, url, headers=headers, timeout=config.timeout, stream=True)
            return UserResponse(response)
        except ResponseError as error:
            code = ElementTree.fromstring(error.response.text)[0].text
            if _allow_retry and code in self._RETRY_CODE:
                self._clear_get_policy()
                return self.get_object(key, False)
            raise error from None

    def put_object(self, key: str, path: Path, _allow_retry: bool = True) -> None:
        """Put the object to OSS.

        Arguments:
            key: The key of the file.
            path: The path of the file.
            _allow_retry: Whether requesting the put policy again is allowed.

        Raises:
            ResponseError: If post response error.

        """
        policy = self._init_put_policy()
        verb = "PUT"

        headers: Dict[str, Any] = self._get_headers(policy, verb, key)
        url = f"https://{policy['bucket']}.{policy['endpoint']}/{key}"
        mime_type = mimetypes.guess_type(path)[0]
        if mime_type is not None:
            headers["Content-Type"] = mime_type

        try:
            with path.open("rb") as fp:
                do(verb, url, headers=headers, data=fp)
        except ResponseError as error:
            code = ElementTree.fromstring(error.response.text)[0].text
            if _allow_retry and code in self._RETRY_CODE:
                self._clear_put_policy()
                self.put_object(key, path, False)
            raise error from None
