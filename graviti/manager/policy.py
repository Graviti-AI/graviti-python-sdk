#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the dataset policy."""

import base64
import hmac
import mimetypes
from datetime import datetime, timezone
from hashlib import sha1
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Optional
from xml.etree import ElementTree

from graviti.exception import ResponseError
from graviti.openapi import do, get_object_policy
from graviti.utility import UserResponse, config, convert_datetime_to_gmt

if TYPE_CHECKING:
    from graviti.manager import Dataset

_EXPIRED_IN_SECOND = 600
_OSS_RETRY_CODE = {"InvalidAccessKeyId", "AccessDenied"}


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
    ) -> Dict[str, str]:
        date = convert_datetime_to_gmt(datetime.now(timezone.utc))
        signature = (
            f"{verb}\n\n\n{date}\nx-oss-security-token:{policy['SecurityToken']}\n"
            f"/{policy['bucket']}/{key}"
        )
        _hmac = policy["hmac"].copy()
        _hmac.update(signature.encode("utf-8"))
        signature = base64.b64encode(_hmac.digest()).decode("utf-8")
        authorization = f"OSS {policy['AccessKeyId']}:{signature}"

        return {
            "Authorization": authorization,
            "x-oss-security-token": policy["SecurityToken"],
            "Date": date,
        }

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
            if _allow_retry and code in _OSS_RETRY_CODE:
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
            if _allow_retry and code in _OSS_RETRY_CODE:
                self._clear_put_policy()
                self.put_object(key, path, False)
            raise error from None
