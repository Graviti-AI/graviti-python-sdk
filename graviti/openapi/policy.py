#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Interfaces about the dataset policy."""

import base64
import hashlib
import hmac
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, Optional
from xml.etree import ElementTree

from graviti.exception import ResponseError
from graviti.openapi.requests import do, open_api_do
from graviti.utility import UserResponse, config

if TYPE_CHECKING:
    from graviti.manager import Dataset

_EXPIRED_IN_SECOND = 600
_WEEKS = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")
_MONTHS = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")
_OSS_RETRY_CODE = {"InvalidAccessKeyId", "AccessDenied"}


class ObjectPolicy:
    """The basic structure of the object policy of the dataset.

    Arguments:
        dataset: Class :class:`~graviti.dataset.dataset.Dataset` instance.

    """

    _get_policy: Optional[Dict[str, str]] = None

    def __init__(self, dataset: "Dataset") -> None:
        self._dataset = dataset

    def _request_get_policy(self) -> Dict[str, str]:
        dataset = self._dataset
        return get_object_policy(  # type: ignore[no-any-return]
            dataset.access_key,
            dataset.url,
            dataset.owner,
            dataset.name,
            actions="GET",
            is_internal=config.is_internal,
            expired=_EXPIRED_IN_SECOND,
        )["policy"]

    def _init_get_policy(self) -> Dict[str, str]:
        """Initialize and return the get policy.

        Returns:
            The get policy.

        """
        if self._get_policy is None:
            self._get_policy = self._request_get_policy()
        return self._get_policy

    def _clear_get_policy(self) -> None:
        """Clear the get policy."""
        delattr(self, "_get_policy")


class OSSObjectPolicy(ObjectPolicy):
    """The basic structure of the object policy of the dataset stored in oss."""

    _get_hmac: hmac.HMAC

    def _request_get_policy(self) -> Dict[str, str]:
        get_policy = super()._request_get_policy()
        self._get_hmac = hmac.new(
            get_policy["AccessKeySecret"].encode("utf-8"),
            digestmod=hashlib.sha1,
        )
        return get_policy

    def _get_authorization(
        self,
        policy: Dict[str, str],
        verb: str,
        date: str,
        key: str,
    ) -> str:
        signature = (
            f"{verb}\n\n\n{date}\nx-oss-security-token:{policy['SecurityToken']}\n"
            f"/{policy['bucket']}/{key}"
        )
        get_hmac = self._get_hmac.copy()
        get_hmac.update(signature.encode("utf-8"))
        signature = base64.b64encode(get_hmac.digest()).decode("utf-8")
        return f"OSS {policy['AccessKeyId']}:{signature}"

    def get_object(self, key: str, _allow_retry: bool = True) -> UserResponse:
        """Read the object from oss.

        Arguments:
            key: The object name of the remote file.
            _allow_retry: Whether requesting the get policy again is allowed.

        Raises:
            ResponseError: If post response error.

        Returns:
            The response of oss get object API.

        """
        policy = self._init_get_policy()
        date = _from_datetime_to_gmt(datetime.now(timezone.utc))
        verb = "GET"

        authorization = self._get_authorization(policy, verb, date, key)
        headers = {
            "Authorization": authorization,
            "x-oss-security-token": policy["SecurityToken"],
            "Date": date,
        }
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


def get_object_policy(
    access_key: str,
    url: str,
    owner: str,
    dataset: str,
    *,
    actions: str,
    is_internal: Optional[bool] = None,
    expired: Optional[int] = None,
) -> Dict[str, Any]:
    """Execute the OpenAPI `GET /v2/datasets/{owner}/{dataset}/policy/object`.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        owner: The owner of the dataset.
        dataset: Name of the dataset, unique for a user.
        actions: The specific actions including "GET" and "PUT". Supports multiple actions,
            which need to be separated by ``|``, like "GET|PUT".
        is_internal: Whether to return the intranet upload address, the default value in
            the OpenAPI is False.
        expired: Token expiry time in seconds. It cannot be negative.

    Returns:
        The response of OpenAPI.

    Examples:
        Request permission to get dataset data:

        >>> get_policy(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com",
        ...     "graviti-example",
        ...     "MNIST",
        ...     actions="GET",
        ... )
        {
            "backend_type":"OSS",
            "policy": {
                "AccessKeyId":"LTAI4FjgXD3yFJUaasdasd",
                "AccessKeySecret":"LTAI4FjgXD3yFJJKasdad",
                "SecurityToken":"CAISrgJ1q6Ft5B2yfSjIr5bkKILdaseqw",
                "bucket":"content-store-dev",
                "endpoint":"content-store-dev.oss-cn-qingdao.aliyuncs.com",
                "expireAt":"2022-07-12T06:07:52Z"
            }
        }

        Request permission to put dataset data:

        >>> get_policy(
        ...     "ACCESSKEY-********",
        ...     "https://api.graviti.com",
        ...     "graviti-example",
        ...     "MNIST",
        ...     actions="PUT",
        ... )
        {
            "backend_type":"OSS",
            "policy": {
                "AccessKeyId":"LTAI4FjgXD3yFJUaasdasd",
                "AccessKeySecret":"LTAI4FjgXD3yFJJKasdad",
                "SecurityToken":"CAISrgJ1q6Ft5B2yfSjIr5bkKILdaseqw",
                "bucket":"content-store-dev",
                "endpoint":"content-store-dev.oss-cn-qingdao.aliyuncs.com",
                "expireAt":"2022-07-12T06:07:52Z",
                "prefix":"051dd0676cc74f548a7e9b7ace45c26b/"
            }
        }

    """
    url = f"{url}/v2/datasets/{owner}/{dataset}/policy/object"
    params: Dict[str, Any] = {"actions": actions}

    if is_internal is not None:
        params["is_internal"] = is_internal
    if expired is not None:
        params["expired"] = expired

    return open_api_do("GET", access_key, url, params=params).json()  # type: ignore[no-any-return]


def _from_datetime_to_gmt(utctime: datetime) -> str:
    return (
        f"{_WEEKS[utctime.weekday()]}, {utctime.day:02d} {_MONTHS[utctime.month - 1]}"
        f" {utctime.year:04d} {utctime.hour:02d}:{utctime.minute:02d}:{utctime.second:02d} GMT"
    )
