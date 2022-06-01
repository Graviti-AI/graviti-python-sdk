#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The basic concepts and methods of the Graviti OpenAPI."""

from typing import Any
from uuid import uuid4

from requests.models import Response

from graviti.__version__ import __version__
from graviti.exception import ResponseError, ResponseErrorRegister
from graviti.utility import config, get_session

RESPONSE_ERROR_DISTRIBUTOR = ResponseErrorRegister.RESPONSE_ERROR_DISTRIBUTOR


def do(method: str, url: str, **kwargs: Any) -> Response:  # pylint: disable=invalid-name
    """Send a request.

    Arguments:
        method: The method of the request.
        url: The URL of the request.
        **kwargs: Extra keyword arguments to send in the GET request.

    Returns:
        Response of the request.

    """
    return get_session().request(method=method, url=url, **kwargs)


def open_api_do(method: str, access_key: str, url: str, **kwargs: Any) -> Response:
    """Send a request to the Graviti OpenAPI.

    Arguments:
        method: The method of the request.
        access_key: User's access key.
        url: The URL of the graviti website.
        **kwargs: Extra keyword arguments to send in the POST request.

    Raises:
        ResponseError: When the status code OpenAPI returns is unexpected.

    Returns:
        Response of the request.

    """
    headers = kwargs.setdefault("headers", {})
    headers["X-Token"] = access_key
    headers["X-Source"] = f"{config._x_source}/{__version__}"  # pylint: disable=protected-access
    headers["X-Request-Id"] = uuid4().hex

    try:
        return do(method=method, url=url, **kwargs)
    except ResponseError as error:
        response = error.response
        error_code = response.json()["code"]
        raise RESPONSE_ERROR_DISTRIBUTOR.get(error_code, ResponseError)(response=response) from None
