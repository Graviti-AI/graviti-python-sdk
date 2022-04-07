#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The basic concepts and methods of the Graviti OpenAPI."""

from typing import Any
from uuid import uuid4

from requests.models import Response
from tensorbay.exception import ResponseError, ResponseErrorDistributor
from tensorbay.utility import config, get_session

from graviti.__version__ import __version__

URL_PATH_PREFIX = "api/v1"


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
    headers["access_key"] = access_key
    headers["source"] = f"{config._x_source}/{__version__}"  # pylint: disable=protected-access
    headers["request_id"] = uuid4().hex

    try:
        return get_session().request(method=method, url=url, **kwargs)
    except ResponseError as error:
        response = error.response
        error_code = response.json()["code"]
        raise ResponseErrorDistributor.get(error_code, ResponseError)(response=response) from None
