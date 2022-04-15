#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Graviti file related class."""

from typing import Union

from _io import BufferedReader
from tensorbay.cli.utility import shorten

from graviti.utility.repr import ReprMixin
from graviti.utility.requests import UserResponse, config, get_session


class File(ReprMixin):
    """This class represents the file on Graviti platform.

    Arguments:
        url: The url string of the file.
        checksum: The checksum of the file.

    """

    __slots__ = ("url", "_checksum")

    def __init__(self, url: str, checksum: str) -> None:
        self.url = url
        self._checksum = checksum

    def _repr_head(self) -> str:
        return f'{self.__class__.__name__}("{shorten(self._checksum)}")'

    def _urlopen(self) -> UserResponse:
        session = get_session()
        return UserResponse(session.request("GET", self.url, timeout=config.timeout, stream=True))

    def open(self) -> Union[UserResponse, BufferedReader]:
        """Return the binary file pointer of this file.

        Returns:
            The remote file pointer.

        """
        return self._urlopen()

    @property
    def checksum(self) -> str:
        """Get the checksum of the file.

        Returns:
            The checksum of the file.

        """
        return self._checksum
