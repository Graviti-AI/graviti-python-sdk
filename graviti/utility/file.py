#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Graviti file related class."""

from functools import partial

from tensorbay.cli.utility import shorten
from tensorbay.utility.file import URL, RemoteFileMixin

_URL = partial(URL, updater=lambda: "update is not supported currently")


class File(RemoteFileMixin):
    """This class represents the file on Graviti platform.

    Arguments:
        url: The url string of the file.
        checksum: The checksum of the file.

    """

    __slots__ = ("url", "_checksum")

    def __init__(self, url: str, checksum: str) -> None:
        super().__init__("")
        self.url = _URL(url)
        self._checksum = checksum

    def _repr_head(self) -> str:
        return f'{self.__class__.__name__}("{shorten(self._checksum)}")'

    @property
    def checksum(self) -> str:
        """Get the checksum fo the file.

        Returns:
            The checksum of the file.

        """
        return self._checksum
