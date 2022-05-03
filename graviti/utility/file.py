#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Graviti file related class."""

from hashlib import sha1
from pathlib import Path
from typing import Dict, Union

from _io import BufferedReader

from graviti.utility.common import shorten
from graviti.utility.repr import ReprMixin
from graviti.utility.requests import UserResponse, config, get_session


class FileBase(ReprMixin):
    """This class represents the file in a DataFrame."""

    _checksum: str

    def open(self) -> Union[UserResponse, BufferedReader]:
        """Return the binary file pointer of this file.

        Raises:
            NotImplementedError: The method of the base class should not be called.

        Return:
            The file pointer.

        """
        raise NotImplementedError

    @property
    def checksum(self) -> str:
        """Get the checksum of the file.

        Returns:
            The checksum of the file.

        """
        return self._checksum


class RemoteFile(FileBase):
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


class File(FileBase):
    """This class represents the local file.

    Arguments:
        path: The local path of the file.

    """

    _BUFFER_SIZE = 65536

    def __init__(self, path: str) -> None:
        self.path = Path(path)

    def _repr_head(self) -> str:
        return f'{self.__class__.__name__}("{self.path.name}")'

    @property
    def checksum(self) -> str:
        """Get the sha1 checksum of the local file.

        Returns:
            The sha1 checksum of the local file.

        """
        if not hasattr(self, "_checksum"):
            sha1_object = sha1()
            with self.path.open("rb", encoding="utf-8") as fp:
                while True:
                    data = fp.read(self._BUFFER_SIZE)
                    if not data:
                        break
                    sha1_object.update(data)

            self._checksum = sha1_object.hexdigest()

        return self._checksum

    def open(self) -> BufferedReader:
        """Return the binary file pointer of this file.

        Returns:
            The local file pointer.

        """
        return self.path.open("rb", encoding="utf-8")

    def to_pyobj(self) -> Dict[str, str]:
        """Dump the local file to a python dict.

        Returns:
            A python dict representation of the local file.

        """
        return {"checksum": self.checksum}
