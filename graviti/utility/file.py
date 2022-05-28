#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Graviti file related class."""

from hashlib import sha1
from pathlib import Path
from typing import Dict, Type, TypeVar, Union
from urllib.request import url2pathname

import pyarrow as pa
from _io import BufferedReader

from graviti.utility.common import shorten
from graviti.utility.repr import ReprMixin
from graviti.utility.requests import UserResponse, config, get_session

_T = TypeVar("_T", bound="File")


class FileBase(ReprMixin):
    """This class represents the file in a DataFrame."""

    _url: str
    _checksum: str

    def open(self) -> Union[UserResponse, BufferedReader]:
        """Return the binary file pointer of this file.

        Raises:
            NotImplementedError: The method of the base class should not be called.

        """
        raise NotImplementedError

    @property
    def url(self) -> str:
        """Get the url of the file.

        Returns:
            The url of the file.

        """
        return self._url

    @property
    def checksum(self) -> str:
        """Get the checksum of the file.

        Returns:
            The checksum of the file.

        """
        return self._checksum

    @staticmethod
    def from_pyarrow(scalar: pa.StructScalar) -> Union["RemoteFile", "File"]:
        """Create File or RemoteFile instance with pyarrow struct scalar.

        Arguments:
            scalar: The input pyarrow struct scalar.

        Returns:
            The File or RemoteFile instance.

        """
        url: str = scalar["url"].as_py()
        checksum: str = scalar["checksum"].as_py()

        if url.startswith("file://"):
            return File._create(url, checksum)  # pylint: disable=protected-access

        return RemoteFile(url, checksum)

    def to_pyobj(self) -> Dict[str, str]:
        """Dump the local file to a python dict.

        Returns:
            A python dict representation of the local file.

        """
        return {"url": self.url, "checksum": self.checksum}


class RemoteFile(FileBase):
    """This class represents the file on Graviti platform.

    Arguments:
        url: The url string of the file.
        checksum: The checksum of the file.

    """

    def __init__(self, url: str, checksum: str) -> None:
        self._url = url
        self._checksum = checksum

    def _repr_head(self) -> str:
        return f'{self.__class__.__name__}("{shorten(self._checksum)}")'

    def open(self) -> Union[UserResponse, BufferedReader]:
        """Return the binary file pointer of this file.

        Returns:
            The remote file pointer.

        """
        session = get_session()
        return UserResponse(session.request("GET", self._url, timeout=config.timeout, stream=True))


class File(FileBase):
    """This class represents the local file.

    Arguments:
        path: The local path of the file.

    """

    _BUFFER_SIZE = 65536

    def __init__(self, path: str) -> None:
        self.path = Path(path).absolute()

    def _repr_head(self) -> str:
        return f'{self.__class__.__name__}("{self.path.name}")'

    @classmethod
    def _create(cls: Type[_T], url: str, checksum: str) -> _T:
        obj: _T = object.__new__(cls)
        obj.path = Path(url2pathname(url[7:]))
        obj._url = url
        obj._checksum = checksum
        return obj

    @property
    def url(self) -> str:
        """Get the url of the file.

        Returns:
            The url of the file.

        """
        if not hasattr(self, "_url"):
            self._url = self.path.as_uri()

        return self._url

    @property
    def checksum(self) -> str:
        """Get the sha1 checksum of the local file.

        Returns:
            The sha1 checksum of the local file.

        """
        if not hasattr(self, "_checksum"):
            sha1_object = sha1()
            with self.path.open("rb") as fp:
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
        return self.path.open("rb")
