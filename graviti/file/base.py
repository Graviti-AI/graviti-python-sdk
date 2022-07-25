#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Graviti basic file class."""

import mimetypes
from hashlib import sha1
from pathlib import Path
from typing import TYPE_CHECKING, Dict, Union

from _io import BufferedReader

from graviti.utility.repr import ReprMixin
from graviti.utility.requests import UserResponse

if TYPE_CHECKING:
    # from graviti.openapi import ObjectPolicy
    ...

_ENCODINGS = mimetypes.encodings_map


class FileBase(ReprMixin):
    """This class represents the file in a DataFrame."""

    __slots__ = ("_key", "_extension", "_size")

    _key: str
    _extension: str
    _size: int

    def _to_post_data(self) -> Dict[str, Union[int, str]]:
        return {"key": self.key, "extension": self.extension, "size": self.size}

    @property
    def key(self) -> str:
        """Get the key of the file.

        Returns:
            The key of the file.

        """
        return self._key

    @property
    def extension(self) -> str:
        """Get the extension of the file.

        Returns:
            The extension of the file.

        """
        return self._extension

    @property
    def size(self) -> int:
        """Get the size of the file.

        Returns:
            The size of the file.

        """
        return self._size

    def open(self) -> Union[UserResponse, BufferedReader]:
        """Return the binary file pointer of this file.

        Raises:
            NotImplementedError: The method of the base class should not be called.

        """
        raise NotImplementedError


class File(FileBase):
    """This class represents local files.

    Arguments:
        path: The local path of the file.

    """

    __slots__ = FileBase.__slots__ + ("_path", "_checksum")

    _BUFFER_SIZE = 65536

    _checksum: str

    def __init__(self, path: str) -> None:
        self._path = Path(path).absolute()

    def _repr_head(self) -> str:
        return f'{self.__class__.__name__}("{self._path.name}")'

    @property
    def path(self) -> Path:
        """Get the path of the file.

        Returns:
            The path of the file.

        """
        return self._path

    @property
    def key(self) -> str:
        """Get the key of the file.

        Returns:
            The key of the file.

        """
        if not hasattr(self, "_key"):
            self._key = str(self._path)
        return self._key

    @property
    def extension(self) -> str:
        """Get the extension of the file.

        Returns:
            The extension of the file.

        """
        if not hasattr(self, "_extension"):
            suffix = self._path.suffix
            if suffix in _ENCODINGS:
                suffix = Path(self._path.stem).suffix + suffix
            self._extension = suffix
        return self._extension

    @property
    def size(self) -> int:
        """Get the size of the file.

        Returns:
            The size of the file.

        """
        if not hasattr(self, "_size"):
            self._size = self._path.stat().st_size
        return self._size

    def get_checksum(self) -> str:
        """Get the sha1 checksum of the local file.

        Returns:
            The sha1 checksum of the local file.

        """
        if not hasattr(self, "_checksum"):
            sha1_object = sha1()
            with self._path.open("rb") as fp:
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
        return self._path.open("rb")


class RemoteFile(FileBase):
    """This class represents the file on Graviti platform.

    Arguments:
        key: The key of the file.
        extension: The extension of the file.
        size: The size of the file.
        object_policy: The policy to access the file.

    """

    __slots__ = FileBase.__slots__ + ("_object_policy",)

    # def __init__(
    #     self, key: str, extension: str, size: int, object_policy: "ObjectPolicy"
    # ) -> None:
    def __init__(self, key: str, extension: str, size: int) -> None:
        self._key = key
        self._extension = extension
        self._size = size
        # self._object_policy = object_policy

    def _repr_head(self) -> str:
        return f'{self.__class__.__name__}("{self.key}")'

    def open(self) -> UserResponse:
        """Return the binary file pointer of this file.

        Return:
            The remote file pointer.

        """
        # return self._object_policy.get_object(self._key)
