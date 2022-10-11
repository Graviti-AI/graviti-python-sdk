#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Graviti image file class."""

from pathlib import Path
from typing import TYPE_CHECKING, Dict, Optional, Type, TypeVar, Union

import pyarrow as pa

from graviti.file.base import File, RemoteFile
from graviti.file.image_size import get_image_size
from graviti.portex import STANDARD_URL, ExternalElementResgister

if TYPE_CHECKING:
    from graviti.manager import ObjectPermissionManager


_I = TypeVar("_I", bound="Image")
_RI = TypeVar("_RI", bound="RemoteImage")


class Image(File):
    """This class represents local image files.

    Arguments:
        path: The local path of the image.

    """

    __slots__ = File.__slots__ + ("_height", "_width")

    _height: int
    _width: int

    @classmethod
    def _from_pyarrow(
        cls: Type[_I],
        scalar: pa.StructScalar,
        _: Optional["ObjectPermissionManager"] = None,
    ) -> _I:
        obj: _I = object.__new__(cls)
        pyobj = scalar.as_py()

        obj._key = pyobj["key"]
        obj._path = Path(pyobj["key"])
        obj._extension = pyobj["extension"]
        obj._size = pyobj["size"]
        obj._height = pyobj["height"]
        obj._width = pyobj["width"]

        return obj

    def _to_post_data(self) -> Dict[str, Union[int, str]]:
        post_data = super()._to_post_data()
        post_data["height"] = self.height
        post_data["width"] = self.width
        return post_data

    @property
    def height(self) -> int:
        """Get the height of the image.

        Returns:
            The height of the image.

        """
        if not hasattr(self, "_height"):
            self._height, self._width = get_image_size(self.path)
        return self._height

    @property
    def width(self) -> int:
        """Get the width of the image.

        Returns:
            The width of the image.

        """
        if not hasattr(self, "_width"):
            self._height, self._width = get_image_size(self.path)
        return self._width


@ExternalElementResgister(STANDARD_URL, "main", "file.Image", "label.Mask")
class RemoteImage(RemoteFile):
    """This class represents remote image files.

    Arguments:
        key: The key of the image file.
        extension: The extension of the image file.
        size: The size of the image file.
        height: The height of the image.
        width: The width of the image.
        object_permission_manager: The permission to access the file.

    """

    __slots__ = RemoteFile.__slots__ + ("_height", "_width")

    _height: int
    _width: int

    def __init__(  # pylint: disable=too-many-arguments
        self,
        key: str,
        extension: str,
        size: int,
        height: int,
        width: int,
        object_permission_manager: "ObjectPermissionManager",
    ) -> None:
        RemoteFile.__init__(self, key, extension, size, object_permission_manager)
        self._height = height
        self._width = width

    @classmethod
    def _from_pyarrow(  # type: ignore[override]
        cls: Type[_RI],
        scalar: pa.StructScalar,
        object_permission_manager: "ObjectPermissionManager",
    ) -> _RI:
        obj: _RI = object.__new__(cls)
        pyobj = scalar.as_py()

        obj._key = pyobj["key"]
        obj._extension = pyobj["extension"]
        obj._size = pyobj["size"]
        obj._height = pyobj["height"]
        obj._width = pyobj["width"]
        obj._object_permission = object_permission_manager

        return obj

    def _to_post_data(self) -> Dict[str, Union[int, str]]:
        post_data = super()._to_post_data()
        post_data["height"] = self.height
        post_data["width"] = self.width
        return post_data

    @property
    def height(self) -> int:
        """Get the height of the image.

        Returns:
            The height of the image.

        """
        return self._height

    @property
    def width(self) -> int:
        """Get the width of the image.

        Returns:
            The width of the image.

        """
        return self._width
