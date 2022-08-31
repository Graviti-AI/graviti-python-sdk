#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Graviti image file class."""

from typing import TYPE_CHECKING, Dict, Union

from graviti.file.base import File, RemoteFile
from graviti.file.image_size import get_image_size
from graviti.portex import STANDARD_URL, RemoteFileTypeResgister

if TYPE_CHECKING:
    from graviti.manager import ObjectPolicyManager


class Image(File):
    """This class represents local image files.

    Arguments:
        path: The local path of the image.

    """

    __slots__ = File.__slots__ + ("_height", "_width")

    _height: int
    _width: int

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


@RemoteFileTypeResgister(STANDARD_URL, "main", "file.Image", "label.Mask")
class RemoteImage(RemoteFile):
    """This class represents remote image files.

    Arguments:
        key: The key of the image file.
        extension: The extension of the image file.
        size: The size of the image file.
        height: The height of the image.
        width: The width of the image.
        object_policy_manager: The policy to access the file.

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
        object_policy_manager: "ObjectPolicyManager",
    ) -> None:
        RemoteFile.__init__(self, key, extension, size, object_policy_manager)
        self._height = height
        self._width = width

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
