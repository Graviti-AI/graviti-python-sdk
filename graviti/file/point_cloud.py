#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Graviti point cloud file class."""

from graviti.file.base import File, RemoteFile


class PointCloud(File):
    """This class represents local point cloud files."""

    __slots__ = File.__slots__


class RemotePointCloud(RemoteFile):
    """This class represents remote point cloud files."""

    __slots__ = RemoteFile.__slots__
