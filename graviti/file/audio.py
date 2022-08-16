#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Graviti audio file class."""

from graviti.file.base import File, RemoteFile
from graviti.portex import STANDARD_URL, RemoteFileTypeResgister


class Audio(File):
    """This class represents local audio files."""

    __slots__ = File.__slots__


@RemoteFileTypeResgister(STANDARD_URL, "main", "file.Audio")
class RemoteAudio(RemoteFile):
    """This class represents remote audio files."""

    __slots__ = RemoteFile.__slots__
