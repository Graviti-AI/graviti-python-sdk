#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#
"""Utility module."""

from graviti.utility.attr import AttrDict

# https://github.com/python/mypy/issues/9318
from graviti.utility.avro import convert_arrow_schema_to_avro  # type: ignore[attr-defined]
from graviti.utility.collections import (
    NameOrderedDict,
    UserMapping,
    UserMutableMapping,
    UserMutableSequence,
    UserSequence,
)
from graviti.utility.common import locked, shorten
from graviti.utility.file import File, FileBase, RemoteFile
from graviti.utility.itertools import chunked
from graviti.utility.paging import LazyFactory, PagingList
from graviti.utility.pyarrow import (
    BuiltinExtension,
    ExtensionBase,
    ExternalExtension,
    GravitiExtension,
    RemoteFileArray,
    RemoteFileType,
)
from graviti.utility.repr import MAX_REPR_ROWS, ReprMixin, ReprType
from graviti.utility.requests import config, get_session
from graviti.utility.typing import NestedDict

__all__ = [
    "AttrDict",
    "BuiltinExtension",
    "ExtensionBase",
    "ExternalExtension",
    "File",
    "FileBase",
    "GravitiExtension",
    "LazyFactory",
    "MAX_REPR_ROWS",
    "NameOrderedDict",
    "NestedDict",
    "PagingList",
    "RemoteFile",
    "RemoteFileArray",
    "RemoteFileType",
    "ReprMixin",
    "ReprType",
    "UserMapping",
    "UserMutableMapping",
    "UserMutableSequence",
    "UserSequence",
    "chunked",
    "config",
    "convert_arrow_schema_to_avro",
    "get_session",
    "locked",
    "shorten",
]
