#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the Graviti Extension for pyarrow."""

from typing import TYPE_CHECKING, Type, Union, overload

import pyarrow as pa

from graviti.dataframe.frame import DataFrame
from graviti.utility.file import File
from graviti.utility.pyarrow import GravitiExtension

if TYPE_CHECKING:
    # https://pylint.pycqa.org/en/latest/technical_reference/c_extensions.html
    from pyarrow.lib import DataType


class FileArray(pa.ExtensionArray):  # type: ignore[misc]
    """This class defines the PyArrow representation of FileArray."""

    @overload
    def __getitem__(self, index: int) -> File:
        pass

    @overload
    def __getitem__(self, index: slice) -> "FileArray":
        pass

    def __getitem__(self, index: Union[int, slice]) -> Union[File, "FileArray"]:
        if isinstance(index, slice):
            return super().__getitem__(index)  # type: ignore[no-any-return]

        item = super().__getitem__(index).as_py()
        return File(item["url"], item["checksum"])


class FileType(GravitiExtension):
    """This class defines the PyArrow representation of FileType type."""

    def __init__(self) -> None:
        super().__init__("file", pa.struct({"url": pa.string(), "checksum": pa.string()}))

    def __arrow_ext_class__(self) -> Type[FileArray]:  # pylint: disable=no-self-use
        return FileArray


class DataFrameArray(pa.ExtensionArray):  # type: ignore[misc]
    """This class defines the PyArrow representation of DataFrameArray."""

    @overload
    def __getitem__(self, index: int) -> DataFrame:
        pass

    @overload
    def __getitem__(self, index: slice) -> "DataFrameArray":
        pass

    def __getitem__(self, index: Union[int, slice]) -> Union[DataFrame, "DataFrameArray"]:
        if isinstance(index, slice):
            return super().__getitem__(index)  # type: ignore[no-any-return]

        item = super().__getitem__(index).as_py()
        return DataFrame(item)


class DataFrameType(GravitiExtension):
    """This class defines the PyArrow representation of FileType type."""

    def __init__(self, storage_type: "DataType") -> None:
        super().__init__("dataframe", storage_type)

    def __arrow_ext_class__(self) -> Type[DataFrameArray]:  # pylint: disable=no-self-use
        return DataFrameArray
