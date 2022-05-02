#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#
"""The PyArrow extension types and arrays to represent PortexType."""

import json
from typing import TYPE_CHECKING, Any, ClassVar, Dict, Type, TypeVar, Union, overload

import pyarrow as pa

from graviti.utility.file import RemoteFile

if TYPE_CHECKING:
    # https://pylint.pycqa.org/en/latest/technical_reference/c_extensions.html
    from pyarrow.lib import DataType


class ExtensionBase(pa.ExtensionType):  # type: ignore[misc]
    """The base class for the PyArrow representation of PortexType types.

    Arguments:
        name: The PortexType name.
        storage_type: The corresponding PyArrow type.
        nullable: Whether the PortexType is nullable.
        kwargs: Other keyword arguments to init the PortexType.

    """

    _T = TypeVar("_T", bound="ExtensionBase")

    _PYARROW_PACKAGE_NAME: ClassVar[str]

    def __init__(
        self,
        name: str,
        storage_type: "DataType",
        *,
        nullable: bool = False,
        **kwargs: Any,
    ):
        self._name = name
        self._nullable = nullable
        self._kwargs = kwargs
        pa.ExtensionType.__init__(self, storage_type, f"{self._PYARROW_PACKAGE_NAME}.{name}")

    def __arrow_ext_serialize__(self) -> bytes:
        return json.dumps(self._to_pyobj()).encode("utf-8")

    @classmethod
    def __arrow_ext_deserialize__(cls: Type[_T], storage_type: "DataType", serialized: bytes) -> _T:
        kwargs = json.loads(serialized.decode("utf-8"))
        return cls(**kwargs, storage_type=storage_type)

    def _to_pyobj(self) -> Dict[str, Any]:
        return {"name": self._name, "nullable": self._nullable, **self._kwargs}


class BuiltinExtension(ExtensionBase):
    """This class defines the PyArrow representation of PortexBuiltinType type."""

    _PYARROW_PACKAGE_NAME = "builtin"


class ExternalExtension(ExtensionBase):
    """This class defines the PyArrow representation of PortexExternalType type.

    Arguments:
        name: The PortexExternalType name.
        storage_type: The corresponding PyArrow type.
        url: The repo url which contains the PortexExternalType.
        revision: The repo revision which contains the PortexExternalType.
        nullable: Whether the PortexExternalType is nullable.
        kwargs: Other keyword arguments to init the PortexExternalType.

    """

    _PYARROW_PACKAGE_NAME = "external"

    def __init__(
        self,
        name: str,
        storage_type: "DataType",
        url: str,
        revision: str,
        *,
        nullable: bool = False,
        **kwargs: Any,
    ):
        self._url = url
        self._revision = revision
        super().__init__(name, storage_type, nullable=nullable, **kwargs)

    def _to_pyobj(self) -> Dict[str, Any]:
        return {**super()._to_pyobj(), "url": self._url, "revision": self._revision}


class GravitiExtension(ExtensionBase):
    """This class defines the PyArrow representation of PortexGravitiType type."""

    _PYARROW_PACKAGE_NAME = "graviti"


class RemoteFileArray(pa.ExtensionArray):  # type: ignore[misc]
    """This class defines the PyArrow representation of RemoteFileArray."""

    @overload
    def __getitem__(self, index: int) -> RemoteFile:
        pass

    @overload
    def __getitem__(self, index: slice) -> "RemoteFileArray":
        pass

    def __getitem__(self, index: Union[int, slice]) -> Union[RemoteFile, "RemoteFileArray"]:
        if isinstance(index, slice):
            return super().__getitem__(index)  # type: ignore[no-any-return]

        item = super().__getitem__(index).as_py()
        return RemoteFile(item["url"], item["checksum"])


class RemoteFileType(GravitiExtension):
    """This class defines the PyArrow representation of RemoteFileType type."""

    def __init__(self) -> None:
        super().__init__("file", pa.struct({"url": pa.string(), "checksum": pa.string()}))

    def __arrow_ext_class__(self) -> Type[RemoteFileArray]:  # pylint: disable=no-self-use
        return RemoteFileArray
