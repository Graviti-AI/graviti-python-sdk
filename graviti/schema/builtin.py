#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#
"""The Portex builtin types."""


from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    Union,
    overload,
)

from graviti.schema.base import _INDENT, PortexType, param
from graviti.schema.package import packages

builtins = packages.builtins


class PortexNumericType(PortexType):
    """The base class of the Portex numeric types.

    Arguments:
        minimum: The minimum value.
        maximum: The maximum value.

    """

    minimum: Optional[float] = param(None)
    maximum: Optional[float] = param(None)

    def __init__(self, minimum: Optional[float] = None, maximum: Optional[float] = None) -> None:
        self.minimum = minimum
        self.maximum = maximum


@builtins("string")
class string(PortexType):  # pylint: disable=invalid-name
    """Portex primitive type ``string``.

    Examples:
        >>> t = string()
        >>> t
        string()

    """


@builtins("bytes")
class bytes_(PortexType):  # pylint: disable=invalid-name
    """Portex primitive type ``bytes``.

    Examples:
        >>> t = bytes_()
        >>> t
        bytes_()

    """


@builtins("boolean")
class boolean(PortexType):  # pylint: disable=invalid-name
    """Portex primitive type ``boolean``.

    Examples:
        >>> t = boolean()
        >>> t
        boolean()

    """


@builtins("int32")
class int32(PortexNumericType):  # pylint: disable=invalid-name
    """Portex primitive type ``int32``.

    Examples:
        >>> t = int32(0, 100)
        >>> t
        int32(
          minimum=0,
          maximum=100,
        )

    """


@builtins("int64")
class int64(PortexNumericType):  # pylint: disable=invalid-name
    """Portex primitive type ``int64``.

    Examples:
        >>> t = int64(0, 100)
        >>> t
        int64(
          minimum=0,
          maximum=100,
        )

    """


@builtins("float32")
class float32(PortexNumericType):  # pylint: disable=invalid-name
    """Portex primitive type ``float32``.

    Examples:
        >>> t = float32(0, 100)
        >>> t
        float32(
          minimum=0,
          maximum=100,
        )

    """


@builtins("float64")
class float64(PortexNumericType):  # pylint: disable=invalid-name
    """Portex primitive type ``float64``.

    Examples:
        >>> t = float64(0, 100)
        >>> t
        float64(
          minimum=0,
          maximum=100,
        )

    """


class Field(NamedTuple):
    """Represents a Portex ``record`` field, contains the field name and type."""

    name: str
    type: PortexType


class Fields(Sequence[Field]):
    """Represents a Portex ``record`` field list."""

    def __init__(
        self,
        fields: Union[Iterable[Union[Field, Tuple[str, PortexType]]], Mapping[str, PortexType]],
    ):
        iterable = fields.items() if isinstance(fields, Mapping) else fields
        self._data = [Field(*field) for field in iterable]  # pyright: reportGeneralTypeIssues=false

    @overload
    def __getitem__(self, index: int) -> Field:
        ...

    @overload
    def __getitem__(self, index: slice) -> List[Field]:
        ...

    def __getitem__(self, index: Union[int, slice]) -> Union[Field, List[Field]]:
        return self._data.__getitem__(index)

    def __len__(self) -> int:
        return self._data.__len__()

    def __repr__(self) -> str:
        return self._repr1(0)

    def _repr1(self, level: int) -> str:
        indent = level * _INDENT
        lines = ["{"]
        for field in self._data:
            lines.append(
                f"{_INDENT}'{field.name}': "  # pylint: disable=protected-access
                f"{field.type._repr1(level + 1)},"
            )

        lines.append("}")
        return f"\n{indent}".join(lines)

    @classmethod
    def from_pyobj(cls, content: List[Dict[str, Any]]) -> "Fields":
        """Create Portex field list instance from python list.

        Arguments:
            content: A python list representing a Portex field list.

        Returns:
            A Portex field list instance created from the input python list.

        """
        return Fields(Field(item["name"], PortexType.from_pyobj(item)) for item in content)

    def to_pyobj(self) -> List[Dict[str, Any]]:
        """Dump the instance to a python list.

        Returns:
            A Python List representation of the field list.

        """
        pylist = []
        for field in self._data:
            pydict = {"name": field.name}
            pydict.update(field.type.to_pyobj())
            pylist.append(pydict)

        return pylist


@builtins("record")
class record(PortexType):  # pylint: disable=invalid-name
    """Portex complex type ``record``.

    Arguments:
        fields: The fields of the record.

    Examples:
        Create a record by dict:

        >>> t = record({"f0": int32(), "f1": float32(0, 100)})
        >>> t
        record(
          fields={
            'f0': int32(),
            'f1': float32(
              minimum=0,
              maximum=100,
            ),
          },
        )

        Create a record by tuple list:

        >>> t = record([("f0", string()), ("f1", enum(["v0", "v1"]))])
        >>> t
        record(
          fields={
            'f0': string(),
            'f1': enum(
              values=['v0', 'v1'],
            ),
          },
        )

        Create a record by :class:`Field` list:

        >>> t = record([Field("f0", float64(0)), Field("f1", array(int64()))])
        >>> t
        record(
          fields={
            'f0': float64(
              minimum=0,
            ),
            'f1': array(
              items=int64(),
            ),
          },
        )

    """

    fields: Fields = param()

    def __init__(
        self,
        fields: Union[Sequence[Union[Field, Tuple[str, PortexType]]], Mapping[str, PortexType]],
    ) -> None:
        self.fields = Fields(fields)

    @overload
    def __getitem__(self, index: Union[int, str]) -> PortexType:
        ...

    @overload
    def __getitem__(self, index: slice) -> List[PortexType]:
        ...

    def __getitem__(self, index: Union[int, str, slice]) -> Union[PortexType, List[PortexType]]:
        if isinstance(index, slice):
            return [field.type for field in self.fields[index]]

        if isinstance(index, str):
            for field in self.fields:
                if index == field.name:
                    return field.type

            raise KeyError(index)

        return self.fields[index].type


@builtins("enum")
class enum(PortexType):  # pylint: disable=invalid-name
    """Portex complex type ``enum``.

    Arguments:
        values: The values of the enum members.

    Examples:
        >>> t = enum(["v0", "v1"])
        >>> t
        enum(
          values=['v0', 'v1'],
        )

    """

    values: List[Any] = param()

    def __init__(self, values: List[Any]) -> None:
        self.values = values


@builtins("array")
class array(PortexType):  # pylint: disable=invalid-name
    """Portex complex type ``array``.

    Arguments:
        items: The item type of the array.
        length: The length of the array.

    Examples:
        >>> t = array(int32(0), 100)
        >>> t
        array(
          items=int32(
            minimum=0,
          ),
          length=100,
        )

    """

    items: PortexType = param()
    length: Optional[int] = param(None)

    def __init__(self, items: PortexType, length: Optional[int] = None) -> None:
        self.items = items
        self.length = length


@builtins("tensor")
class tensor(PortexType):  # pylint: disable=invalid-name
    """Portex complex type ``tensor``.

    Arguments:
        shape: The shape of the tensor.
        dtype: The dtype of the tensor.

    Examples:
        >>> t = tensor((3, 3), "float64")
        >>> t
        tensor(
          shape=(3, 3),
          dtype='float64',
        )

    """

    shape: Tuple[Optional[int], ...] = param()
    dtype: str = param()

    def __init__(self, shape: Iterable[Optional[int]], dtype: str) -> None:
        self.shape = tuple(shape)
        self.dtype = dtype
