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

from graviti.schema.base import _INDENT, PortexType, TypeRegister, param


class PortexNumericType(PortexType):
    """The base class of the Portex numeric types.

    Arguments:
        minimum: The minimum value.
        maximum: The maximum value.

    """

    minimum: Optional[float] = param("minimum", False, None)
    maximum: Optional[float] = param("maximum", False, None)

    def __init__(self, minimum: Optional[float] = None, maximum: Optional[float] = None) -> None:
        self.minimum = minimum
        self.maximum = maximum


@TypeRegister("string")
class string(PortexType):  # pylint: disable=invalid-name
    """Portex primitive type ``string``.

    Examples:
        >>> t = string()
        >>> t
        string()

    """


@TypeRegister("bytes")
class bytes_(PortexType):  # pylint: disable=invalid-name
    """Portex primitive type ``bytes``.

    Examples:
        >>> t = bytes_()
        >>> t
        bytes_()

    """


@TypeRegister("boolean")
class boolean(PortexType):  # pylint: disable=invalid-name
    """Portex primitive type ``boolean``.

    Examples:
        >>> t = boolean()
        >>> t
        boolean()

    """


@TypeRegister("int")
class int_(PortexNumericType):  # pylint: disable=invalid-name
    """Portex primitive type ``int``.

    Examples:
        >>> t = int_(0, 100)
        >>> t
        int_(
          minimum=0,
          maximum=100,
        )

    """


@TypeRegister("long")
class long(PortexNumericType):  # pylint: disable=invalid-name
    """Portex primitive type ``long``.

    Examples:
        >>> t = long(0, 100)
        >>> t
        long(
          minimum=0,
          maximum=100,
        )

    """


@TypeRegister("float")
class float_(PortexNumericType):  # pylint: disable=invalid-name
    """Portex primitive type ``float``.

    Examples:
        >>> t = float_(0, 100)
        >>> t
        float_(
          minimum=0,
          maximum=100,
        )

    """


@TypeRegister("double")
class double(PortexNumericType):  # pylint: disable=invalid-name
    """Portex primitive type ``double``.

    Examples:
        >>> t = double(0, 100)
        >>> t
        double(
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


@TypeRegister("record")
class record(PortexType):  # pylint: disable=invalid-name
    """Portex complex type ``record``.

    Arguments:
        fields: The fields of the record.

    Examples:
        Create a record by dict:

        >>> t = record({"f0": int_(), "f1": float_(0, 100)})
        >>> t
        record(
          fields={
            'f0': int_(),
            'f1': float_(
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

        >>> t = record([Field("f0", double(0)), Field("f1", array(long()))])
        >>> t
        record(
          fields={
            'f0': double(
              minimum=0,
            ),
            'f1': array(
              items=long(),
            ),
          },
        )

    """

    fields: Fields = param("fields", True)

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


@TypeRegister("enum")
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

    values: List[Any] = param("values", True)

    def __init__(self, values: List[Any]) -> None:
        self.values = values


@TypeRegister("array")
class array(PortexType):  # pylint: disable=invalid-name
    """Portex complex type ``array``.

    Arguments:
        items: The item type of the array.
        length: The length of the array.

    Examples:
        >>> t = array(int_(0), 100)
        >>> t
        array(
          items=int_(
            minimum=0,
          ),
          length=100,
        )

    """

    items: PortexType = param("items", True)
    length: Optional[int] = param("length", False, None)

    def __init__(self, items: PortexType, length: Optional[int] = None) -> None:
        self.items = items
        self.length = length


@TypeRegister("tensor")
class tensor(PortexType):  # pylint: disable=invalid-name
    """Portex complex type ``tensor``.

    Arguments:
        shape: The shape of the tensor.
        dtype: The dtype of the tensor.

    Examples:
        >>> t = tensor((3, 3), "double")
        >>> t
        tensor(
          shape=(3, 3),
          dtype='double',
        )

    """

    shape: Tuple[Optional[int], ...] = param("shape", True)
    dtype: str = param("dtype", True)

    def __init__(self, shape: Iterable[Optional[int]], dtype: str) -> None:
        self.shape = tuple(shape)
        self.dtype = dtype
